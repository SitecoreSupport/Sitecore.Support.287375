using System;

namespace Sitecore.Support.EmailCampaign.Cm.Pipelines.EmailOpened
{
  using System.Net;
  using System.Collections.Generic;
  using System.Linq;
  using System.Runtime.CompilerServices;
  using Sitecore.Data;
  using Sitecore.Diagnostics;
  using Sitecore.EmailCampaign.Cm.Pipelines.EmailOpened;
  using Sitecore.EmailCampaign.XConnect.Web;
  using Sitecore.ExM.Framework.Diagnostics;
  using Sitecore.XConnect;
  using Sitecore.XConnect.Client;
  using Sitecore.XConnect.Collection.Model;
  using Sitecore.XConnect.Operations;
  using Sitecore.Analytics.Model;
  using Sitecore.Analytics.Lookups;
  public class SaveInteraction
  {
    private readonly ILogger _logger;

    private readonly XConnectRetry _xConnectRetry;

    public double Delay
    {
      get;
      set;
    }

    public int RetryCount
    {
      get;
      set;
    }

    public SaveInteraction(ILogger logger, XConnectRetry xConnectRetry)
    {
      Assert.ArgumentNotNull(logger, "logger");
      Assert.ArgumentNotNull(xConnectRetry, "xConnectRetry");
      _logger = logger;
      _xConnectRetry = xConnectRetry;
    }

    public void Process(EmailOpenedPipelineArgs args)
    {
      Assert.ArgumentNotNull(args, "args");
      Assert.ArgumentCondition(args.MessageItem != null, "args", "MessageItem not set");
      Assert.ArgumentCondition(args.Interaction != null, "args", "Interaction not set");
      Assert.ArgumentCondition(!ID.IsNullOrEmpty(args.ChannelId), "args", "ChannelId not set");
      if (args.MessageItem.ExcludeFromReports)
      {
        _logger.LogDebug(FormattableString.Invariant($"Interaction not saved as '{args.MessageItem.ID}' has been excluded from reports."));
      }
      else
      {
        try
        {
          foreach (Event @event in args.Events)
          {
            args.Interaction.Events.Add(@event);
          }
          _xConnectRetry.RequestWithRetry(delegate (IXdbContext client)
          {
            client.AddInteraction(args.Interaction);
            IpInfo ipInfo = new IpInfo(args.EmailOpen.IPAddress);
            PopulateGeoIpInfo(ipInfo, args.Interaction.Id);
            client.SetIpInfo(args.Interaction, ipInfo);
            client.Submit();
          }, IsTransient, Delay, RetryCount);
        }
        catch (Exception e)
        {
          _logger.LogError(FormattableString.Invariant($"Failed to create interaction for a campaign '{args.Interaction.CampaignId}'"), e);
          throw;
        }
      }
    }

    protected bool IsTransient(Exception ex, IXdbContext client)
    {
      IEnumerable<IXdbOperation> enumerable = client?.LastBatch?.Where((IXdbOperation x) => x.Status == XdbOperationStatus.Failed);
      if (ex is XdbUnavailableException || (enumerable != null && enumerable.Any()))
      {
        _logger.LogWarn(FormattableString.Invariant(FormattableStringFactory.Create("[{0}] Transient error. Retrying. (Message: {1})", "SaveInteraction", ex.Message)), ex);
        return true;
      }
      _logger.LogError(FormattableString.Invariant(FormattableStringFactory.Create("[{0}] Not a transient error. Throwing: (Message: {1})", "SaveInteraction", ex.Message)), ex);
      return false;
    }

    protected void PopulateGeoIpInfo(IpInfo ipInfo, Guid? interactionId)
    {
      Assert.ArgumentNotNull(ipInfo, "ipInfo");
      try
      {
        WhoIsInformation whoIsInformation = GetWhoisInformation(ipInfo.IpAddress);
        if (whoIsInformation != null)
        {
          ipInfo.Country = whoIsInformation.Country;
          ipInfo.Region = whoIsInformation.Region;
          ipInfo.City = whoIsInformation.City;
        }
      }
      catch (Exception e)
      {
        _logger.LogError($"Cannot lookup WhoIsInformation for Interaction '{interactionId}'", e);
      }
    }

    protected virtual WhoIsInformation GetWhoisInformation(string ipAddress)
    {
      Assert.IsNotNull(ipAddress, "ipAddress");
      IPAddress address;
      return IPAddress.TryParse(ipAddress, out  address) ? GetWhoisInformation(address) : null;
    }

    protected virtual WhoIsInformation GetWhoisInformation(IPAddress ipAddress)
    {
      Assert.IsNotNull(ipAddress, "ipAddress");
      try
      {
        byte[] addressBytes = ipAddress.GetAddressBytes();
        var geoIpOptions = new GeoIpOptions
        {
          Ip = GeoIpManager.IpHashProvider.ResolveIpAddress(addressBytes),
          Id = GeoIpManager.IpHashProvider.ComputeGuid(addressBytes),
          MillisecondsTimeout = (-1)
        };
        return GeoIpManager.GetGeoIpData(geoIpOptions).GeoIpData;
      }
      catch (Exception e)
      {
        _logger.LogWarn("Cannot get geoIpData", e);
      }
      return null;
    }
  }
}