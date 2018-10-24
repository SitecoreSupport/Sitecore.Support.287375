using System.Net;

namespace Sitecore.Support.EmailCampaign.ExperienceAnalytics.Dimensions
{
  using Sitecore;
  using Sitecore.Analytics.Lookups;
  using Sitecore.Analytics.Model;
  using Sitecore.Diagnostics;
  using Sitecore.EmailCampaign.ExperienceAnalytics;
  using Sitecore.EmailCampaign.Model;
  using Sitecore.EmailCampaign.Model.XConnect.Events;
  using Sitecore.EmailCampaign.XConnect.Web;
  using Sitecore.ExM.Framework.Diagnostics;
  using Sitecore.XConnect;
  using Sitecore.XConnect.Collection.Model;
  using System;
  using System.Globalization;

  internal class ByLocation : Sitecore.Support.EmailCampaign.ExperienceAnalytics.Dimensions.ExmDimensionBase
  {
    private readonly ILogger _logger;

    [UsedImplicitly]
    public ByLocation(Guid dimensionId) : base(dimensionId)
    {
    }

    internal ByLocation(ILogger logger, IUniqueEventCache uniqueEventCache, XConnectRetry xConnectRetry, Guid dimensionId) : base(logger, uniqueEventCache, xConnectRetry, dimensionId)
    {
      _logger = logger;
    }

    internal override string GenerateCustomKey(Interaction interaction, EmailEvent exmEvent, EmailEventType eventType)
    {
      string country;
      string region;
      string city;
      IpInfo info = interaction.IpInfo();
      if (info != null)
      {
        EmailEvent event2 = exmEvent;
        if (event2 != null)
        {
          if (!(event2 is EmailClickedEvent) && !(event2 is UnsubscribedFromEmailEvent))
          {
            if (event2 is EmailOpenedEvent)
            {
              if (string.IsNullOrEmpty(info.IpAddress))
              {
                return null;
              }
              try
              {
                WhoIsInformation whoisInformation = this.GetWhoisInformation(info.IpAddress);
                if (whoisInformation == null)
                {
                  return null;
                }

                country = whoisInformation.Country;
                region = whoisInformation.Region;
                city = whoisInformation.City;
                goto Label_00A7;
              }
              catch (Exception exception)
              {
                _logger.LogError($"Cannot lookup WhoIsInformation for Interaction '{interaction.Id}'", exception);
                return null;
              }
            }
          }
          else
          {
            country = info.Country;
            region = info.Region;
            city = info.City;
            goto Label_00A7;
          }
        }
      }
      return null;
      Label_00A7:
      if (((country == null) || (region == null)) || (city == null))
      {
        return null;
      }
      int num = (int)eventType;
      return new KeyBuilder().Add(num.ToString(CultureInfo.InvariantCulture)).Add(country).Add(region).Add(city).ToString();
    }

    public virtual WhoIsInformation GetWhoisInformation(string ipAddress)
    {
      Assert.ArgumentNotNull(ipAddress, "ipAddress");

      var ip = IPAddress.Parse(ipAddress).GetAddressBytes();
      GeoIpOptions options = new GeoIpOptions
      {
        Ip = GeoIpManager.IpHashProvider.ResolveIpAddress(ip),
        Id = GeoIpManager.IpHashProvider.ComputeGuid(ip),
        MillisecondsTimeout = -1
      };
      var result = GeoIpManager.GetGeoIpData(options);

      return result.GeoIpData;
    }
  }
}
