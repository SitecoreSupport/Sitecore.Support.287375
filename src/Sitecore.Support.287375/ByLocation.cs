using System.Net;

namespace Sitecore.Support.EmailCampaign.ExperienceAnalytics.Dimensions
{
  using Sitecore;
  using Sitecore.Analytics.Lookups;
  using Sitecore.Analytics.Configuration;
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
                WhoIsInformation whoIsInformation = GetWhoisInformation(info.IpAddress);
                if (whoIsInformation != null)
                {
                  country = whoIsInformation.Country;
                  region = whoIsInformation.Region;
                  city = whoIsInformation.City;
                  goto Label_00A7;
                }
              }
              catch (Exception e)
              {
                Logger.LogWarn($"Cannot lookup WhoIsInformation for Interaction '{interaction.Id}'", e);
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

    public virtual WhoIsInformation GetWhoisInformation([NotNull] string ipAddress)
    {
      Assert.IsNotNull(ipAddress, "ipAddress");
      IPAddress adress;
      if (IPAddress.TryParse(ipAddress, out adress))
      {
        return GetWhoisInformation(adress);
      }
      return null;
    }

    public virtual WhoIsInformation GetWhoisInformation([NotNull] IPAddress ipAddress)
    {
      Assert.IsNotNull(ipAddress, "ipAddress");
      byte[] addressBytes = ipAddress.GetAddressBytes();
      var geoIpOptions = new GeoIpOptions
      {
        Ip = GeoIpManager.IpHashProvider.ResolveIpAddress(addressBytes),
        Id = GeoIpManager.IpHashProvider.ComputeGuid(addressBytes),
        MillisecondsTimeout = (-1)
      };
      return GeoIpManager.GetGeoIpData(geoIpOptions).GeoIpData;
    }
  }
}
