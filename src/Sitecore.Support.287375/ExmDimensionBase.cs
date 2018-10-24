using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using Microsoft.Extensions.DependencyInjection;
using Sitecore.Analytics.Aggregation.Data.Model;
using Sitecore.Analytics.Model;
using Sitecore.DependencyInjection;
using Sitecore.Diagnostics;
using Sitecore.EmailCampaign.ExperienceAnalytics;
using Sitecore.EmailCampaign.ExperienceAnalytics.Dimensions;
using Sitecore.EmailCampaign.ExperienceAnalytics.Properties;
using Sitecore.EmailCampaign.Model;
using Sitecore.EmailCampaign.Model.XConnect;
using Sitecore.EmailCampaign.Model.XConnect.Events;
using Sitecore.EmailCampaign.Model.XConnect.Facets;
using Sitecore.EmailCampaign.XConnect.Web;
using Sitecore.ExM.Framework.Diagnostics;
using Sitecore.ExperienceAnalytics.Aggregation.Data.Model;
using Sitecore.ExperienceAnalytics.Aggregation.Data.Schema;
using Sitecore.ExperienceAnalytics.Aggregation.Dimensions;
using Sitecore.ExperienceAnalytics.Core;
using Sitecore.Framework.Conditions;
using Sitecore.XConnect;
using Sitecore.XConnect.Client;
using Sitecore.XConnect.Collection.Model;

namespace Sitecore.Support.EmailCampaign.ExperienceAnalytics.Dimensions
{
  public abstract class ExmDimensionBase : DimensionBase
  {
    internal readonly ILogger Logger;
    private readonly IUniqueEventCache _uniqueEventCache;
    private readonly XConnectRetry _xConnectRetry;

    protected ExmDimensionBase(Guid dimensionId)
        : this(ServiceLocator.ServiceProvider.GetService<ILogger>(), ServiceLocator.ServiceProvider.GetService<IUniqueEventCache>(), ServiceLocator.ServiceProvider.GetService<XConnectRetry>(), dimensionId)
    {
    }

    protected ExmDimensionBase([NotNull] ILogger logger, IUniqueEventCache uniqueEventCache, XConnectRetry xConnectRetry, Guid dimensionId)
        : base(dimensionId)
    {
      Condition.Requires(logger, nameof(logger)).IsNotNull();
      Condition.Requires(uniqueEventCache, nameof(uniqueEventCache)).IsNotNull();
      Condition.Requires(xConnectRetry, nameof(xConnectRetry)).IsNotNull();

      Logger = logger;
      _uniqueEventCache = uniqueEventCache;
      _xConnectRetry = xConnectRetry;
    }

    public override IEnumerable<DimensionData> GetData([NotNull] IVisitAggregationContext context)
    {
      Assert.ArgumentNotNull(context, "context");

      var dimensions = new List<DimensionData>();

      if (context.Visit?.CustomValues == null || !context.Visit.CustomValues.Any())
      {
        return dimensions;
      }

      VisitData visit = context.Visit;

      if (!(context.Visit.CustomValues.First().Value is Interaction))
      {
        return dimensions;
      }

      Interaction interaction = context.Visit.CustomValues.First().Value as Interaction;

      Logger.LogDebug($"Processing {GetType()} dimension on Interaction {visit.InteractionId}");

      try
      {
        dimensions = GetDimensions(interaction).ToList();
      }
      catch (Exception e)
      {
        Logger.LogError(e);
      }

      Logger.LogDebug($"{GetType()} returned {dimensions.Count} dimensions on Interaction {visit.InteractionId}");
      return dimensions;
    }

    protected virtual IEnumerable<DimensionData> GetDimensions(Interaction interaction)
    {
      Assert.ArgumentNotNull(interaction, nameof(interaction));

      List<EmailEvent> events = interaction.Events.OfType<EmailEvent>().ToList();
      if (!events.Any())
      {
        Logger.LogDebug("No EXM events found");
        yield break;
      }

      List<EmailEvent> exmEvents = interaction.Events.OfType<EmailEvent>().ToList();

      foreach (EmailEvent emailEvent in exmEvents)
      {
        EmailEventType emailEventType = DimensionUtils.ParsePageEvent(emailEvent.DefinitionId);

        string dimensionKey = CreateDimensionKey(interaction, emailEvent, emailEventType);

        if (dimensionKey == null)
        {
          continue;
        }

        var dimension = new DimensionData
        {
          DimensionKey = dimensionKey,
          MetricsValue = new SegmentMetricsValue
          {
            Visits = 1,
            PageViews = 1
          }
        };

        EmailEvent nextExmEvent = exmEvents.FirstOrDefault(x => x.Timestamp > emailEvent.Timestamp);
        DateTime until = nextExmEvent?.Timestamp ?? DateTime.MaxValue;

        List<PageViewEvent> browsedPages = interaction
            .Events
            .OfType<PageViewEvent>()
            .Where(x => x.Timestamp >= emailEvent.Timestamp && x.Timestamp < until && x.Id != emailEvent.ParentEventId)
            .ToList();

        PageViewEvent parentEvent = interaction
            .Events
            .OfType<PageViewEvent>()
            .SingleOrDefault(x => x.Id == emailEvent.ParentEventId);

        List<Goal> goals = interaction
            .Events
            .OfType<Goal>()
            .Where(x => x.Timestamp >= emailEvent.Timestamp && x.Timestamp < until)
            .ToList();

        dimension.MetricsValue.PageViews += browsedPages.Count;

        dimension.MetricsValue.Bounces = browsedPages.Count > 0 ? 0 : 1;

        dimension.MetricsValue.Value = interaction.Events.Where(x => x.Timestamp >= emailEvent.Timestamp && x.Timestamp < until).Sum(x => x.EngagementValue);

        dimension.MetricsValue.TimeOnSite += (int)(parentEvent?.Duration.TotalSeconds ?? 0);

        dimension.MetricsValue.Conversions = goals.Count;

        if ((emailEventType == EmailEventType.Click || emailEventType == EmailEventType.Open) &&
            IsUniqueEvent(interaction.Contact.Id, emailEvent, emailEventType))
        {
          dimension.MetricsValue.Count = 1;
        }

        yield return dimension;
      }
    }

    internal abstract string GenerateCustomKey([NotNull] Interaction interaction, [NotNull] EmailEvent exmEvent, EmailEventType eventType);

    private bool IsUniqueEvent(Guid? contactId, EmailEvent emailEvent, EmailEventType emailEventType)
    {
      if (!contactId.HasValue)
      {
        return false;
      }

      if (_uniqueEventCache.HasUniqueEvent(contactId.Value, emailEvent.MessageId, emailEvent.InstanceId, emailEventType, emailEvent.Id))
      {
        return true;
      }

      Contact contact = GetContact(contactId);

      ExmKeyBehaviorCache exmKeyBehaviorCache = contact?.ExmKeyBehaviorCache();

      if (exmKeyBehaviorCache?.UniqueEvents == null)
      {
        return false;
      }

      string key = exmKeyBehaviorCache.GetUniqueEventDictionaryKey(emailEvent.MessageId, emailEvent.InstanceId, emailEventType);

      Guid eventId;

      if (!exmKeyBehaviorCache.UniqueEvents.TryGetValue(key, out eventId))
      {
        return false;
      }

      bool isUniqueEvent = emailEvent.Id == eventId;

      if (isUniqueEvent)
      {
        _uniqueEventCache.SetUniqueEvent(contactId.Value, emailEvent.MessageId, emailEvent.InstanceId, emailEventType, emailEvent.Id);
      }

      return isUniqueEvent;

    }

    private Contact GetContact(Guid? contactId)
    {
      if (!contactId.HasValue)
      {
        return null;
      }

      var reference = new ContactReference(contactId.Value);
      Contact contact = null;
      _xConnectRetry.RequestWithRetry(client =>
      {
        contact = client.Get(reference, new ContactExpandOptions(ExmKeyBehaviorCache.DefaultFacetKey));
      });
      return contact;
    }

    private string CreateDimensionKey(Interaction interaction, EmailEvent emailEvent, EmailEventType emailEventType)
    {
      string customKey = GenerateCustomKey(interaction, emailEvent, emailEventType);

      return customKey == null ? null : string.Format(CultureInfo.InvariantCulture, "{0}_{1}", GenerateBaseKey(emailEvent.ManagerRootId, emailEvent.MessageId), customKey);
    }

    private string GenerateBaseKey(Guid managerRootId, Guid messageId)
    {
      if (managerRootId == default(Guid))
      {
        Logger.LogDebug(string.Format(CultureInfo.InvariantCulture, "Parameter '{0}' of VisitAggregationState is null or empty and the '{1}' dimension will not be processed!",
            "ManagerRootId", GetType().Name));
        return null;
      }

      if (messageId == default(Guid))
      {
        Logger.LogDebug(string.Format(CultureInfo.InvariantCulture, "Parameter '{0}' of VisitAggregationState is null or empty and the '{1}' dimension will not be processed!",
            "MessageId", GetType().Name));
        return null;
      }

      return
          new HierarchicalKeyBuilder()
              .Add(managerRootId)
              .Add(messageId)
              .ToString();
    }
  }
}
