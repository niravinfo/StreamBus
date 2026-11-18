namespace StreamBus;

/// <summary>Why a retry policy decided not to give a failed delivery another attempt.</summary>
internal enum RetryDecision
{
    /// <summary>The exception is handled; the schedule decides whether attempts remain.</summary>
    Retry,
    /// <summary>An Ignore rule matched the exception.</summary>
    Ignored,
    /// <summary>Handle rules exist but none matched the exception.</summary>
    Unhandled,
}

/// <summary>
/// Include/exclude rules used to classify a handler exception before the retry
/// schedule is consulted. With no rules every exception is retried; an exclude
/// match wins over an include match.
/// </summary>
internal sealed class ExceptionFilterSet
{
    private readonly List<Func<Exception, bool>> _includes = new();
    private readonly List<Func<Exception, bool>> _excludes = new();

    public bool HasRules => _includes.Count > 0 || _excludes.Count > 0;

    public void AddInclude(params Type[] types) => AddTypes(_includes, types);

    public void AddInclude<T>(Func<T, bool> predicate) where T : Exception
        => AddTyped(_includes, predicate);

    public void AddInclude(Func<Exception, bool> predicate) => _includes.Add(predicate);

    public void AddExclude(params Type[] types) => AddTypes(_excludes, types);

    public void AddExclude<T>(Func<T, bool> predicate) where T : Exception
        => AddTyped(_excludes, predicate);

    public void AddExclude(Func<Exception, bool> predicate) => _excludes.Add(predicate);

    public RetryDecision Decide(Exception ex)
    {
        ArgumentNullException.ThrowIfNull(ex);
        var candidates = Candidates(ex);
        if (Matches(_excludes, candidates))
            return RetryDecision.Ignored;
        if (_includes.Count > 0 && !Matches(_includes, candidates))
            return RetryDecision.Unhandled;
        return RetryDecision.Retry;
    }

    private static List<Exception> Candidates(Exception ex)
    {
        var candidates = new List<Exception>();
        var baseException = ex.GetBaseException();
        Add(ex);
        Add(baseException);
        if (baseException is AggregateException aggregate)
        {
            foreach (var inner in aggregate.InnerExceptions)
            {
                Add(inner);
                Add(inner.GetBaseException());
            }
        }
        return candidates;

        void Add(Exception candidate)
        {
            if (!candidates.Exists(c => ReferenceEquals(c, candidate)))
                candidates.Add(candidate);
        }
    }

    private static bool Matches(List<Func<Exception, bool>> rules, IReadOnlyList<Exception> candidates)
    {
        foreach (var rule in rules)
        {
            foreach (var candidate in candidates)
            {
                if (rule(candidate))
                    return true;
            }
        }
        return false;
    }

    private static void AddTypes(List<Func<Exception, bool>> rules, Type[] types)
    {
        foreach (var type in types)
            rules.Add(candidate => type.IsInstanceOfType(candidate));
    }

    private static void AddTyped<T>(List<Func<Exception, bool>> rules, Func<T, bool> predicate)
        where T : Exception
        => rules.Add(candidate => candidate is T typed && predicate(typed));
}
