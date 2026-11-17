using SPS.Abstractions;

namespace SPS.Bench;

public sealed class FixedCapacityPolicy : ISystemPolicy
{
    public IRequestPolicy RpcPolicy { get; } = new FixedRpcPolicy();

    public EndpointSelectionDecision? SelectEndpoint(RoutingContext context, Random random)
    {
        if (context.Endpoints.Count == 0) return null;
        var best = context.Endpoints.OrderBy(e => e.OutstandingCalls / (double)e.Slots).First();
        return EndpointSelectionDecision.Assign(best.Id);
    }

    public EndpointDecision ReviewEndpoint(EndpointContext context, Random random)
    {
        var last = context.RecentResults.TakeLast(2).ToList();
        if (context.Endpoint.Role == InstanceRole.Api && last.Count == 2 && last.All(r => r.Outcome == RequestOutcome.Timeout))
            return EndpointDecision.Probe();
        return EndpointDecision.Allow();
    }

    public CacheReadDecision OnCacheRead(CacheReadContext context, Random random)
    {
        if (context.Entry != null && context.Now + TimeSpan.FromMilliseconds(5) - context.Entry.SourceObservedAt <= context.Request.MaxDataAge)
            return CacheReadDecision.ServeCached();
        if (context.PendingFetches.Count > 0) return CacheReadDecision.JoinFetch(context.PendingFetches[^1].Id);
        return CacheReadDecision.StartFetch();
    }

    public CacheWriteDecision OnCacheFill(CacheFillContext context, Random random)
    {
        long used = context.Entries.Where(e => e.Key != context.Value.Key).Sum(e => e.SizeBytes);
        var evict = new List<string>();
        foreach (var entry in context.Entries.Where(e => e.Key != context.Value.Key).OrderBy(e => e.SourceObservedAt))
        {
            if (context.CapacityBytes - used >= context.Value.SizeBytes) break;
            evict.Add(entry.Key);
            used -= entry.SizeBytes;
        }
        if (context.CapacityBytes - used < context.Value.SizeBytes) return CacheWriteDecision.Skip();
        return CacheWriteDecision.Retain(TimeSpan.FromSeconds(2), evict.ToArray());
    }

    public JobAssignmentDecision? SelectJobAssignment(JobSchedulingContext context, Random random)
    {
        if (context.QueuedJobs.Count == 0 || context.AvailableWorkers.Count == 0) return null;
        return JobAssignmentDecision.Assign(context.QueuedJobs[0].Id, context.AvailableWorkers.OrderByDescending(w => w.Speed).First().Id);
    }

    public JobRecoveryDecision ReviewJob(JobRecoveryContext context, Random random)
    {
        var last = context.Job.LastHeartbeat ?? context.Job.AssignedAt ?? context.Now;
        return context.Now - last > TimeSpan.FromSeconds(3) ? JobRecoveryDecision.RequeueAfter(TimeSpan.Zero) : JobRecoveryDecision.KeepWaiting();
    }

    public CapacityPlan ReviewCapacity(CapacityContext context, Random random)
    {
        var s = context.Settings;
        int workers = Math.Min(s.MaximumWorkers, (s.ComputeUnitLimit - 2 * s.Api.ComputeUnits) / s.Worker.ComputeUnits);
        return new CapacityPlan(2, workers);
    }

    public void Observe(SystemObservation observation) { }

    private sealed class FixedRpcPolicy : IRequestPolicy
    {
        public TimeSpan GetTimeout(RequestContext context, Random random) => TimeSpan.FromMilliseconds(400);

        public RetryDecision OnResult(RequestObservation observation, Random random) =>
            observation.Outcome != RequestOutcome.Success && observation.Attempt < 4
                ? RetryDecision.RetryAfter(TimeSpan.Zero)
                : RetryDecision.NoRetry();
    }
}
