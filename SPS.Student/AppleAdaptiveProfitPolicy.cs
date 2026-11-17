using SPS.Abstractions;

namespace SPS.Student;

public sealed class AppleAdaptiveProfitPolicy : ISystemPolicy
{
    private const double MinGeneratePatience = 15;
    private const double MaxGeneratePatience = 60;
    private const double MetadataPatience = 1.5;
    private const double PreviewPatience = 3;
    private const double BudgetMargin = 0.05;
    private const double DeliveryDelay = 0.002;
    private const double FreshnessMargin = 0.004;
    private const double KeyRateTau = 10;
    private const double DemandWindow = 10;
    private const double BacklogHorizon = 8;
    private const double PublishOverhead = 0.3;
    private const double WorthFactor = 0.6;
    private const double KeepAliveMargin = 1.5;
    private const double ScaleDownDelay = 1;
    private const double WorkerRoundingSlack = 0.5;
    private const double DispatchMargin = 0.15;
    private const double SuspectHeartbeats = 2.5;
    private const double DeadSilence = 3;
    private const double ApiTargetUtilization = 0.5;
    private const double ApiUnhealthyGrace = 0.5;
    private const double SlowRatioDecay = 20;
    private const int ApiFailureStreak = 2;

    private readonly AdaptiveRpcPolicy rpc;
    private SystemSettings? settings;
    private TimeSpan now;
    private RequestKind? pendingKind;
    private TimeSpan pendingCreated;

    private readonly Queue<Arrival> arrivals = new();
    private readonly Dictionary<string, KeyStat> keys = new(StringComparer.Ordinal);
    private readonly double[] maxDataAge = { 2, 5, 0 };
    private readonly double[] meanRevenue = { 0.05, 0.2, 3.5 };
    private readonly bool[] revenueSeen = new bool[3];
    private readonly Dictionary<long, InstanceTrack> tracks = new();
    private readonly Dictionary<long, Assignment> assignments = new();
    private readonly Queue<PlanSample> history = new();
    private TimeSpan? firstArrival;
    private TimeSpan lastArrival;
    private TimeSpan lastApiArrival;
    private double meanGap = 0.2;
    private double apiLoad;
    public AppleAdaptiveProfitPolicy()
    {
        rpc = new AdaptiveRpcPolicy(this);
    }
    public IRequestPolicy RpcPolicy => rpc;

    public TimeSpan? GetProcessingBudget(RequestInfo request, Random random)
    {
        Touch(request.CreatedAt);
        RecordArrival(request);
        return TimeSpan.FromSeconds(PatienceLimit(request.Kind) + BudgetMargin);
    }
    public EndpointSelectionDecision? SelectEndpoint(RoutingContext context, Random random)
    {
        Touch(context.Now);
        int queueLimit = settings?.Api.QueueLimit ?? 0;
        EndpointInfo? best = null;
        double bestScore = double.MaxValue;
        foreach (var endpoint in context.Endpoints)
        {
            if (endpoint.OutstandingCalls >= endpoint.Slots + queueLimit)
            {
                continue;
            }
            var track = Track(endpoint);
            double score = (endpoint.OutstandingCalls + 1.0) / (endpoint.Slots * endpoint.Speed) * (1 + 4 * track.FailRate);
            if (score < bestScore)
            {
                bestScore = score;
                best = endpoint;
            }
        }
        if (best == null)
        {
            return null;
        }
        SetPending(context.Request);
        return EndpointSelectionDecision.Assign(best.Id);
    }
    public EndpointDecision ReviewEndpoint(EndpointContext context, Random random)
    {
        Touch(context.Now);
        var endpoint = context.Endpoint;
        var track = Track(endpoint);
        if (endpoint.State != InstanceState.Ready)
        {
            track.UnhealthySince = null;
            return EndpointDecision.Allow();
        }
        bool healthy = endpoint.Role == InstanceRole.Api ? ApiHealthy(context.RecentResults) : WorkerResponsive(track);
        if (healthy)
        {
            track.UnhealthySince = null;
            return EndpointDecision.Allow();
        }
        track.UnhealthySince ??= now;
        return EndpointDecision.Probe();
    }
    public CacheReadDecision OnCacheRead(CacheReadContext context, Random random)
    {
        Touch(context.Now);
        var request = context.Request;
        var entry = context.Entry;
        if (entry != null && Seconds(now) + DeliveryDelay + FreshnessMargin - Seconds(entry.SourceObservedAt) <= Seconds(request.MaxDataAge))
        {
            return CacheReadDecision.ServeCached();
        }
        FetchInfo? youngest = null;
        foreach (var fetch in context.PendingFetches)
        {
            if (youngest == null || fetch.StartedAt > youngest.StartedAt)
            {
                youngest = fetch;
            }
        }
        if (youngest != null)
        {
            return CacheReadDecision.JoinFetch(youngest.Id);
        }
        SetPending(request);
        return CacheReadDecision.StartFetch();
    }
    public CacheWriteDecision OnCacheFill(CacheFillContext context, Random random)
    {
        Touch(context.Now);
        var value = context.Value;
        int kind = (int)KindOfKey(value.Key);
        double ttl = Seconds(value.SourceObservedAt) + maxDataAge[kind] - Seconds(now) - DeliveryDelay;
        if (ttl <= 0.05 || value.SizeBytes > context.CapacityBytes)
        {
            return CacheWriteDecision.Skip();
        }
        long used = 0;
        var others = new List<(CacheEntryInfo Entry, double Value)>();
        foreach (var entry in context.Entries)
        {
            if (entry.Key == value.Key)
            {
                continue;
            }
            used += entry.SizeBytes;
            others.Add((entry, CacheValue(entry.Key, entry.SizeBytes, entry.SourceObservedAt)));
        }
        long free = context.CapacityBytes - used;
        var retained = TimeSpan.FromSeconds(ttl);
        if (value.SizeBytes <= free)
        {
            return CacheWriteDecision.Retain(retained);
        }
        double newValue = CacheValue(value.Key, value.SizeBytes, value.SourceObservedAt);
        var evictions = new List<string>();
        foreach (var candidate in others.OrderBy(o => o.Value))
        {
            if (free >= value.SizeBytes)
            {
                break;
            }
            if (candidate.Value >= newValue)rfr
            {
                return CacheWriteDecision.Skip();
            }
            evictions.Add(candidate.Entry.Key);
            free += candidate.Entry.SizeBytes;
        }
        return free >= value.SizeBytes ? CacheWriteDecision.Retain(retained, evictions.ToArray()) : CacheWriteDecision.Skip();
    }
    public JobAssignmentDecision? SelectJobAssignment(JobSchedulingContext context, Random random)
    {
        Touch(context.Now);
        double price = WorkerPrice();
        JobInfo? bestJob = null;
        EndpointInfo? bestWorker = null;
        double bestScore = 0;
        double bestAge = 0;
        double fastest = 0;
        foreach (var worker in context.AvailableWorkers)
        {
            fastest = Math.Max(fastest, EffectiveSpeed(worker));
        }
        foreach (var job in context.QueuedJobs)
        {
            double age = Seconds(now - job.Request.CreatedAt);
            double work = Math.Max(job.Request.EstimatedWorkSeconds, 0.001);
            if (fastest > 0 && Survival(age + work / fastest + PublishOverhead) <= 0)
            {
                return JobAssignmentDecision.Reject(job.Id);
            }
            double revenue = (double)job.Request.Revenue;
            foreach (var worker in context.AvailableWorkers)
            {
                double duration = work / EffectiveSpeed(worker);
                double expected = revenue * Survival(age + duration + PublishOverhead);
                if (expected <= 0 || expected < duration * price * WorthFactor)
                {
                    continue;
                }
                double score = expected / duration;
                if (score > bestScore * 1.000001 || (score >= bestScore * 0.999999 && age > bestAge))
                {
                    bestScore = score;
                    bestAge = age;
                    bestJob = job;
                    bestWorker = worker;
                }
            }
        }
        if (bestJob == null || bestWorker == null)
        {
            return null;
        }
        assignments[bestJob.Id] = new Assignment(now, bestWorker.Id, bestJob.Request.EstimatedWorkSeconds);
        return JobAssignmentDecision.Assign(bestJob.Id, bestWorker.Id);
    }
    public JobRecoveryDecision ReviewJob(JobRecoveryContext context, Random random)
    {
        Touch(context.Now);
        var job = context.Job;
        if (job.WorkerId is not long workerId || job.AssignedAt is not TimeSpan assignedAt)
        {
            return JobRecoveryDecision.KeepWaiting();
        }
        if (!tracks.TryGetValue(workerId, out var worker) || worker.State == InstanceState.Stopped)
        {
            return JobRecoveryDecision.RequeueAfter(TimeSpan.Zero);
        }
        if (worker.LastHeartbeat is TimeSpan beat && Seconds(beat - assignedAt) > DispatchMargin &&
            (job.LastHeartbeat is not TimeSpan jobBeat || jobBeat < beat))
        {
            return JobRecoveryDecision.RequeueAfter(TimeSpan.Zero);
        }
        var lastSeen = job.LastHeartbeat ?? assignedAt;
        if (worker.LastHeartbeat is TimeSpan workerBeat && workerBeat > lastSeen)
        {
            lastSeen = workerBeat;
        }
        if (Seconds(now - lastSeen) <= DeadSilence)
        {
            return JobRecoveryDecision.KeepWaiting();
        }
        if (WorkerDead(worker))
        {
            return JobRecoveryDecision.RequeueAfter(TimeSpan.Zero);
        }
        double expected = job.Request.EstimatedWorkSeconds / Math.Max(worker.Speed, 0.01) * 1.5 + PublishOverhead + DeadSilence;
        return Seconds(now - assignedAt) > expected ? JobRecoveryDecision.RequeueAfter(TimeSpan.Zero) : JobRecoveryDecision.KeepWaiting();
    }
    public CapacityPlan ReviewCapacity(CapacityContext context, Random random)
    {
        Touch(context.Now);
        settings = context.Settings;
        foreach (var instance in context.Instances)
        {
            Track(instance);
        }
        PruneArrivals();
        var apiSpec = settings.Api;
        var workerSpec = settings.Worker;
        double price = WorkerPrice();
        var workers = context.Instances.Where(i => i.Role == InstanceRole.Worker && i.State is InstanceState.Starting or InstanceState.Ready).ToList();
        var apis = context.Instances.Where(i => i.Role == InstanceRole.Api && i.State is InstanceState.Starting or InstanceState.Ready).ToList();
        double speed = workers.Count > 0 ? workers.Average(EffectiveSpeed) : workerSpec.Speed;

        double window = firstArrival is TimeSpan first ? Math.Clamp(Seconds(now - first), 1, DemandWindow) : DemandWindow;
        double workRate = 0, revenueRate = 0;
        bool apiTraffic = false;
        foreach (var arrival in arrivals)
        {
            if (arrival.Kind != RequestKind.Metadata)
            {
                apiTraffic = true;
            }
            if (arrival.Kind != RequestKind.Generate)
            {
                continue;
            }
            double duration = arrival.Work / speed;
            double expected = arrival.Revenue * Survival(duration + PublishOverhead);
            if (expected >= duration * price * WorthFactor)
            {
                workRate += arrival.Work;
                revenueRate += expected;
            }
        }
        workRate /= window;
        revenueRate /= window;

        double backlog = 0;
        foreach (var job in context.QueuedJobs)
        {
            double age = Seconds(now - job.Request.CreatedAt);
            double duration = job.Request.EstimatedWorkSeconds / speed;
            double expected = (double)job.Request.Revenue * Survival(age + BacklogHorizon / 2 + duration + PublishOverhead);
            if (expected > 0 && expected >= duration * price * WorthFactor)
            {
                backlog += job.Request.EstimatedWorkSeconds;
            }
        }
        int busyWorkers = workers.Count(w => w.AssignedJobs > 0);
        double need = workRate / speed + backlog / speed / BacklogHorizon;
        int rawWorkers = (int)Math.Ceiling(need - WorkerRoundingSlack);
        if (revenueRate >= price * KeepAliveMargin || backlog > 0)
        {
            rawWorkers = Math.Max(rawWorkers, 1);
        }
        rawWorkers = Math.Max(rawWorkers, busyWorkers);

        double outstanding = apis.Where(a => a.State == InstanceState.Ready).Sum(a => a.OutstandingCalls) + context.PendingCalls;
        apiLoad += 0.3 * (outstanding - apiLoad);
        int rawApis = Math.Max(1, (int)Math.Ceiling(apiLoad / (apiSpec.Slots * ApiTargetUtilization)));

        double idleLimit = Math.Max(2, 15 * meanGap);
        bool idle = Seconds(now - lastArrival) > idleLimit && context.QueuedJobs.Count == 0 &&
            workers.All(w => w.AssignedJobs == 0) && context.PendingCalls == 0;
        bool apiIdle = !apiTraffic && Seconds(now - lastApiArrival) > idleLimit && context.PendingCalls == 0 && context.QueuedJobs.Count == 0;
        if (firstArrival == null && Seconds(now) < 1)
        {
            idle = apiIdle = false;
        }
        if (idle)
        {
            history.Clear();
            rawWorkers = 0;
        }
        if (idle || apiIdle)
        {
            rawApis = 0;
        }

        history.Enqueue(new PlanSample(now, rawWorkers, rawApis));
        while (history.Count > 0 && Seconds(now - history.Peek().At) > ScaleDownDelay)
        {
            history.Dequeue();
        }
        int targetWorkers = history.Max(h => h.Workers);
        int targetApis = history.Max(h => h.Apis);

        int cuLimit = settings.ComputeUnitLimit;
        targetApis = Math.Clamp(targetApis, 0, settings.MaximumApis);
        int reservedApis = Math.Min(targetApis, 1);
        targetWorkers = Math.Clamp(targetWorkers, 0, Math.Min(settings.MaximumWorkers, (cuLimit - reservedApis * apiSpec.ComputeUnits) / workerSpec.ComputeUnits));
        targetApis = Math.Clamp(targetApis, reservedApis, Math.Max(reservedApis, (cuLimit - targetWorkers * workerSpec.ComputeUnits) / apiSpec.ComputeUnits));

        var retire = new List<long>();
        int desiredWorkers = PlanRole(workers, targetWorkers, IsWorkerUnhealthy, retire);
        int desiredApis = PlanRole(apis, targetApis, IsApiUnhealthy, retire);
        desiredApis = Math.Min(desiredApis, settings.MaximumApis);
        desiredWorkers = Math.Min(desiredWorkers, settings.MaximumWorkers);
        int apiFloor = apis.Count(a => !retire.Contains(a.Id));
        int workerFloor = workers.Count(w => !retire.Contains(w.Id));
        while (desiredApis * apiSpec.ComputeUnits + desiredWorkers * workerSpec.ComputeUnits > cuLimit)
        {
            if (desiredApis > Math.Max(apiFloor, 1))
            {
                desiredApis--;
            }
            else if (desiredWorkers > workerFloor)
            {
                desiredWorkers--;
            }
            else
            {
                break;
            }
        }
        return new CapacityPlan(desiredApis, desiredWorkers, retire.Count == 0 ? null : retire);
    }

    public void Observe(SystemObservation observation)
    {
        Touch(observation.Now);
        switch (observation.Kind)
        {
            case ObservationKind.SimulationStarted:
                settings = observation.Settings ?? settings;
                break;
            case ObservationKind.RpcAttemptCompleted:
                if (observation.EndpointId is long endpointId && tracks.TryGetValue(endpointId, out var endpoint))
                {
                    bool failed = observation.Outcome is RequestOutcome.Timeout or RequestOutcome.Unavailable or RequestOutcome.Rejected;
                    endpoint.FailRate += 0.2 * ((failed ? 1 : 0) - endpoint.FailRate);
                }
                break;
            case ObservationKind.ProbeCompleted:
                if (observation.EndpointId is long probed && tracks.TryGetValue(probed, out var probedTrack))
                {
                    probedTrack.LastProbeAt = now;
                    if (observation.Outcome is RequestOutcome.Success or RequestOutcome.Rejected)
                    {
                        probedTrack.LastAliveProbe = now;
                    }
                }
                break;
            case ObservationKind.WorkerHeartbeatReceived:
                if (observation.EndpointId is long beating)
                {
                    TrackById(beating, InstanceRole.Worker).LastHeartbeat = now;
                }
                break;
            case ObservationKind.JobCompleted:
                if (observation.JobId is long jobId && assignments.Remove(jobId, out var assignment) &&
                    tracks.TryGetValue(assignment.Worker, out var done))
                {
                    double expected = assignment.Work / Math.Max(done.Speed, 0.01);
                    double actual = Seconds(now - assignment.At) - PublishOverhead;
                    double ratio = Math.Clamp(actual / Math.Max(expected, 0.05), 0.5, 8);
                    double current = SlowRatio(done);
                    done.SlowRatio = current + 0.5 * (ratio - current);
                    done.SlowRatioAt = now;
                }
                break;
            case ObservationKind.JobRequeued:
                if (observation.JobId is long requeued)
                {
                    assignments.Remove(requeued);
                }
                break;
            case ObservationKind.RequestFailed:
                if (observation.RequestId is long failedId)
                {
                    assignments.Remove(failedId);
                }
                break;
            case ObservationKind.InstanceStarting:
                if (observation.EndpointId is long starting)
                {
                    TrackById(starting, null).State = InstanceState.Starting;
                }
                break;
            case ObservationKind.InstanceReady:
                if (observation.EndpointId is long ready)
                {
                    var readyTrack = TrackById(ready, null);
                    readyTrack.State = InstanceState.Ready;
                    readyTrack.ReadyAt = now;
                }
                break;
            case ObservationKind.InstanceDraining:
                if (observation.EndpointId is long draining)
                {
                    TrackById(draining, null).State = InstanceState.Draining;
                }
                break;
            case ObservationKind.InstanceStopped:
                if (observation.EndpointId is long stopped)
                {
                    TrackById(stopped, null).State = InstanceState.Stopped;
                }
                break;
        }
    }
    private void Touch(TimeSpan time)
    {
        if (time > now)
        {
            now = time;
        }
    }
    private void SetPending(RequestInfo request)
    {
        pendingKind = request.Kind;
        pendingCreated = request.CreatedAt;
    }
    private (RequestKind Kind, TimeSpan Created) TakePending()
    {
        var result = (pendingKind ?? RequestKind.Metadata, pendingKind == null ? now : pendingCreated);
        pendingKind = null;
        return result;
    }
    private static double Seconds(TimeSpan value) => value.TotalSeconds;
    private static double PatienceLimit(RequestKind kind) => kind switch
    {
        RequestKind.Metadata => MetadataPatience,
        RequestKind.Preview => PreviewPatience,
        _ => MaxGeneratePatience
    };
    private static double Survival(double finishAge)
    {
        if (finishAge <= MinGeneratePatience)
        {
            return 1;
        }
        if (finishAge >= MaxGeneratePatience)
        {
            return 0;
        }
        return (MaxGeneratePatience - finishAge) / (MaxGeneratePatience - MinGeneratePatience);
    }
    private double WorkerPrice() => (double)(settings?.Worker.PricePerSecond ?? 0.3m);

    private void RecordArrival(RequestInfo request)
    {
        int kind = (int)request.Kind;
        if (firstArrival == null)
        {
            firstArrival = request.CreatedAt;
        }
        else
        {
            double gap = Math.Max(0, Seconds(request.CreatedAt - lastArrival));
            meanGap += 0.05 * (gap - meanGap);
        }
        lastArrival = request.CreatedAt;
        if (request.Kind != RequestKind.Metadata)
        {
            lastApiArrival = request.CreatedAt;
        }
        double revenue = (double)request.Revenue;
        if (!revenueSeen[kind])
        {
            revenueSeen[kind] = true;
            meanRevenue[kind] = revenue;
        }
        else
        {
            meanRevenue[kind] += 0.02 * (revenue - meanRevenue[kind]);
        }
        arrivals.Enqueue(new Arrival(request.CreatedAt, request.Kind, request.EstimatedWorkSeconds, revenue));
        PruneArrivals();
        if (request.Kind == RequestKind.Generate)
        {
            return;
        }
        maxDataAge[kind] = Math.Max(maxDataAge[kind], Seconds(request.MaxDataAge));
        if (!keys.TryGetValue(request.ResourceKey, out var stat))
        {
            stat = new KeyStat();
            keys[request.ResourceKey] = stat;
        }
        stat.Score = KeyScore(stat) + 1;
        stat.At = now;
    }
    private void PruneArrivals()
    {
        while (arrivals.Count > 0 && Seconds(now - arrivals.Peek().At) > DemandWindow)
        {
            arrivals.Dequeue();
        }
    }
    private double KeyScore(KeyStat stat) => stat.Score * Math.Exp(-Seconds(now - stat.At) / KeyRateTau);
    private static RequestKind KindOfKey(string key)
    {
        int separator = key.IndexOf(':');
        var prefix = separator < 0 ? key : key[..separator];
        return Enum.TryParse<RequestKind>(prefix, out var kind) ? kind : RequestKind.Metadata;
    }
    private double CacheValue(string key, long size, TimeSpan observedAt)
    {
        int kind = (int)KindOfKey(key);
        double remaining = Seconds(observedAt) + maxDataAge[kind] - Seconds(now) - DeliveryDelay;
        if (remaining <= 0)
        {
            return 0;
        }
        double rate = keys.TryGetValue(key, out var stat) ? KeyScore(stat) / KeyRateTau : 0;
        return rate * remaining * meanRevenue[kind] / Math.Max(size, 1);
    }
    private InstanceTrack Track(EndpointInfo endpoint)
    {
        var track = TrackById(endpoint.Id, endpoint.Role);
        track.Speed = endpoint.Speed;
        if (track.State != InstanceState.Stopped)
        {
            track.State = endpoint.State;
        }
        track.Mode = endpoint.Mode;
        if (endpoint.LastHeartbeat is TimeSpan beat && (track.LastHeartbeat == null || beat > track.LastHeartbeat))
        {
            track.LastHeartbeat = beat;
        }
        return track;
    }
    private InstanceTrack TrackById(long id, InstanceRole? role)
    {
        if (!tracks.TryGetValue(id, out var track))
        {
            track = new InstanceTrack { ReadyAt = now };
            tracks[id] = track;
        }
        if (role is InstanceRole known)
        {
            track.Role = known;
        }
        return track;
    }
    private double SlowRatio(InstanceTrack track) =>
        1 + (track.SlowRatio - 1) * Math.Exp(-Seconds(now - track.SlowRatioAt) / SlowRatioDecay);

    private double EffectiveSpeed(EndpointInfo worker)
    {
        double ratio = tracks.TryGetValue(worker.Id, out var track) ? SlowRatio(track) : 1;
        return Math.Max(worker.Speed / Math.Max(ratio, 1), 0.01);
    }

    private static bool ApiHealthy(IReadOnlyList<CallResultInfo> results)
    {
        int streak = 0;
        for (int i = results.Count - 1; i >= 0; i--)
        {
            var outcome = results[i].Outcome;
            if (outcome is RequestOutcome.Timeout or RequestOutcome.Unavailable)
            {
                if (++streak >= ApiFailureStreak)
                {
                    return false;
                }
                continue;
            }
            break;
        }
        return true;
    }
    private TimeSpan LastSign(InstanceTrack track)
    {
        var sign = track.ReadyAt;
        if (track.LastHeartbeat is TimeSpan beat && beat > sign)
        {
            sign = beat;
        }
        if (track.LastAliveProbe is TimeSpan probe && probe > sign)
        {
            sign = probe;
        }
        return sign;
    }
    private bool WorkerResponsive(InstanceTrack track)
    {
        double interval = Seconds(settings?.HeartbeatInterval ?? TimeSpan.FromMilliseconds(500));
        return Seconds(now - LastSign(track)) <= SuspectHeartbeats * interval;
    }
    private bool WorkerDead(InstanceTrack track) =>
        track.State == InstanceState.Stopped ||
        (track.State == InstanceState.Ready && track.UnhealthySince != null && Seconds(now - LastSign(track)) > DeadSilence);

    private bool IsWorkerUnhealthy(EndpointInfo endpoint) =>
        endpoint.State == InstanceState.Ready && tracks.TryGetValue(endpoint.Id, out var track) && WorkerDead(track);

    private bool IsApiUnhealthy(EndpointInfo endpoint) =>
        endpoint.State == InstanceState.Ready && tracks.TryGetValue(endpoint.Id, out var track) &&
        track.UnhealthySince is TimeSpan since && Seconds(now - since) >= ApiUnhealthyGrace;

    private int PlanRole(List<EndpointInfo> owned, int target, Func<EndpointInfo, bool> unhealthy, List<long> retire)
    {
        var healthy = owned.Where(i => !unhealthy(i)).ToList();
        var broken = owned.Where(unhealthy).ToList();
        int retired = 0;
        if (healthy.Count > target)
        {
            var spare = healthy
                .Where(i => i.State == InstanceState.Starting || (i.AssignedJobs == 0 && i.OutstandingCalls == 0))
                .OrderBy(i => i.State == InstanceState.Starting ? 0 : 1)
                .ThenBy(i => i.Mode == EndpointMode.AllowTraffic ? 1 : 0)
                .ThenBy(EffectiveSpeed)
                .ThenByDescending(i => i.Id)
                .Take(healthy.Count - target)
                .ToList();
            foreach (var instance in spare)
            {
                retire.Add(instance.Id);
                retired++;
            }
        }
        int readyHealthy = healthy.Count(i => i.State == InstanceState.Ready && !retire.Contains(i.Id));
        if (broken.Count > 0 && readyHealthy >= Math.Max(target, 1))
        {
            foreach (var instance in broken)
            {
                retire.Add(instance.Id);
                retired++;
            }
            broken.Clear();
        }
        int desired = owned.Count - retired;
        if (healthy.Count < target)
        {
            desired += target - healthy.Count;
        }
        return desired;
    }

    private sealed class AdaptiveRpcPolicy : IRequestPolicy
    {
        private const int MaxAttempts = 8;
        private static readonly double[] TimeoutLimitMs = { 750, 1500, 3000 };
        private static readonly double[] InitialTimeoutMs = { 250, 500, 500 };

        private readonly AppleAdaptiveProfitPolicy owner;
        private readonly Dictionary<long, CallTrack> calls = new();
        private readonly Lab1RequestPolicy[] lab1 =
        {
            new(InitialTimeoutMs[0], TimeoutLimitMs[0]),
            new(InitialTimeoutMs[1], TimeoutLimitMs[1]),
            new(InitialTimeoutMs[2], TimeoutLimitMs[2])
        };
        private readonly long[] sequence = new long[3];
        private int sinceCleanup;

        public AdaptiveRpcPolicy(AppleAdaptiveProfitPolicy owner)
        {
            this.owner = owner;
        }

        public TimeSpan GetTimeout(RequestContext context, Random random)
        {
            if (!calls.TryGetValue(context.RequestId, out var call))
            {
                var (kind, created) = owner.TakePending();
                call = new CallTrack(kind, created, ++sequence[(int)kind]);
                calls[context.RequestId] = call;
                call.StartedAt = owner.now;
                Cleanup();
            }
            else
            {
                call.StartedAt = call.NextStart ?? owner.now;
            }
            return lab1[(int)call.Kind].GetTimeout(new RequestContext(call.Sequence, context.Attempt), random);
        }

        public RetryDecision OnResult(RequestObservation observation, Random random)
        {
            if (!calls.TryGetValue(observation.RequestId, out var call))
            {
                return observation.Outcome is RequestOutcome.Timeout or RequestOutcome.Unavailable && observation.Attempt < 3
                    ? RetryDecision.RetryAfter(TimeSpan.Zero)
                    : RetryDecision.NoRetry();
            }
            var finishedAt = call.StartedAt + observation.Elapsed;
            owner.Touch(finishedAt);

            RetryDecision decision;
            switch (observation.Outcome)
            {
                case RequestOutcome.Success:
                case RequestOutcome.Timeout:
                    decision = lab1[(int)call.Kind].OnResult(
                        new RequestObservation(call.Sequence, observation.Attempt, observation.Outcome, observation.Elapsed), random);
                    break;
                case RequestOutcome.Unavailable:
                    decision = RetryDecision.RetryAfter(TimeSpan.Zero);
                    break;
                case RequestOutcome.Rejected:
                    decision = RetryDecision.RetryAfter(TimeSpan.FromSeconds((0.02 + 0.06 * random.NextDouble()) * observation.Attempt));
                    break;
                default:
                    decision = RetryDecision.NoRetry();
                    break;
            }

            double remaining = Seconds(call.Created - finishedAt) + PatienceLimit(call.Kind);
            if (!decision.Retry || observation.Attempt >= MaxAttempts || remaining <= 0.01)
            {
                calls.Remove(observation.RequestId);
                return RetryDecision.NoRetry();
            }
            double delay = Math.Min(decision.Delay.TotalSeconds, Math.Max(0, remaining - 0.01));
            call.NextStart = finishedAt + TimeSpan.FromSeconds(delay);
            return RetryDecision.RetryAfter(TimeSpan.FromSeconds(delay));
        }
        private void Cleanup()
        {
            if (++sinceCleanup < 500)
            {
                return;
            }
            sinceCleanup = 0;
            var expired = calls.Where(c => Seconds(owner.now - c.Value.Created) > 2 * MaxGeneratePatience).Select(c => c.Key).ToList();
            foreach (long id in expired)
            {
                calls.Remove(id);
            }
        }

        private sealed class CallTrack
        {
            public RequestKind Kind { get; }
            public TimeSpan Created { get; }
            public long Sequence { get; }
            public TimeSpan StartedAt { get; set; }
            public TimeSpan? NextStart { get; set; }

            public CallTrack(RequestKind kind, TimeSpan created, long sequence)
            {
                Kind = kind;
                Created = created;
                Sequence = sequence;
            }
        }
    }

    private sealed class Lab1RequestPolicy : IRequestPolicy
    {
        private const int WindowSize = 600;
        private const int MinSamples = 60;
        private const int RecomputeEvery = 20;
        private const int MaxAttempts = 8;
        private const double MinTimeoutMs = 1;

        private const double RequiredGain = 0.9;
        private const double NearOptimum = 1.05;
        private const double MaxRestartShare = 0.15;
        private const double MaxClustering = 2;
        private const int MinClusterEvidence = 3;
        private const double TailMargin = 2;
        private const double InteriorMargin = 1.05;
        private const double MaxDecrease = 0.85;

        private const double GuardAlpha = 0.05;
        private const double GuardMinRate = 0.25;
        private const int GuardCooldown = 10;
        private const double GuardGrowth = 4;

        private const double RetryCheckAlpha = 0.1;
        private const int RetryCheckMinCount = 20;

        private readonly double MaxTimeoutMs;
        private readonly Sample[] window = new Sample[WindowSize];
        private int windowCount;
        private int windowNext;
        private int sinceRecompute;
        private int sinceGuard = GuardCooldown;

        private double timeoutMs;
        private double expectedTimeoutRate;
        private double recentTimeoutRate;

        private bool restartsTrusted = true;
        private double retryFastRate = 1;
        private int retriesChecked;
        public Lab1RequestPolicy(double initialTimeoutMs, double maxTimeoutMs)
        {
            timeoutMs = initialTimeoutMs;
            MaxTimeoutMs = maxTimeoutMs;
        }

        public TimeSpan GetTimeout(RequestContext context, Random random) =>
            TimeSpan.FromMilliseconds(TimeoutFor(context.Attempt));

        public RetryDecision OnResult(RequestObservation observation, Random random)
        {
            bool timedOut = observation.Outcome == RequestOutcome.Timeout;
            double elapsedMs = observation.Elapsed.TotalMilliseconds;
            Add(new Sample(observation.RequestId, observation.Attempt, elapsedMs, timedOut));

            if (observation.Attempt == 1)
            {
                UpdateGuard(timedOut);
            }
            else if (observation.Attempt == 2)
            {
                CheckRetryIndependence(!timedOut && elapsedMs <= timeoutMs);
            }

            if (++sinceRecompute >= RecomputeEvery && windowCount >= MinSamples)
            {
                sinceRecompute = 0;
                Recompute();
            }

            if (!timedOut || observation.Attempt >= MaxAttempts)
            {
                return RetryDecision.NoRetry();
            }

            if (!IsOverloaded())
            {
                return RetryDecision.RetryAfter(TimeSpan.Zero);
            }

            double backoff = TimeoutFor(observation.Attempt) * random.NextDouble();
            return RetryDecision.RetryAfter(TimeSpan.FromMilliseconds(backoff));
        }

        private double TimeoutFor(int attempt)
        {
            double value = timeoutMs * Math.Pow(2, Math.Min(attempt - 1, 20));
            return Math.Clamp(value, MinTimeoutMs, MaxTimeoutMs);
        }

        private bool IsOverloaded() => recentTimeoutRate > Math.Max(GuardMinRate, 2 * expectedTimeoutRate + 0.15);

        private void UpdateGuard(bool timedOut)
        {
            recentTimeoutRate += GuardAlpha * ((timedOut ? 1 : 0) - recentTimeoutRate);
            sinceGuard++;
            if (!IsOverloaded() || sinceGuard < GuardCooldown)
            {
                return;
            }

            sinceGuard = 0;
            timeoutMs = Math.Min(timeoutMs * GuardGrowth, MaxTimeoutMs);
            expectedTimeoutRate = 0;
            windowCount = 0;
            windowNext = 0;
            sinceRecompute = 0;
        }

        private void CheckRetryIndependence(bool fast)
        {
            retryFastRate += RetryCheckAlpha * ((fast ? 1 : 0) - retryFastRate);
            retriesChecked++;
            if (retriesChecked >= RetryCheckMinCount && retryFastRate < 0.5 * (1 - expectedTimeoutRate))
            {
                restartsTrusted = false;
            }
        }

        private void Add(Sample sample)
        {
            window[windowNext] = sample;
            windowNext = (windowNext + 1) % WindowSize;
            windowCount = Math.Min(windowCount + 1, WindowSize);
        }

        private void Recompute()
        {
            var samples = new Sample[windowCount];
            Array.Copy(window, samples, windowCount);
            Array.Sort(samples, (a, b) => a.Value != b.Value ? a.Value.CompareTo(b.Value) : a.Censored.CompareTo(b.Censored));

            var candidates = new List<Candidate>();
            double survival = 1;
            double area = 0;
            double previous = 0;
            int atRisk = samples.Length;
            foreach (var sample in samples)
            {
                area += survival * (sample.Value - previous);
                previous = sample.Value;
                if (!sample.Censored)
                {
                    survival *= 1 - 1.0 / atRisk;
                    if (survival <= MaxRestartShare)
                    {
                        candidates.Add(new Candidate(sample.Value, area / (1 - survival), survival));
                    }
                }
                atRisk--;
            }

            if (candidates.Count == 0)
            {
                timeoutMs = Math.Min(timeoutMs * 2, MaxTimeoutMs);
                expectedTimeoutRate = 0;
                return;
            }

            double noRestartLatency = area;
            var last = candidates[^1];
            double best = candidates.Min(candidate => candidate.ExpectedLatency);
            var chosen = candidates.Last(candidate => candidate.ExpectedLatency <= best * NearOptimum);

            if (restartsTrusted && best < RequiredGain * noRestartLatency && !IsClustered(samples, chosen.Value))
            {
                SetTimeout(chosen.Value * InteriorMargin);
                expectedTimeoutRate = chosen.Survival;
            }
            else
            {
                SetTimeout(last.Value * TailMargin);
                expectedTimeoutRate = last.Survival;
            }
        }
        private void SetTimeout(double value) =>
            timeoutMs = Math.Clamp(Math.Max(value, timeoutMs * MaxDecrease), MinTimeoutMs, MaxTimeoutMs);
        private static bool IsClustered(Sample[] samples, double threshold)
        {
            var first = samples.Where(sample => sample.Attempt == 1).OrderBy(sample => sample.RequestId).ToArray();
            int slow = 0, pairs = 0, slowAfterSlow = 0;
            for (int i = 0; i < first.Length; i++)
            {
                bool isSlow = first[i].Censored || first[i].Value > threshold;
                if (isSlow)
                {
                    slow++;
                }
                if (i == 0 || first[i].RequestId != first[i - 1].RequestId + 1)
                {
                    continue;
                }
                if (!(first[i - 1].Censored || first[i - 1].Value > threshold))
                {
                    continue;
                }
                pairs++;
                if (isSlow)
                {
                    slowAfterSlow++;
                }
            }
            if (slowAfterSlow < MinClusterEvidence)
            {
                return false;
            }
            double overall = (double)slow / first.Length;
            double afterSlow = (double)slowAfterSlow / pairs;
            return afterSlow > MaxClustering * overall + 0.1;
        }

        private readonly struct Sample
        {
            public long RequestId { get; }
            public int Attempt { get; }
            public double Value { get; }
            public bool Censored { get; }

            public Sample(long requestId, int attempt, double value, bool censored)
            {
                RequestId = requestId;
                Attempt = attempt;
                Value = value;
                Censored = censored;
            }
        }
        private readonly struct Candidate
        {
            public double Value { get; }
            public double ExpectedLatency { get; }
            public double Survival { get; }

            public Candidate(double value, double expectedLatency, double survival)
            {
                Value = value;
                ExpectedLatency = expectedLatency;
                Survival = survival;
            }
        }
    }
    private sealed class InstanceTrack
    {
        public InstanceRole Role { get; set; }
        public InstanceState State { get; set; } = InstanceState.Starting;
        public EndpointMode Mode { get; set; }
        public double Speed { get; set; } = 1;
        public TimeSpan ReadyAt { get; set; }
        public TimeSpan? LastHeartbeat { get; set; }
        public TimeSpan? LastProbeAt { get; set; }
        public TimeSpan? LastAliveProbe { get; set; }
        public TimeSpan? UnhealthySince { get; set; }
        public double SlowRatio { get; set; } = 1;
        public TimeSpan SlowRatioAt { get; set; }
        public double FailRate { get; set; }
    }
    private sealed class KeyStat
    {
        public double Score { get; set; }
        public TimeSpan At { get; set; }
    }
    private readonly struct Arrival
    {
        public TimeSpan At { get; }
        public RequestKind Kind { get; }
        public double Work { get; }
        public double Revenue { get; }

        public Arrival(TimeSpan at, RequestKind kind, double work, double revenue)
        {
            At = at;
            Kind = kind;
            Work = work;
            Revenue = revenue;
        }
    }
    private readonly struct Assignment
    {
        public TimeSpan At { get; }
        public long Worker { get; }
        public double Work { get; }

        public Assignment(TimeSpan at, long worker, double work)
        {
            At = at;
            Worker = worker;
            Work = work;
        }
    }
    private readonly struct PlanSample
    {
        public TimeSpan At { get; }
        public int Workers { get; }
        public int Apis { get; }

        public PlanSample(TimeSpan at, int workers, int apis)
        {
            At = at;
            Workers = workers;
            Apis = apis;
        }
    }
}
