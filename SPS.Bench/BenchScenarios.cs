using SPS.Abstractions;
using SPS.Simulation;
using SPS.Simulation.Public;

namespace SPS.Bench;

public static class BenchScenarios
{
    private static readonly WorkloadPhase[] PublicPhases =
    {
        new(0, 12, .05, .30, .85),
        new(40, 36, .08, .35, .96),
        new(70, 18, .06, .30, .50, 30),
        new(110, 24, .10, .20, .85, 50),
        new(150, 5, .03, .30, .80, 50)
    };

    public static IEnumerable<SimulationScenario> All()
    {
        yield return PublicScenario.Create();

        yield return Build("Advanced settings, public load", true, PublicPhases,
            new FaultInterval(45, 49, FaultKind.Crash, InstanceRole.Api, 1),
            new FaultInterval(98, 102, FaultKind.Crash, InstanceRole.Worker, 1));

        yield return Build("Advanced + price variants", true, PublicPhases, Variants(),
            new FaultInterval(60, 66, FaultKind.Crash, InstanceRole.Worker, 0));

        yield return Build("Advanced, heavy load", true, new WorkloadPhase[]
        {
            new(0, 30, .08, .30, .9),
            new(30, 60, .10, .35, .9, 10),
            new(90, 45, .12, .25, .7, 20, 8),
            new(140, 20, .06, .30, .8, 40)
        });

        yield return Build("Advanced, light load", true, new WorkloadPhase[]
        {
            new(0, 4, .04, .30, .8),
            new(60, 8, .05, .30, .8, 10),
            new(120, 2, .03, .30, .8, 20)
        });

        yield return Build("Bursty traffic", true, Enumerable.Range(0, 12)
            .Select(i => new WorkloadPhase(i * 15, i % 2 == 0 ? 6 : 50, i % 2 == 0 ? .04 : .09, .3, .85, (i / 2) * 7))
            .ToArray());

        yield return Build("Spread keys, frequent shifts", true, Enumerable.Range(0, 9)
            .Select(i => new WorkloadPhase(i * 20, 24, .06, .35, .45, i * 9, 12))
            .ToArray());

        yield return Build("Worker faults: isolation + slowdown", true, PublicPhases,
            new FaultInterval(20, 40, FaultKind.HeartbeatIsolation, InstanceRole.Worker, 0),
            new FaultInterval(50, 90, FaultKind.Slowdown, InstanceRole.Worker, 1, 4),
            new FaultInterval(100, 115, FaultKind.NetworkIsolation, InstanceRole.Worker, 2),
            new FaultInterval(120, 135, FaultKind.Crash, InstanceRole.Worker, 0));

        yield return Build("API faults + storage slowdown", true, PublicPhases,
            new FaultInterval(20, 35, FaultKind.Crash, InstanceRole.Api, 0),
            new FaultInterval(50, 65, FaultKind.NetworkLoss, InstanceRole.Api, 0),
            new FaultInterval(80, 95, FaultKind.NetworkIsolation, InstanceRole.Api, 1),
            new FaultInterval(105, 125, FaultKind.StorageSlowdown, InstanceRole.Api, 0, 4),
            new FaultInterval(140, 150, FaultKind.Slowdown, InstanceRole.Api, 0, 6));

        var lossy = Build("Public settings, lossy network", false, PublicPhases,
            new FaultInterval(30, 60, FaultKind.NetworkLoss, InstanceRole.Worker, 0),
            new FaultInterval(70, 100, FaultKind.NetworkLoss, InstanceRole.Api, 0));
        yield return new SimulationScenario(lossy.Name, lossy.GenerationDuration, lossy.Settings, lossy.Profiles, lossy.Phases, lossy.Faults)
        { ResourceKeyCount = 80, ResponseLossProbability = .03, NetworkMaxSeconds = .06 };
    }

    private static SimulationScenario Build(string name, bool advanced, WorkloadPhase[] phases, params FaultInterval[] faults) =>
        Build(name, advanced, phases, null, faults);

    private static SimulationScenario Build(string name, bool advanced, WorkloadPhase[] phases,
        IReadOnlyList<RequestProfile>? profiles, params FaultInterval[] faults) =>
        new(name, TimeSpan.FromSeconds(180), PublicScenario.CreateSettings(advanced),
            profiles ?? PublicScenario.CreateProfiles(advanced), Array.AsReadOnly(phases), Array.AsReadOnly(faults))
        { ResourceKeyCount = 80, ResponseLossProbability = .005 };

    private static IReadOnlyList<RequestProfile> Variants()
    {
        var baseline = PublicScenario.CreateProfiles(true);
        return Array.AsReadOnly(new[]
        {
            new RequestProfile(RequestKind.Metadata, .05m, .5, 1.5, 0, .015, 1, 1, 2, 1024)
            {
                Variants = new[] { new RequestVariant(.03m, 2, 2), new RequestVariant(.10m, 1, 1) }
            },
            new RequestProfile(RequestKind.Preview, .20m, 1, 3, .015, .060, 1, 1, 5, 16384)
            {
                Variants = new[] { new RequestVariant(.12m, 5, 2), new RequestVariant(.45m, 1.5, 1) }
            },
            new RequestProfile(RequestKind.Generate, 5m, 15, 60, .010, .120, 2, 12, 1, 1024)
            {
                Variants = new[] { new RequestVariant(1.5m, 1, 1), new RequestVariant(5m, 1, 2), new RequestVariant(11m, 1, 1) }
            }
        });
    }
}
