using System.Diagnostics;
using System.Globalization;
using SPS.Abstractions;
using SPS.Bench;
using SPS.Simulation;
using SPS.Simulation.Public;
using SPS.Student;

CultureInfo.CurrentCulture = CultureInfo.InvariantCulture;
int seeds = args.Length > 0 ? int.Parse(args[0]) : 5;
string? filter = args.Length > 1 ? args[1] : null;
bool detailed = args.Contains("--detail");

var policies = new (string Name, Func<ISystemPolicy> Create)[]
{
    ("Sample", () => new SampleSystemPolicy()),
    ("FixedMax", () => new FixedCapacityPolicy()),
    ("Adaptive", () => new AppleAdaptiveProfitPolicy())
};

var watch = Stopwatch.StartNew();
var totals = policies.ToDictionary(p => p.Name, _ => 0m);
Console.WriteLine($"Seeds: 1..{seeds}");
Console.WriteLine($"{"Scenario",-34}{"Policy",-10}{"Profit",10}{"Revenue",10}{"API",8}{"Worker",9}{"Meta%",8}{"Prev%",8}{"Gen%",8}{"Dup",6}{"Stale",6}");
foreach (var scenario in BenchScenarios.All())
{
    if (filter != null && !scenario.Name.Contains(filter, StringComparison.OrdinalIgnoreCase))
    {
        continue;
    }
    foreach (var policy in policies)
    {
        var results = Enumerable.Range(1, seeds)
            .AsParallel()
            .Select(seed => new SimulationRunner().Run(scenario, policy.Create(), new SimulationOptions(seed)))
            .ToList();
        decimal profit = results.Average(r => r.Economy.Profit);
        totals[policy.Name] += profit;
        double Rate(RequestKind kind) => results.Average(r => r.RequestTypes.Single(t => t.Kind == kind).OnTimeRate) * 100;
        Console.WriteLine($"{Trim(scenario.Name, 33),-34}{policy.Name,-10}{profit,10:F1}{results.Average(r => r.Economy.Revenue),10:F1}" +
            $"{results.Average(r => r.Economy.ApiCost),8:F1}{results.Average(r => r.Economy.WorkerCost),9:F1}" +
            $"{Rate(RequestKind.Metadata),8:F1}{Rate(RequestKind.Preview),8:F1}{Rate(RequestKind.Generate),8:F1}" +
            $"{results.Average(r => r.Diagnostics.DuplicateJobExecutions),6:F1}{results.Average(r => r.Diagnostics.StaleResponses),6:F1}");
        if (detailed && policy.Name == "Adaptive")
        {
            var d = results[0].Diagnostics;
            Console.WriteLine($"    seed1: timeouts {d.Timeouts} retries {d.Retries} rejections {d.Rejections} policyRej {d.PolicyRejections} hits {d.CacheHits} fetches {d.Fetches} reassign {d.JobReassignments} wasted {d.WastedInstanceTime.TotalSeconds:F1}s apiSec {d.ApiInstanceSeconds:F0} workerSec {d.WorkerInstanceSeconds:F0}");
        }
    }
}
Console.WriteLine();
foreach (var total in totals)
{
    Console.WriteLine($"Sum of mean profits, {total.Key}: {total.Value:F1}");
}
Console.WriteLine($"Elapsed: {watch.Elapsed.TotalSeconds:F1}s");

static string Trim(string value, int length) => value.Length <= length ? value : value[..length];
