using System;
using System.Diagnostics;
using System.Threading.Tasks;
using SlowTests.Client.Counters;
using SlowTests.Cluster;
using SlowTests.Issues;
using SlowTests.Server.Documents.ETL;
using SlowTests.Server.Documents.ETL.Raven;
using SlowTests.Voron;
using Tests.Infrastructure;
using Xunit.Sdk;

namespace Tryouts
{
    public static class Program
    {
        static Program()
        {
            XunitLogging.RedirectStreams = false;
        }
        private const string _script4 = @"
var user = loadToUsers(this);
user.addTimeSeries(loadTimeSeries('Heartrate', new Date(2020, 3, 26), new Date(2020, 3, 28)));
function loadTimeSeriesOfUsersBehavior(doc, ts)
{
    return false;
};
";// the month is 0-indexed

        public static async Task Main(string[] args)
        {
            Console.WriteLine(Process.GetCurrentProcess().Id);
            for (int i = 0; i < 1; i++)
            {
                Console.WriteLine($"Starting to run {i}");
                try
                {
                    using (var testOutputHelper = new ConsoleTestOutputHelper())
                    using (var test = new EtlTimeSeriesTests(testOutputHelper))
                    {
                        await test.RavenEtlWithTimeSeries_WhenStoreDocumentAndMultipleSegmentOfTimeSeriesInSameSession_ShouldDestBeAsSrc("", new[] {"Users"}, _script4);
                    }
                }
                catch (Exception e)
                {
                    Console.ForegroundColor = ConsoleColor.Red;
                    Console.WriteLine(e);
                    Console.ForegroundColor = ConsoleColor.White;
                   // Console.ReadLine();
                }
            }
        }
    }
}
