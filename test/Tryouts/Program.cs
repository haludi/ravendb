using System;
using System.Diagnostics;
using System.IO;
using System.Threading.Tasks;
using FastTests.Blittable;
using FastTests.Client;
using RachisTests;
using Raven.Server.ServerWide.Commands;
using SlowTests.Client.Attachments;
using SlowTests.Client.TimeSeries.Replication;
using SlowTests.Issues;
using SlowTests.MailingList;
using SlowTests.Rolling;
using SlowTests.Server.Documents.ETL.Raven;
using Sparrow.Logging;
using Tests.Infrastructure;

namespace Tryouts
{
    public static class Program
    {
        static Program()
        {
            XunitLogging.RedirectStreams = false;
        }

        public static async Task Main(string[] args)
        {
            Console.WriteLine(Process.GetCurrentProcess().Id);
            for (int i = 0; i < 10000; i++)
            {
                Console.WriteLine($"Starting to run {i}");
                try
                {
                    if(Debugger.IsAttached == false)
                        LoggingSource.Instance.SetupLogMode(LogMode.Information, "/home/haludi/work/ravendb/RavenDB-21170/temp", TimeSpan.MaxValue, Int64.MaxValue, false);

                    using (var testOutputHelper = new ConsoleTestOutputHelper())
                    using (var test = new MyTest2(testOutputHelper))
                    {
                        await test.AAAAAAAAAAAAAA_______________();
                    }

                    if (ClusterTransactionCommand.A)
                    {
                        break;
                    }
                    else
                    {
                        LoggingSource.Instance.SetupLogMode(LogMode.None, "/home/haludi/work/ravendb/RavenDB-21170/temp", TimeSpan.MaxValue, Int64.MaxValue, false);

                        var di = new DirectoryInfo("/home/haludi/work/ravendb/RavenDB-21170/temp");
                        foreach (FileInfo file in di.GetFiles())
                        {
                            try
                            {
                                file.Delete();
                            }
                            catch
                            {
                                // ignored
                            }
                        }
                    }
                }
                catch (Exception e)
                {
                    Console.ForegroundColor = ConsoleColor.Red;
                    Console.WriteLine(e);
                    Console.ForegroundColor = ConsoleColor.White;
                }
            }
        }
    }
}
