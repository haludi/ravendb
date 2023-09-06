using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Raven.Client.Documents;
using Raven.Client.Documents.Session;
using Raven.Server.Config;
using Raven.Server.ServerWide.Commands;
using Raven.Tests.Core.Utils.Entities;
using Sparrow.Logging;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace RachisTests;

public class MyTest1 : ClusterTestBase
{
        
    [Fact]
    public async Task AAAAAAAAAAAAAA_______________()
    {
        var loggingSource = new LoggingSource(LogMode.Operations, "/home/haludi/work/ravendb/RavenDB-21170/temp", "Test", TimeSpan.MaxValue, Int64.MaxValue);
        ClusterTransactionCommand.Logger = loggingSource.GetLogger("Test", "Test");
            
        var db = "test";
        var (nodes, leader) = await CreateRaftCluster(3, shouldRunInMemory: false);
        // await CreateDatabaseInCluster(db, 3, leader.WebUrl);
        var nonLeader = nodes.First(x => ReferenceEquals(x, leader) == false);

        // await Task.Delay(1000);
        OpenBrowser($"{leader.WebUrl}/studio/index.html#admin/settings/cluster");
        OpenBrowser($"{leader.WebUrl}/studio/index.html#databases/manageDatabaseGroup?&database=test");
        OpenBrowser($"{leader.WebUrl}/studio/index.html#admin/settings/trafficWatch");

        using (var store = GetDocumentStore(new Options{Server = leader, RunInMemory = false, ModifyDatabaseName = _ => db, ReplicationFactor = 3}))
        {
            var result = await DisposeServerAndWaitForFinishOfDisposalAsync(nonLeader);

            await WhenAll(store);
            WaitForUserToContinueTheTest(store);

            using var newServer = GetNewServer(new ServerCreationOptions
            {
                DeletePrevious = false,
                RunInMemory = false,
                DataDirectory = result.DataDirectory,
                CustomSettings = new Dictionary<string, string> {[RavenConfiguration.GetKey(x => x.Core.ServerUrls)] = result.Url}
            });

            Console.WriteLine($"Started");

            await WhenAll(store);

            WaitForUserToContinueTheTest(store);
        }
    }

    private static async Task WhenAll(IDocumentStore store)
    {
        var tasks = new List<Task>();
        var iterates = 1_000_000 / Environment.ProcessorCount;
        var current = 0;
        for (var i = 0; i < Environment.ProcessorCount; i++)
        {
            tasks.Add(Task.Run(() =>
            {
                for (int j = 0; j < iterates; j++)
                {
                    if(j % 1000 == 0)
                        Console.WriteLine(j);
                        
                    try
                    {
                        using (var session = store.OpenSession(new SessionOptions {TransactionMode = TransactionMode.ClusterWide, DisableAtomicDocumentWritesInClusterWideTransaction = true}))
                        {
                            session.Store(new User(), $"User/{j}");
                            session.SaveChanges();
                        }
                    }
                    catch
                    {
                        // ignored
                    }
                }
            }));
        }

        await Task.WhenAll(tasks);
    }

    public MyTest1(ITestOutputHelper output) : base(output)
    {
    }
}
