using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client.Documents;
using Raven.Client.Documents.Session;
using Raven.Client.ServerWide.Operations;
using Raven.Server.Config;
using Raven.Server.ServerWide.Commands;
using Raven.Tests.Core.Utils.Entities;
using Sparrow.Logging;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace RachisTests;

public class MyTest2 : ClusterTestBase
{
    public MyTest2(ITestOutputHelper output) : base(output)
    {
    }
    
    [Fact]
    public async Task AAAAAAAAAAAAAA_______________()
    {
        // var loggingSource = new LoggingSource(LogMode.Operations, "/home/haludi/work/ravendb/RavenDB-21170/temp", "Test", TimeSpan.MaxValue, Int64.MaxValue);
        ClusterTransactionCommand.Logger = LoggingSource.Instance.GetLogger("Test", "Test");
        
        var db = "test";
        var (_, leader) = await CreateRaftCluster(3);
        await CreateDatabaseInCluster(db, 3, leader.WebUrl);
        var nonLeader = Servers.First(x => ReferenceEquals(x, leader) == false);

        await Task.Delay(1000);
        var result = await DisposeServerAndWaitForFinishOfDisposalAsync(nonLeader);
        Console.WriteLine(leader.WebUrl);

        using (var store = new DocumentStore
               {
                   Database = db,
                   Urls = new[] { leader.WebUrl }
               }.Initialize())
        {
            try
            {
                var tasks = new List<Task>();
                var count = 0L;
                for (var i = 0; i < Environment.ProcessorCount; i++)
                {
                    tasks.Add(Task.Run(() =>
                    {

                        while (true)
                        {
                            long increment = Interlocked.Increment(ref count);
                            if (increment % 10000 == 0)
                                Console.WriteLine(increment);

                            if (increment > 1_000_000)
                                break;

                            using (var session = store.OpenSession(new SessionOptions {TransactionMode = TransactionMode.ClusterWide}))
                            {
                                session.Store(new User());
                                session.SaveChanges();
                            }
                        }
                    }));
                }

                await Task.WhenAll(tasks);
            }
            catch
            {
            }
            // WaitForUserToContinueTheTest(store);

            // using var newServer = GetNewServer(new ServerCreationOptions
            // {
            //     DeletePrevious = false,
            //     RunInMemory = false,
            //     DataDirectory = result.DataDirectory,
            //     CustomSettings = new Dictionary<string, string> { [RavenConfiguration.GetKey(x => x.Core.ServerUrls)] = result.Url }
            // });

            /*Console.WriteLine($"Started");

            for (var i = 0; i < 1_000_000; i++)
            {
                if(i % 10000 == 0)
                    Console.WriteLine(i);
                
                using (var session = store.OpenSession(new SessionOptions { TransactionMode = TransactionMode.ClusterWide }))
                {
                    session.Store(new User());
                    session.SaveChanges();
                }
            }*/
            try
            {
                store.Maintenance.Server.Send(new DeleteDatabasesOperation("test", hardDelete: true));
            }
            catch
            {
                // ignored
            }
        }
    }
}
