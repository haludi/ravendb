﻿using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents;
using Raven.Client.Documents.Operations.ConnectionStrings;
using Raven.Client.Documents.Operations.ETL;
using Raven.Client.Documents.Operations.OngoingTasks;
using Raven.Client.Util;
using Raven.Server.Documents.ETL;
using Raven.Server.NotificationCenter;
using Sparrow.Json;
using Tests.Infrastructure.Utils;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Server.Documents.ETL
{
    [Trait("Category", "ETL")]
    public abstract class EtlTestBase : RavenTestBase
    {
        protected EtlTestBase(ITestOutputHelper output) : base(output)
        {
        }

        protected AddEtlOperationResult AddEtl<T>(DocumentStore src, EtlConfiguration<T> configuration, T connectionString) where T : ConnectionString
        {
            var putResult = src.Maintenance.Send(new PutConnectionStringOperation<T>(connectionString));
            Assert.NotNull(putResult.RaftCommandIndex);

            var addResult = src.Maintenance.Send(new AddEtlOperation<T>(configuration));
            return addResult;
        }

        protected AddEtlOperationResult AddEtl(DocumentStore src, DocumentStore dst, string collection, string script, bool applyToAllDocuments = false, bool disabled = false, string mentor = null)
        {
            return AddEtl(src, dst, new[] { collection }, script, applyToAllDocuments, disabled, mentor);
        }

        protected AddEtlOperationResult AddEtl(DocumentStore src, DocumentStore dst, IEnumerable<string> collections, string script, bool applyToAllDocuments = false, bool disabled = false, string mentor = null)
        {
            var connectionStringName = $"{src.Database}@{src.Urls.First()} to {dst.Database}@{dst.Urls.First()}";

            return AddEtl(src, new RavenEtlConfiguration()
            {
                Name = connectionStringName,
                ConnectionStringName = connectionStringName,
                Transforms =
                    {
                        new Transformation
                        {
                            Name = $"ETL : {connectionStringName}",
                            Collections = new List<string>(collections),
                            Script = script,
                            ApplyToAllDocuments = applyToAllDocuments,
                            Disabled = disabled
                        }
                    },
                MentorNode = mentor
            },
                new RavenConnectionString
                {
                    Name = connectionStringName,
                    Database = dst.Database,
                    TopologyDiscoveryUrls = dst.Urls,
                }
            );
        }

        protected ManualResetEventSlim WaitForEtl(DocumentStore store, Func<string, EtlProcessStatistics, bool> predicate)
        {
            var database = GetDatabase(store.Database).Result;

            var mre = new ManualResetEventSlim();

            database.EtlLoader.BatchCompleted += x =>
            {
                if (predicate($"{x.ConfigurationName}/{x.TransformationName}", x.Statistics))
                    mre.Set();
            };


            return mre;
        }
        
        protected async Task<(string, string, EtlProcessStatistics)> WaitForEtlAsync(DocumentStore store, Func<string, EtlProcessStatistics, bool> predicate, TimeSpan timeout)
        {
            var database = GetDatabase(store.Database).Result;

            var taskCompletionSource = new TaskCompletionSource<(string, string, EtlProcessStatistics)>();

            void EtlLoaderOnBatchCompleted((string ConfigurationName, string TransformationName, EtlProcessStatistics Statistics) x)
            {
                try
                {
                    if (predicate($"{x.ConfigurationName}/{x.TransformationName}", x.Statistics) == false) 
                        return;
                    taskCompletionSource.SetResult(x);
                }
                catch (Exception e)
                {
                    taskCompletionSource.SetException(e);
                }
            }

            database.EtlLoader.BatchCompleted += EtlLoaderOnBatchCompleted;
            var whenAny = await Task.WhenAny(taskCompletionSource.Task, Task.Delay(timeout));
            database.EtlLoader.BatchCompleted -= EtlLoaderOnBatchCompleted;

            if(whenAny != taskCompletionSource.Task)
                throw new TimeoutException($"Etl predicate timeout - {timeout}");

            return await taskCompletionSource.Task;
        }
        
        protected void ThrowWithEtlErrors(DocumentStore src, Exception e = null)
        {
            string[] notifications = GetEtlErrorNotifications(src);

            string message = string.Join(",\n", notifications);
            var additionalDetails = new InvalidOperationException(message);
            if (e == null)
                throw additionalDetails;
                
            throw new AggregateException(e, additionalDetails);
        }

        private string[] GetEtlErrorNotifications(DocumentStore src)
        {
            string[] notifications;
            var databaseInstanceFor = GetDocumentDatabaseInstanceFor(src);
            using (databaseInstanceFor.Result.NotificationCenter.GetStored(out IEnumerable<NotificationTableValue> storedNotifications, postponed: false))
            {
                notifications = storedNotifications
                    .Select(n => n.Json)
                    .Where(n => n.TryGet("AlertType", out string type) && type.StartsWith("Etl_"))
                    .Where(n => n.TryGet("Details", out BlittableJsonReaderObject _))
                    .Select(n =>
                    {
                        n.TryGet("Details", out BlittableJsonReaderObject details);
                        return details.ToString();
                    }).ToArray();
            }

            return notifications;
        }
        
        protected IAsyncDisposable OpenEtlOffArea(IDocumentStore store, long etlTaskId, bool cleanTombstones = false)
        {
            store.Maintenance.Send(new ToggleOngoingTaskStateOperation(etlTaskId, OngoingTaskType.RavenEtl, true));
            return new DisposableAsyncAction(async () =>
            {
                if (cleanTombstones)
                {
                    var srcDatabase = await GetDatabase(store.Database);
                    await srcDatabase.TombstoneCleaner.ExecuteCleanup();    
                } 
                
                store.Maintenance.Send(new ToggleOngoingTaskStateOperation(etlTaskId, OngoingTaskType.RavenEtl, false));
            });
        }
    }
}
