﻿using System;
using Raven.Client.Documents.Subscriptions;
using Raven.Client.Json.Serialization;
using Raven.Client.ServerWide;
using Raven.Client.ServerWide.Sharding;
using Raven.Server.Rachis;
using Raven.Server.ServerWide.Context;
using Raven.Server.Utils;
using Sparrow.Json.Parsing;
using Sparrow.Logging;
using Voron;
using Voron.Data.Tables;

namespace Raven.Server.ServerWide.Commands.Sharding
{
    public class StartBucketMigrationCommand : UpdateDatabaseCommand
    {
        public int DestinationShard;
        public int Bucket;

        private ShardBucketMigration _migration;

        public StartBucketMigrationCommand()
        {
        }

        public StartBucketMigrationCommand(int bucket, int destShard, string database, string raftId) : base(database, raftId)
        {
            Bucket = bucket;
            DestinationShard = destShard;
        }

        public override void UpdateDatabaseRecord(DatabaseRecord record, long etag)
        {
            var sourceShard = ShardHelper.GetShardNumberFor(record.Sharding, Bucket);
            if (sourceShard == DestinationShard)
                return; // nothing to do

            if (record.Sharding.BucketMigrations.Count > 0)
            {
                foreach (var migration in record.Sharding.BucketMigrations)
                {
                    if (migration.Value.Status < MigrationStatus.OwnershipTransferred)
                        throw new RachisApplyException(
                            $"Only one bucket can be transferred at a time, currently bucket {migration.Key} is {migration.Value.Status}");

                    if (migration.Key == Bucket)
                        throw new RachisApplyException($"Can't migrate bucket {Bucket}, since it is still migrating.");
                }
            }

            if (record.Sharding.Shards.ContainsKey(DestinationShard) == false)
                throw new RachisApplyException($"Destination shard {DestinationShard} doesn't exists");

            _migration = new ShardBucketMigration
            {
                Bucket = Bucket,
                DestinationShard = DestinationShard,
                SourceShard = sourceShard,
                MigrationIndex = etag,
                Status = MigrationStatus.Moving
            };

            record.Sharding.BucketMigrations.Add(Bucket, _migration);
        }

        public override void AfterDatabaseRecordUpdate(ClusterOperationContext ctx, Table items, Logger clusterAuditLog)
        {
            if (_migration == null)
                return;

            ProcessSubscriptionsForMigration(ctx, _migration);
        }

        private void ProcessSubscriptionsForMigration(ClusterOperationContext context, ShardBucketMigration migration)
        {
            var index = migration.MigrationIndex;
            var database = ShardHelper.ToShardName(DatabaseName, migration.SourceShard);
            foreach (var (key, state) in ClusterStateMachine.ReadValuesStartingWith(context, SubscriptionState.SubscriptionPrefix(DatabaseName)))
            {
                var subscriptionState = JsonDeserializationClient.SubscriptionState(state);
                if (subscriptionState.ShardingState.ChangeVectorForNextBatchStartingPointPerShard.TryGetValue(database, out var changeVector) == false)
                {
                    changeVector = string.Empty;
                }

                if (subscriptionState.ShardingState.ProcessedChangeVectorPerBucket.ContainsKey(migration.Bucket) == false)
                {
                    subscriptionState.ShardingState.ProcessedChangeVectorPerBucket[migration.Bucket] = changeVector;
                }

                using (state)
                using (Slice.From(context.Allocator, subscriptionState.SubscriptionName, out Slice valueName))
                using (var updated = context.ReadObject(subscriptionState.ToJson(), "migration"))
                {
                    ClusterStateMachine.UpdateValueForItemsTable(context, index, key, valueName, updated);
                }
            }
        }

        public override void FillJson(DynamicJsonValue json)
        {
            json[nameof(DestinationShard)] = DestinationShard;
            json[nameof(Bucket)] = Bucket;
        }
    }
}
