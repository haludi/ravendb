using System;
using System.Collections.Generic;
using System.Diagnostics;
using Sparrow.Logging;
using Sparrow.Threading;
using Voron;
using Voron.Impl;

namespace Raven.Server.ServerWide.Context
{
    public class ClusterOperationContext : TransactionOperationContext<ClusterTransaction>
    {
        private readonly ClusterChanges _changes;
        public readonly StorageEnvironment Environment;

        public ClusterOperationContext(ClusterChanges changes, StorageEnvironment environment, int initialSize, int longLivedSize, int maxNumberOfAllocatedStringValues, SharedMultipleUseFlag lowMemoryFlag)
            : base(initialSize, longLivedSize, maxNumberOfAllocatedStringValues, lowMemoryFlag)
        {
            _changes = changes ?? throw new ArgumentNullException(nameof(changes));
            Environment = environment ?? throw new ArgumentNullException(nameof(environment));
        }

        protected override ClusterTransaction CloneReadTransaction(ClusterTransaction previous)
        {
            var clonedTx = new ClusterTransaction(Environment.CloneReadTransaction(previous.InnerTransaction, PersistentContext, Allocator), _changes);

            previous.Dispose();

            return clonedTx;
        }

        protected override ClusterTransaction CreateReadTransaction()
        {
            return new ClusterTransaction(Environment.ReadTransaction(PersistentContext, Allocator), _changes);
        }

        protected override ClusterTransaction CreateWriteTransaction(TimeSpan? timeout = null, string debug = null)
        {
            return new ClusterTransaction(Environment.WriteTransaction(PersistentContext, Allocator, timeout), _changes, debug);
        }
    }

    public class ClusterTransaction : RavenTransaction
    {
        private static Logger _log = LoggingSource.Instance.GetLogger("ClusterTransaction", "ClusterTransaction");
        private List<CompareExchangeChange> _compareExchangeNotifications;

        protected readonly ClusterChanges _clusterChanges;
        private readonly string _debug;
        private readonly Stopwatch _stopwatch; 

        public ClusterTransaction(Transaction transaction, ClusterChanges clusterChanges, string debug = null)
            : base(transaction)
        {
            _debug = debug;
            if (transaction is {IsWriteTransaction: true})
            {
                _stopwatch = Stopwatch.StartNew();
                _log.Operations($"Create ClusterTransaction _debug:{_debug}");
            }
            _clusterChanges = clusterChanges ?? throw new System.ArgumentNullException(nameof(clusterChanges));
        }

        public void AddAfterCommitNotification(CompareExchangeChange change)
        {
            Debug.Assert(_clusterChanges != null, "_clusterChanges != null");

            if (_compareExchangeNotifications == null)
                _compareExchangeNotifications = new List<CompareExchangeChange>();
            _compareExchangeNotifications.Add(change);
        }

        protected override bool ShouldRaiseNotifications()
        {
            return _compareExchangeNotifications != null;
        }

        protected override void RaiseNotifications()
        {
            if (_compareExchangeNotifications?.Count > 0)
            {
                foreach (var notification in _compareExchangeNotifications)
                {
                    _clusterChanges.RaiseNotifications(notification);
                }
            }
        }

        public override void Dispose()
        {
            base.Dispose();
            if(_stopwatch != default)
            {
                _stopwatch.Stop();
                if(_stopwatch.ElapsedMilliseconds > 1000)
                    _log.Operations($"Dispose ClusterTransaction _debug:{_debug} {_stopwatch.ElapsedMilliseconds}");
            }
        }
    }
}
