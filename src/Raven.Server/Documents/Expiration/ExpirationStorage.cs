using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Globalization;
using System.Threading;
using Raven.Client;
using Raven.Client.Exceptions;
using Raven.Client.Exceptions.Documents;
using Raven.Server.ServerWide.Context;
using Sparrow;
using Sparrow.Binary;
using Sparrow.Json;
using Voron;
using Voron.Impl;

namespace Raven.Server.Documents.Expiration
{
    public unsafe class ExpirationStorage
    {
        private const string DocumentsByExpiration = "DocumentsByExpiration";
        private const string DocumentsByRefresh = "DocumentsByRefresh";

        private readonly DocumentDatabase _database;
        private static readonly Size MaxTransactionSize = new(16, SizeUnit.Megabytes);

        public ExpirationStorage(DocumentDatabase database, Transaction tx)
        {
            _database = database;

            tx.CreateTree(DocumentsByExpiration);
            tx.CreateTree(DocumentsByRefresh);
        }

        public void Put(DocumentsOperationContext context, Slice lowerId, BlittableJsonReaderObject document)
        {
            if (document.TryGet(Constants.Documents.Metadata.Key, out BlittableJsonReaderObject metadata) == false)
                return;

            var hasExpirationDate = metadata.TryGet(Constants.Documents.Metadata.Expires, out string expirationDate);
            var hasRefreshDate = metadata.TryGet(Constants.Documents.Metadata.Refresh, out string refreshDate);

            if (hasExpirationDate == false && hasRefreshDate == false)
                return;

            if (hasExpirationDate)
                PutInternal(context, lowerId, expirationDate, DocumentsByExpiration);

            if (hasRefreshDate)
                PutInternal(context, lowerId, refreshDate, DocumentsByRefresh);
        }

        private void PutInternal(DocumentsOperationContext context, Slice lowerId, string expirationDate, string treeName)
        {
            if (DateTime.TryParseExact(expirationDate, DefaultFormat.DateTimeFormatsToRead, CultureInfo.InvariantCulture, DateTimeStyles.RoundtripKind, out DateTime date) == false)
                ThrowWrongExpirationDateFormat(lowerId, expirationDate);

            // We explicitly enable adding documents that have already been expired, we have to, because if the time lag is short, it is possible
            // that we add a document that expire in 1 second, but by the time we process it, it already expired. The user did nothing wrong here
            // and we'll use the normal cleanup routine to clean things up later.

            var expiry = date.ToUniversalTime();
            var ticksBigEndian = Bits.SwapBytes(expiry.Ticks);

            var tree = context.Transaction.InnerTransaction.ReadTree(treeName);
            using (Slice.External(context.Allocator, (byte*)&ticksBigEndian, sizeof(long), out Slice ticksSlice))
                tree.MultiAdd(ticksSlice, lowerId);
        }

        private void ThrowWrongExpirationDateFormat(Slice lowerId, string expirationDate)
        {
            throw new InvalidOperationException(
                $"The expiration date format for document '{lowerId}' is not valid: '{expirationDate}'. Use the following format: {_database.Time.GetUtcNow():O}");
        }

        public record ExpiredDocumentsOptions
        {
            public DocumentsOperationContext Context;
            public DateTime CurrentTime;
            public bool IsFirstInTopology; 
            public long AmountToTake;
            public long MaxItemsToProcess { get; set; }

            public ExpiredDocumentsOptions(DocumentsOperationContext context, DateTime currentTime, bool isFirstInTopology, long amountToTake, long maxItemsToProcess) =>
                (Context, CurrentTime, IsFirstInTopology, AmountToTake, MaxItemsToProcess)
                = (context, currentTime, isFirstInTopology, amountToTake, maxItemsToProcess);
        }

        public Queue<ExpiredDocumentInfo> GetExpiredDocuments(ExpiredDocumentsOptions options, ref int totalCount, out Stopwatch duration,  CancellationToken cancellationToken)
        {
            return GetDocuments(options, DocumentsByExpiration, Constants.Documents.Metadata.Expires, ref totalCount, out duration, cancellationToken);
        }

        public Queue<ExpiredDocumentInfo> GetDocumentsToRefresh(ExpiredDocumentsOptions options, ref int totalCount, out Stopwatch duration, CancellationToken cancellationToken)
        {
            return GetDocuments(options, DocumentsByRefresh, Constants.Documents.Metadata.Refresh, ref totalCount, out duration, cancellationToken);
        }

        private Queue<ExpiredDocumentInfo> GetDocuments(ExpiredDocumentsOptions options, string treeName, string metadataPropertyToCheck, ref int totalCount, out Stopwatch duration, CancellationToken cancellationToken)
        {
            var currentTicks = options.CurrentTime.Ticks;

            var expirationTree = options.Context.Transaction.InnerTransaction.ReadTree(treeName);
            using (var it = expirationTree.Iterate(false))
            {
                if (it.Seek(Slices.BeforeAllKeys) == false)
                {
                    duration = null;
                    return null;
                }

                var expired = new Queue<ExpiredDocumentInfo>();
                duration = Stopwatch.StartNew();
                
                do
                {
                    var entryTicks = it.CurrentKey.CreateReader().ReadBigEndianInt64();
                    if (entryTicks > currentTicks)
                        break;

                    var ticksAsSlice = it.CurrentKey.Clone(options.Context.Transaction.InnerTransaction.Allocator);

                    using (var multiIt = expirationTree.MultiRead(it.CurrentKey))
                    {
                        if (multiIt.Seek(Slices.BeforeAllKeys))
                        {
                            do
                            {
                                if (cancellationToken.IsCancellationRequested)
                                    return expired;

                                var clonedId = multiIt.CurrentKey.Clone(options.Context.Transaction.InnerTransaction.Allocator);

                                try
                                {
                                    using (var document = _database.DocumentsStorage.Get(options.Context, clonedId, DocumentFields.Id | DocumentFields.Data | DocumentFields.ChangeVector))
                                    {
                                        if (document == null ||
                                            document.TryGetMetadata(out var metadata) == false ||
                                            HasPassed(metadata, metadataPropertyToCheck, options.CurrentTime) == false)
                                        {
                                            expired.Enqueue(new ExpiredDocumentInfo(ticksAsSlice, clonedId, id: null));
                                            totalCount++;
                                            continue;
                                        }

                                        if (options.IsFirstInTopology == false)
                                        {
                                            // this can happen when we are running the expiration on a node that isn't 
                                            // the primary node for the database. In this case, we still run the cleanup
                                            // procedure, but we only account for documents that have already been removed
                                            // or refreshed, to cleanup the expiration queue. We'll stop on the first
                                            // document that is scheduled to be expired / refreshed and wait until the 
                                            // primary node will act on it. In this way, we reduce conflicts between nodes
                                            // performing the same action concurrently. 
                                            break;
                                        }

                                        expired.Enqueue(new ExpiredDocumentInfo(ticksAsSlice, clonedId, document.Id));
                                        totalCount++;
                                        options.Context.Transaction.ForgetAbout(document);
                                    }
                                }
                                catch (DocumentConflictException)
                                {
                                    if (options.IsFirstInTopology == false)
                                        break;

                                    var (allExpired, id) = GetConflictedExpiration(options.Context, options.CurrentTime, clonedId);

                                    if (allExpired)
                                    {
                                        expired.Enqueue(new ExpiredDocumentInfo(ticksAsSlice, clonedId, id));
                                        totalCount++;
                                    }
                                }
                            } while (multiIt.MoveNext() 
                                     && expired.Count < options.AmountToTake 
                                     && totalCount < options.MaxItemsToProcess);
                        }
                    }

                } while (it.MoveNext() 
                         && expired.Count < options.AmountToTake
                         && totalCount < options.MaxItemsToProcess);

                return expired;
            }
        }

        private (bool AllExpired, string Id) GetConflictedExpiration(DocumentsOperationContext context, DateTime currentTime, Slice clonedId)
        {
            string id = null;
            var allExpired = true;
            var conflicts = _database.DocumentsStorage.ConflictsStorage.GetConflictsFor(context, clonedId);
            if (conflicts.Count > 0)
            {
                foreach (var conflict in conflicts)
                {
                    using (conflict)
                    {
                        id = conflict.Id;

                        if (HasPassed(conflict.Doc, currentTime))
                            continue;

                        allExpired = false;
                        break;
                    }
                }
            }

            return (allExpired, id);
        }

        public static bool HasPassed(BlittableJsonReaderObject data, DateTime currentTime)
        {
            // Validate that the expiration value in metadata is still the same.
            // We have to check this as the user can update this value.
            if (data.TryGet(Constants.Documents.Metadata.Key, out BlittableJsonReaderObject metadata) == false)
                return false;

            return HasPassed(metadata, Constants.Documents.Metadata.Expires, currentTime);
        }

        private static bool HasPassed(BlittableJsonReaderObject metadata, string metadataPropertyName, DateTime currentTime)
        {
            if (metadata.TryGet(metadataPropertyName, out string expirationDate))
            {
                if (DateTime.TryParseExact(expirationDate, DefaultFormat.DateTimeFormatsToRead, CultureInfo.InvariantCulture, DateTimeStyles.RoundtripKind, out var date))
                {
                    if (currentTime >= date.ToUniversalTime())
                        return true;
                }
            }
            return false;
        }

        public class ExpiredDocumentInfo
        {
            public Slice Ticks { get; }
            public Slice LowerId { get; }
            public string Id { get; }

            private ExpiredDocumentInfo()
            {
            }

            public ExpiredDocumentInfo(Slice ticksAsSlice, Slice clonedId, string id)
            {
                Ticks = ticksAsSlice;
                LowerId = clonedId;
                Id = id;
            }
        }

        public int DeleteDocumentsExpiration(DocumentsOperationContext context, Queue<ExpiredDocumentInfo> expired, DateTime currentTime)
        {
            var deletionCount = 0;
            var count = 0;
            var expirationTree = context.Transaction.InnerTransaction.ReadTree(DocumentsByExpiration);

            foreach (var documentInfo in expired)
            {
                if (documentInfo.Id != null)
                {
                    try
                    {
                        using (var doc = _database.DocumentsStorage.Get(context, documentInfo.LowerId, DocumentFields.Data, throwOnConflict: true))
                        {
                            if (doc != null && doc.TryGetMetadata(out var metadata))
                            {
                                if (HasPassed(metadata, Constants.Documents.Metadata.Expires, currentTime))
                                {
                                    _database.DocumentsStorage.Delete(context, documentInfo.LowerId, documentInfo.Id, expectedChangeVector: null);
                                }
                            }

                            context.Transaction.ForgetAbout(doc);
                        }
                    }
                    catch (DocumentConflictException)
                    {
                        if (GetConflictedExpiration(context, currentTime, documentInfo.LowerId).AllExpired)
                            _database.DocumentsStorage.Delete(context, documentInfo.LowerId, documentInfo.Id, expectedChangeVector: null);
                    }
                    deletionCount++;
                }

                expirationTree.MultiDelete(documentInfo.Ticks, documentInfo.LowerId);
                count++;

                if (context.Transaction.InnerTransaction.LowLevelTransaction.TransactionSize > MaxTransactionSize)
                    break;
            }

            var tx = context.Transaction.InnerTransaction.LowLevelTransaction;
            tx.OnDispose += _ =>
            {
                if (tx.Committed == false)
                    return;

                for (int i = 0; i < count; i++)
                {
                    expired.Dequeue();
                }
            };

            return deletionCount;
        }

        public int RefreshDocuments(DocumentsOperationContext context, Queue<ExpirationStorage.ExpiredDocumentInfo> expired, DateTime currentTime)
        {
            var refreshCount = 0;
            var count = 0;
            var refreshTree = context.Transaction.InnerTransaction.ReadTree(DocumentsByRefresh);

            foreach (var documentInfo in expired)
            {
                if (documentInfo.Id != null)
                {
                    using (var doc = _database.DocumentsStorage.Get(context, documentInfo.LowerId, throwOnConflict: false))
                    {
                        if (doc != null && doc.TryGetMetadata(out var metadata))
                        {
                            if (HasPassed(metadata, Constants.Documents.Metadata.Refresh, currentTime))
                            {
                                // remove the @refresh tag
                                metadata.Modifications = new Sparrow.Json.Parsing.DynamicJsonValue(metadata);
                                metadata.Modifications.Remove(Constants.Documents.Metadata.Refresh);

                                using (var updated = context.ReadObject(doc.Data, doc.Id, BlittableJsonDocumentBuilder.UsageMode.ToDisk))
                                {
                                    try
                                    {
                                        _database.DocumentsStorage.Put(context, doc.Id, doc.ChangeVector, updated,
                                            flags: doc.Flags.Strip(DocumentFlags.FromClusterTransaction));
                                    }
                                    catch (ConcurrencyException)
                                    {
                                        // This is expected and safe to ignore
                                        // It can happen if there is a mismatch with the Cluster-Transaction-Index, which will
                                        // sort itself out when the cluster & database will be in sync again
                                    }
                                    catch (DocumentConflictException)
                                    {
                                        // no good way to handle this, we'll wait to resolve
                                        // the issue when the conflict is resolved
                                    }
                                }
                            }
                        }

                        context.Transaction.ForgetAbout(doc);
                    }

                    refreshCount++;
                }

                refreshTree.MultiDelete(documentInfo.Ticks, documentInfo.LowerId);
                count++;

                if (context.Transaction.InnerTransaction.LowLevelTransaction.TransactionSize > MaxTransactionSize)
                    break;
            }

            var tx = context.Transaction.InnerTransaction.LowLevelTransaction;
            tx.OnDispose += _ =>
            {
                if (tx.Committed == false)
                    return;

                for (int i = 0; i < count; i++)
                {
                    expired.Dequeue();
                }
            };

            return refreshCount;
        }
    }
}
