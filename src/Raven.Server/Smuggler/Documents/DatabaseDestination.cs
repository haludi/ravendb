﻿using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Threading.Tasks;
using Raven.Client.Documents.Attachments;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Operations.Attachments;
using Raven.Client.Documents.Smuggler;
using Raven.Client.ServerWide;
using Raven.Client.Util;
using Raven.Server.Documents;
using Raven.Server.Documents.Indexes;
using Raven.Server.Documents.TransactionCommands;
using Raven.Server.Routing;
using Raven.Server.ServerWide.Commands;
using Raven.Server.ServerWide.Commands.ConnectionStrings;
using Raven.Server.ServerWide.Context;
using Raven.Server.Smuggler.Documents.Data;
using Raven.Server.Smuggler.Documents.Processors;
using Sparrow.Json;
using Sparrow.Logging;
using Voron;
using Voron.Global;
using Sparrow;
using Sparrow.Json.Parsing;
using Sparrow.Utils;

namespace Raven.Server.Smuggler.Documents
{
    public class DatabaseDestination : ISmugglerDestination
    {
        private readonly DocumentDatabase _database;

        private readonly Logger _log;
        private BuildVersionType _buildType;

        public DatabaseDestination(DocumentDatabase database)
        {
            _database = database;
            _log = LoggingSource.Instance.GetLogger<DatabaseDestination>(database.Name);
        }

        public IDisposable Initialize(DatabaseSmugglerOptions options, SmugglerResult result, long buildVersion)
        {
            _buildType = BuildVersion.Type(buildVersion);
            return null;
        }

        public IDatabaseRecordActions DatabaseRecord()
        {
            return new DatabaseRecordActions(_database, log: _log);
        }

        public IDocumentActions Documents()
        {
            return new DatabaseDocumentActions(_database, _buildType, isRevision: false, log: _log);
        }

        public IDocumentActions RevisionDocuments()
        {
            return new DatabaseDocumentActions(_database, _buildType, isRevision: true, log: _log);
        }

        public IDocumentActions Tombstones()
        {
            return new DatabaseDocumentActions(_database, _buildType, isRevision: false, log: _log);
        }

        public IDocumentActions Conflicts()
        {
            return new DatabaseDocumentActions(_database, _buildType, isRevision: false, log: _log);
        }

        public IKeyValueActions<long> Identities()
        {
            return new DatabaseKeyValueActions(_database);
        }

        public IKeyValueActions<BlittableJsonReaderObject> CompareExchange()
        {
            return new DatabaseCompareExchangeActions(_database);
        }

        public IIndexActions Indexes()
        {
            return new DatabaseIndexActions(_database);
        }

        private class DatabaseIndexActions : IIndexActions
        {
            private readonly DocumentDatabase _database;

            public DatabaseIndexActions(DocumentDatabase database)
            {
                _database = database;
            }

            public void WriteIndex(IndexDefinitionBase indexDefinition, IndexType indexType)
            {
                AsyncHelpers.RunSync(() => _database.IndexStore.CreateIndex(indexDefinition));
            }

            public void WriteIndex(IndexDefinition indexDefinition)
            {
                AsyncHelpers.RunSync(() => _database.IndexStore.CreateIndex(indexDefinition));
            }

            public void Dispose()
            {
            }
        }

        public class DatabaseDocumentActions : IDocumentActions
        {
            private readonly DocumentDatabase _database;
            private readonly BuildVersionType _buildType;
            private readonly bool _isRevision;
            private readonly Logger _log;
            private MergedBatchPutCommand _command;
            private MergedBatchPutCommand _prevCommand;
            private Task _prevCommandTask = Task.CompletedTask;

            private readonly Sparrow.Size _enqueueThreshold;

            public DatabaseDocumentActions(DocumentDatabase database, BuildVersionType buildType, bool isRevision, Logger log)
            {
                _database = database;
                _buildType = buildType;
                _isRevision = isRevision;
                _log = log;
                _enqueueThreshold = new Sparrow.Size(
                    (sizeof(int) == IntPtr.Size || database.Configuration.Storage.ForceUsing32BitsPager) ? 2 : 32,
                    SizeUnit.Megabytes);

                _command = new MergedBatchPutCommand(database, buildType, log)
                {
                    IsRevision = isRevision
                };
            }

            public void WriteDocument(DocumentItem item, SmugglerProgressBase.CountsWithLastEtag progress)
            {
                if (item.Attachments != null)
                    progress.Attachments.ReadCount += item.Attachments.Count;
                _command.Add(item);
                HandleBatchOfDocumentsIfNecessary();
            }

            public void WriteTombstone(Tombstone tombstone, SmugglerProgressBase.CountsWithLastEtag progress)
            {
                _command.Add(new DocumentItem
                {
                    Tombstone = tombstone
                });
                HandleBatchOfDocumentsIfNecessary();
            }

            public void WriteConflict(DocumentConflict conflict, SmugglerProgressBase.CountsWithLastEtag progress)
            {
                _command.Add(new DocumentItem
                {
                    Conflict = conflict
                });
                HandleBatchOfDocumentsIfNecessary();
            }

            public void DeleteDocument(string id)
            {
                AsyncHelpers.RunSync(() => _database.TxMerger.Enqueue(new DeleteDocumentCommand(id, null, _database)));
            }

            public Stream GetTempStream()
            {
                if (_command.AttachmentStreamsTempFile == null)
                    _command.AttachmentStreamsTempFile = _database.DocumentsStorage.AttachmentsStorage.GetTempFile("smuggler");

                return _command.AttachmentStreamsTempFile.StartNewStream();
            }

            public DocumentsOperationContext GetContextForNewDocument()
            {
                _command.Context.CachedProperties.NewDocument();
                return _command.Context;
            }

            public void Dispose()
            {
                FinishBatchOfDocuments();
            }

            private void HandleBatchOfDocumentsIfNecessary()
            {
                var prevDoneAndHasEnough = _command.Context.AllocatedMemory > Constants.Size.Megabyte && _prevCommandTask.IsCompleted;
                var currentReachedLimit = _command.Context.AllocatedMemory > _enqueueThreshold.GetValue(SizeUnit.Bytes);

                if (currentReachedLimit == false && prevDoneAndHasEnough == false)
                    return;

                var prevCommand = _prevCommand;
                var prevCommandTask = _prevCommandTask;

                var commandTask = _database.TxMerger.Enqueue(_command);
                // we ensure that we first enqueue the command to if we 
                // fail to do that, we won't be waiting on the previous
                // one
                _prevCommand = _command;
                _prevCommandTask = commandTask;

                if (prevCommand != null)
                {
                    using (prevCommand)
                    {
                        prevCommandTask.GetAwaiter().GetResult();
                        Debug.Assert(prevCommand.IsDisposed == false,
                            "we rely on reusing this context on the next batch, so it has to be disposed here");
                    }
                }

                _command = new MergedBatchPutCommand(_database, _buildType, _log)
                {
                    IsRevision = _isRevision
                };
            }

            private void FinishBatchOfDocuments()
            {
                if (_prevCommand != null)
                {
                    using (_prevCommand)
                        AsyncHelpers.RunSync(() => _prevCommandTask);

                    _prevCommand = null;
                }

                if (_command.Documents.Count > 0)
                {
                    using (_command)
                        AsyncHelpers.RunSync(() => _database.TxMerger.Enqueue(_command));
                }

                _command = null;
            }
        }

        private class DatabaseCompareExchangeActions : IKeyValueActions<BlittableJsonReaderObject>
        {
            private readonly DocumentDatabase _database;
            private readonly List<AddOrUpdateCompareExchangeCommand> _compareExchangeCommands = new List<AddOrUpdateCompareExchangeCommand>();
            public DatabaseCompareExchangeActions(DocumentDatabase database)
            {
                _database = database;
            }

            public void WriteKeyValue(string key, BlittableJsonReaderObject value)
            {
                const int batchSize = 1024;
                string prefix = _database.Name + "/";
                _compareExchangeCommands.Add(new AddOrUpdateCompareExchangeCommand
                {
                    Key = prefix + key,
                    Index = 0,
                    Value = value
                });

                if (_compareExchangeCommands.Count < batchSize)
                    return;

                SendCommands();
            }

            public void Dispose()
            {
                if (_compareExchangeCommands.Count == 0)
                    return;

                SendCommands();
            }

            private void SendCommands()
            {
                AsyncHelpers.RunSync(async () =>
                {
                    using (_database.ServerStore.ContextPool.AllocateOperationContext(out JsonOperationContext context))
                    {
                        return await _database.ServerStore.SendToLeaderAsync(new AddOrUpdateCompareExchangeBatchCommand(_compareExchangeCommands, context));
                    }
                });

                _compareExchangeCommands.Clear();
            }
        }

        private class DatabaseKeyValueActions : IKeyValueActions<long>
        {
            private readonly DocumentDatabase _database;
            private readonly Dictionary<string, long> _identities;

            public DatabaseKeyValueActions(DocumentDatabase database)
            {
                _database = database;
                _identities = new Dictionary<string, long>();
            }

            public void WriteKeyValue(string key, long value)
            {
                const int batchSize = 1024;

                _identities[key] = value;

                if (_identities.Count < batchSize)
                    return;

                SendIdentities();
            }

            public void Dispose()
            {
                if (_identities.Count == 0)
                    return;

                SendIdentities();
            }

            private void SendIdentities()
            {
                //fire and forget, do not hold-up smuggler operations waiting for Raft command
                AsyncHelpers.RunSync(() => _database.ServerStore.SendToLeaderAsync(new UpdateClusterIdentityCommand(_database.Name, _identities, false)));

                _identities.Clear();
            }
        }

        private class DatabaseRecordActions : IDatabaseRecordActions
        {
            private readonly DocumentDatabase _database;
            private readonly Logger _log;

            public DatabaseRecordActions(DocumentDatabase database, Logger log)
            {
                _database = database;
                _log = log;
            }

            public void WriteDatabaseRecord(DatabaseRecord databaseRecord, SmugglerProgressBase.DatabaseRecordProgress progress, AuthorizationStatus authorizationStatus)
            {
                var currentDatabaseRecord = _database.ReadDatabaseRecord();
                var tasks = new List<Task>();

                if (currentDatabaseRecord?.Revisions == null &&
                    databaseRecord?.Revisions != null)
                {
                    if (_log.IsInfoEnabled)
                        _log.Info("Configuring revisions from smuggler");
                    tasks.Add(_database.ServerStore.SendToLeaderAsync(new EditRevisionsConfigurationCommand(databaseRecord.Revisions, _database.Name)));
                    progress.RevisionsConfigurationUpdated = true;
                }

                if (currentDatabaseRecord?.Expiration == null &&
                    databaseRecord?.Expiration != null)
                {
                    if (_log.IsInfoEnabled)
                        _log.Info("Configuring expiration from smuggler");
                    tasks.Add(_database.ServerStore.SendToLeaderAsync(new EditExpirationCommand(databaseRecord.Expiration, _database.Name)));
                    progress.ExpirationConfigurationUpdated = true;
                }

                if (currentDatabaseRecord?.RavenConnectionStrings.Count == 0 &&
                    databaseRecord?.RavenConnectionStrings.Count > 0)
                {
                    if (_log.IsInfoEnabled)
                        _log.Info("Configuring Raven connection strings configuration from smuggler");
                    foreach (var connectionString in databaseRecord.RavenConnectionStrings)
                    {
                        tasks.Add(_database.ServerStore.SendToLeaderAsync(new PutRavenConnectionStringCommand(connectionString.Value, _database.Name)));
                    }
                    progress.RavenConnectionStringsUpdated = true;
                }

                if (currentDatabaseRecord?.SqlConnectionStrings.Count == 0 &&
                    databaseRecord?.SqlConnectionStrings.Count > 0)
                {
                    if (_log.IsInfoEnabled)
                        _log.Info("Configuring SQL connection strings from smuggler");
                    foreach (var connectionString in databaseRecord.SqlConnectionStrings)
                    {
                        tasks.Add(_database.ServerStore.SendToLeaderAsync(new PutSqlConnectionStringCommand(connectionString.Value, _database.Name)));
                    }
                    progress.SqlConnectionStringsUpdated = true;
                }

                if (currentDatabaseRecord?.Client == null &&
                    databaseRecord?.Client != null)
                {
                    if (_log.IsInfoEnabled)
                        _log.Info("Configuring client configuration from smuggler");
                    tasks.Add(_database.ServerStore.SendToLeaderAsync(new PutClientConfigurationCommand(databaseRecord.Client)));
                    progress.ClientConfigurationUpdated = true;
                }

                foreach (var task in tasks)
                {
                    AsyncHelpers.RunSync(() => task);
                }

                tasks.Clear();
            }

            public void Dispose()
            {
            }
        }

        //Todo To check if it is K.O. ot public this class
        public class MergedBatchPutCommand : TransactionOperationsMerger.MergedTransactionCommand, IDisposable, TransactionOperationsMerger.IRecordableCommand
        {
            public bool IsRevision;

            private readonly DocumentDatabase _database;
            private readonly BuildVersionType _buildType;
            private readonly Logger _log;

            public readonly List<DocumentItem> Documents = new List<DocumentItem>();
            public StreamsTempFile AttachmentStreamsTempFile;

            private IDisposable _resetContext;
            private bool _isDisposed;

            public bool IsDisposed => _isDisposed;

            private readonly DocumentsOperationContext _context;
            private const string PreV4RevisionsDocumentId = "/revisions/";

            public MergedBatchPutCommand(DocumentDatabase database, BuildVersionType buildType, Logger log)
            {
                _database = database;
                _buildType = buildType;
                _log = log;
                _resetContext = _database.DocumentsStorage.ContextPool.AllocateOperationContext(out _context);
            }

            public DocumentsOperationContext Context => _context;

            protected override int ExecuteCmd(DocumentsOperationContext context)
            {
                if (_log.IsInfoEnabled)
                    _log.Info($"Importing {Documents.Count:#,#0} documents");

                var idsOfDocumentsToUpdateAfterAttachmentDeletion = new HashSet<string>(StringComparer.OrdinalIgnoreCase);

                foreach (var documentType in Documents)
                {
                    var tombstone = documentType.Tombstone;
                    if (tombstone != null)
                    {
                        using (Slice.External(context.Allocator, tombstone.LowerId, out Slice key))
                        {
                            var newEtag = _database.DocumentsStorage.GenerateNextEtag();
                            var changeVector = _database.DocumentsStorage.GetNewChangeVector(context, newEtag);
                            switch (tombstone.Type)
                            {
                                case Tombstone.TombstoneType.Document:
                                    _database.DocumentsStorage.Delete(context, key, tombstone.LowerId, null, tombstone.LastModified.Ticks, changeVector, new CollectionName(tombstone.Collection));
                                    break;
                                case Tombstone.TombstoneType.Attachment:
                                    var idEnd = key.Content.IndexOf(SpecialChars.RecordSeparator);
                                    if (idEnd < 1)
                                        throw new InvalidOperationException("Cannot find a document ID inside the attachment key");
                                    var attachmentId = key.Content.Substring(idEnd);
                                    idsOfDocumentsToUpdateAfterAttachmentDeletion.Add(attachmentId);

                                    _database.DocumentsStorage.AttachmentsStorage.DeleteAttachmentDirect(context, key, false, "$fromReplication", null, changeVector, tombstone.LastModified.Ticks);
                                    break;
                                case Tombstone.TombstoneType.Revision:
                                    _database.DocumentsStorage.RevisionsStorage.DeleteRevision(context, key, tombstone.Collection, changeVector, tombstone.LastModified.Ticks);
                                    break;
                            }
                        }

                        continue;
                    }

                    var conflict = documentType.Conflict;
                    if (conflict != null)
                    {
                        _database.DocumentsStorage.ConflictsStorage.AddConflict(context, conflict.Id, conflict.LastModified.Ticks, conflict.Doc, conflict.ChangeVector,
                            conflict.Collection, conflict.Flags, NonPersistentDocumentFlags.FromSmuggler);

                        continue;
                    }

                    if (documentType.Attachments != null)
                    {
                        foreach (var attachment in documentType.Attachments)
                        {
                            _database.DocumentsStorage.AttachmentsStorage.PutAttachmentStream(context, attachment.Tag, attachment.Base64Hash, attachment.Stream);
                        }
                    }

                    var document = documentType.Document;

                    var id = document.Id;

                    if (IsRevision)
                    {
                        if (_database.DocumentsStorage.RevisionsStorage.Configuration == null)
                            ThrowRevisionsDisabled();

                        PutAttachments(context, document);

                        if (document.Flags.Contain(DocumentFlags.DeleteRevision))
                        {
                            _database.DocumentsStorage.RevisionsStorage.Delete(context, id, document.Data, document.Flags,
                                document.NonPersistentFlags, document.ChangeVector, document.LastModified.Ticks);
                        }
                        else
                        {
                            _database.DocumentsStorage.RevisionsStorage.Put(context, id, document.Data, document.Flags,
                                document.NonPersistentFlags, document.ChangeVector, document.LastModified.Ticks);
                        }

                        continue;
                    }

                    if (IsPreV4Revision(id, document))
                    {
                        // handle old revisions
                        if (_database.DocumentsStorage.RevisionsStorage.Configuration == null)
                            ThrowRevisionsDisabled();

                        var endIndex = id.IndexOf(PreV4RevisionsDocumentId, StringComparison.OrdinalIgnoreCase);
                        var newId = id.Substring(0, endIndex);

                        _database.DocumentsStorage.RevisionsStorage.Put(context, newId, document.Data, document.Flags,
                            document.NonPersistentFlags, document.ChangeVector, document.LastModified.Ticks);
                        continue;
                    }

                    PutAttachments(context, document);
                    _database.DocumentsStorage.Put(context, id, null, document.Data, document.LastModified.Ticks, null, document.Flags, document.NonPersistentFlags);

                }

                foreach (var idToUpdate in idsOfDocumentsToUpdateAfterAttachmentDeletion)
                {
                    _database.DocumentsStorage.AttachmentsStorage.UpdateDocumentAfterAttachmentChange(context, idToUpdate);
                }

                return Documents.Count;
            }

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            private unsafe void PutAttachments(DocumentsOperationContext context, Document document)
            {
                if ((document.Flags & DocumentFlags.HasAttachments) != DocumentFlags.HasAttachments)
                    return;

                if (document.Data.TryGet(Client.Constants.Documents.Metadata.Key, out BlittableJsonReaderObject metadata) == false ||
                    metadata.TryGet(Client.Constants.Documents.Metadata.Attachments, out BlittableJsonReaderArray attachments) == false)
                    return;

                foreach (BlittableJsonReaderObject attachment in attachments)
                {
                    if (attachment.TryGet(nameof(AttachmentName.Name), out LazyStringValue name) == false ||
                        attachment.TryGet(nameof(AttachmentName.ContentType), out LazyStringValue contentType) == false ||
                        attachment.TryGet(nameof(AttachmentName.Hash), out LazyStringValue hash) == false)
                        throw new ArgumentException($"The attachment info in missing a mandatory value: {attachment}");

                    var cv = Slices.Empty;
                    var type = (document.Flags & DocumentFlags.Revision) == DocumentFlags.Revision ? AttachmentType.Revision : AttachmentType.Document;

                    var attachmentsStorage = _database.DocumentsStorage.AttachmentsStorage;
                    using (DocumentIdWorker.GetSliceFromId(_context, document.Id, out Slice lowerDocumentId))
                    using (DocumentIdWorker.GetLowerIdSliceAndStorageKey(_context, name, out Slice lowerName, out Slice nameSlice))
                    using (DocumentIdWorker.GetLowerIdSliceAndStorageKey(_context, contentType, out Slice lowerContentType, out Slice contentTypeSlice))
                    using (Slice.External(_context.Allocator, hash, out Slice base64Hash))
                    using (type == AttachmentType.Revision ? Slice.From(_context.Allocator, document.ChangeVector, out cv) : (IDisposable)null)
                    using (attachmentsStorage.GetAttachmentKey(_context, lowerDocumentId.Content.Ptr, lowerDocumentId.Size, lowerName.Content.Ptr, lowerName.Size,
                        base64Hash, lowerContentType.Content.Ptr, lowerContentType.Size, type, cv, out Slice keySlice))
                    {
                        attachmentsStorage.PutDirect(context, keySlice, nameSlice, contentTypeSlice, base64Hash, null);
                    }
                }
            }

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            private bool IsPreV4Revision(string id, Document document)
            {
                if (_buildType == BuildVersionType.V3 == false)
                    return false;

                if ((document.NonPersistentFlags & NonPersistentDocumentFlags.LegacyRevision) != NonPersistentDocumentFlags.LegacyRevision)
                    return false;

                return id.Contains(PreV4RevisionsDocumentId);
            }

            private static void ThrowRevisionsDisabled()
            {
                throw new InvalidOperationException("Revisions needs to be enabled before import!");
            }

            public void Dispose()
            {
                if (_isDisposed)
                    return;

                _isDisposed = true;

                foreach (var doc in Documents)
                {
                    if (doc.Document != null)
                    {
                        doc.Document.Data.Dispose();

                        if (doc.Attachments != null)
                        {
                            foreach (var attachment in doc.Attachments)
                            {
                                attachment.Dispose();
                            }
                        }
                    }
                }
                Documents.Clear();
                _resetContext?.Dispose();
                _resetContext = null;

                AttachmentStreamsTempFile?.Dispose();
                AttachmentStreamsTempFile = null;
            }

            public void Add(DocumentItem document)
            {
                Documents.Add(document);
            }

            public DynamicJsonValue Serialize()
            {
                var ret = new DynamicJsonValue
                {
                    [nameof(BuildVersionType)] = _buildType,
                    [nameof(Documents)] = Documents.Select(DocumentItemToDJson)
                };

                if (IsRevision)
                {
                    ret[nameof(IsRevision)] = true;
                }

                return ret;
            }

            private DynamicJsonValue DocumentItemToDJson(DocumentItem documentItem)
            {
                var ret = new DynamicJsonValue
                {
                    [nameof(DocumentItem.Document)] = DocumentToDJson(documentItem.Document)
                };

                return ret;
            }

            private static DynamicJsonValue DocumentToDJson(Document documentItem)
            {
                var document = new DynamicJsonValue
                {
                    [nameof(Document.Id)] = documentItem.Id,
                    [nameof(Document.Data)] = documentItem.Data,
                    [nameof(Document.Flags)] = documentItem.Flags,
                    [nameof(Document.Etag)] = documentItem.Etag,
                    [nameof(Document.LastModified)] = documentItem.LastModified,
                };
                if (string.IsNullOrEmpty(documentItem.ChangeVector) == false)
                {
                    document[nameof(Document.ChangeVector)] = documentItem.ChangeVector;
                }

                return document;
            }

            public static MergedBatchPutCommand Deserialize(BlittableJsonReaderObject mergedCmdReader, DocumentDatabase database, JsonOperationContext context)
            {
                if (mergedCmdReader.TryGet(nameof(BuildVersionType), out BuildVersionType type) == false)
                {
                    throw new Exception($"Can't read {nameof(BuildVersionType)} from {nameof(MergedBatchPutCommand)}");
                }

                //Todo To check if log is necessary 
                var log = LoggingSource.Instance.GetLogger<DatabaseDestination>(database.Name);
                var ret = new MergedBatchPutCommand(database, type, log);

                if (mergedCmdReader.TryGet(nameof(Documents), out BlittableJsonReaderArray documentsReader) == false)
                {
                    throw new Exception($"Can't read {nameof(Documents)} from {nameof(MergedBatchPutCommand)}");
                }

                for (var i = 0; i < documentsReader.Length; i++)
                {
                    ret.ReadDocumentItem(documentsReader.GetByIndex<BlittableJsonReaderObject>(i));
                }


                return ret;
            }

            private void ReadDocumentItem(BlittableJsonReaderObject documentItemReader)
            {
                var toAdd = new DocumentItem();

                if (documentItemReader.TryGet(nameof(DocumentItem.Document), out BlittableJsonReaderObject documentReader) == false)
                {
                    ThrowCantReadProperty(nameof(DocumentItem), nameof(DocumentItem.Document));
                }
                toAdd.Document = ReadDocument(documentReader, _context);

                Add(toAdd);
            }

            private static Document ReadDocument(BlittableJsonReaderObject documentReader, JsonOperationContext context)
            {
                var ret = new Document();
                if (documentReader.TryGet(nameof(Document.Id), out ret.Id) == false)
                {
                    ThrowCantReadProperty(nameof(Document), nameof(Document.Id));
                }

                if (documentReader.TryGet(nameof(Document.Flags), out ret.Flags) == false)
                {
                    ThrowCantReadProperty(nameof(Document), nameof(Document.Flags));
                }

                if (documentReader.TryGet(nameof(Document.Etag), out ret.Etag) == false)
                {
                    ThrowCantReadProperty(nameof(Document), nameof(Document.Etag));
                }

                if (documentReader.TryGet(nameof(Document.LastModified), out ret.LastModified) == false)
                {
                    ThrowCantReadProperty(nameof(Document), nameof(Document.LastModified));
                }

                documentReader.TryGet(nameof(Document.ChangeVector), out ret.ChangeVector);

                if (documentReader.TryGet(nameof(Document.Data), out BlittableJsonReaderObject dataReader) == false)
                {
                    ThrowCantReadProperty(nameof(Document), nameof(Document.Data));
                }
                ret.Data = dataReader.Clone(context);

                return ret;
            }

            private static void ThrowCantReadProperty(string objName, string propName)
            {
                throw new Exception($"Can't read {propName} from {objName}");
            }
        }
    }
}
