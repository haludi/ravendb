﻿using System;
using Corax.Exceptions;
using Raven.Client.Documents.Indexes;
using Raven.Server.Documents.Indexes.MapReduce.Static;
using Raven.Server.Documents.Indexes.Static;
using Raven.Server.Documents.Indexes.Static.Counters;
using Raven.Server.Documents.Indexes.Static.TimeSeries;
using Raven.Server.Documents.Queries;
using Raven.Server.Indexing;
using Raven.Server.ServerWide.Context;
using Raven.Server.Utils;
using Sparrow.Json;
using Sparrow.Logging;
using Voron;
using Voron.Data.CompactTrees;
using Voron.Impl;
using Constants = Raven.Client.Constants;

namespace Raven.Server.Documents.Indexes.Persistence.Corax;

public class CoraxIndexPersistence : IndexPersistenceBase
{
    private readonly Logger _logger;
    private readonly CoraxDocumentConverterBase _converter;

    public CoraxIndexPersistence(Index index, IIndexReadOperationFactory indexReadOperationFactory) : base(index, indexReadOperationFactory)
    {
        _logger = LoggingSource.Instance.GetLogger<CoraxIndexPersistence>(index.DocumentDatabase.Name);
        bool storeValue = false;
        switch (index.Type)
        {
            case IndexType.AutoMapReduce:
                storeValue = true;
                break;
            case IndexType.MapReduce:
                _converter = new AnonymousCoraxDocumentConverter(index, true);
                break;
            case IndexType.Map:
                switch (_index.SourceType)
                {
                    case IndexSourceType.Documents:
                        _converter = new AnonymousCoraxDocumentConverter(index);
                        break;
                    case IndexSourceType.TimeSeries:
                    case IndexSourceType.Counters:
                        _converter = new CountersAndTimeSeriesAnonymousCoraxDocumentConverter(index);
                        break;
                }
                break;
            case IndexType.JavaScriptMap:
                switch (_index.SourceType)
                {
                    case IndexSourceType.Documents:
                        _converter = new CoraxJintDocumentConverter((MapIndex)index);
                        break;
                    case IndexSourceType.TimeSeries:
                        _converter = new CountersAndTimeSeriesJintCoraxDocumentConverter((MapTimeSeriesIndex)index);
                        break;
                    case IndexSourceType.Counters:
                        _converter = new CountersAndTimeSeriesJintCoraxDocumentConverter((MapCountersIndex)index);
                        break;
                }
                break;
            case IndexType.JavaScriptMapReduce:
                _converter = new CoraxJintDocumentConverter((MapReduceIndex)index, storeValue: true);
                break;
        }
        _converter ??= new CoraxDocumentConverter(index, storeValue: storeValue);
    }

    public override IndexReadOperationBase OpenIndexReader(Transaction readTransaction, IndexQueryServerSide query = null)
    {
        return IndexReadOperationFactory.CreateCoraxIndexReadOperation(_index, _logger, readTransaction, _index._queryBuilderFactories,
            _converter.GetKnownFieldsForQuerying(), query);
    }

    public override bool ContainsField(string field)
    {
        if (field == Constants.Documents.Indexing.Fields.DocumentIdFieldName)
            return _index.Type.IsMap();

        return _index.Definition.IndexFields.ContainsKey(field);
    }

    public override IndexFacetReadOperationBase OpenFacetedIndexReader(Transaction readTransaction)
    {
        return new CoraxIndexFacetedReadOperation(_index, _logger, readTransaction, _index._queryBuilderFactories, _converter.GetKnownFieldsForQuerying());
    }

    public override SuggestionIndexReaderBase OpenSuggestionIndexReader(Transaction readTransaction, string field)
    {
        if (_converter.GetKnownFieldsForQuerying().TryGetByFieldName(readTransaction.Allocator, field, out var binding) == false)
            throw new InvalidOperationException($"No suggestions index found for field '{field}'.");

        return new CoraxSuggestionReader(_index, _logger, binding, readTransaction, _converter.GetKnownFieldsForQuerying());
    }

    public override void Dispose()
    {
        _converter?.Dispose();
    }

    #region LuceneMethods

    public override bool HasWriter { get; }

    public override void CleanWritersIfNeeded()
    {
        // lucene method
    }

    public override void Clean(IndexCleanup mode)
    {
        // lucene method
    }

    private const int IndexingCompressionMaxTestDocuments = 10000;


    public override void Initialize(StorageEnvironment environment)
    {
        var contextPool = _index._contextPool;

        using (contextPool.AllocateOperationContext(out TransactionOperationContext context))
        using (var tx = context.OpenReadTransaction())
        {
            // It is already initialized, we can return.
            if (CompactTree.HasDictionary(tx.InnerTransaction.LowLevelTransaction))
                return;
        }

        var documentStorage = _index.DocumentDatabase.DocumentsStorage;

        using var _ = CultureHelper.EnsureInvariantCulture();
        using var __ = contextPool.AllocateOperationContext(out TransactionOperationContext indexContext);
        using var queryContext = QueryOperationContext.Allocate(_index.DocumentDatabase, _index);
        
        using (CurrentIndexingScope.Current = _index.CreateIndexingScope(indexContext, queryContext))
        {
            indexContext.PersistentContext.LongLivedTransactions = true;
            queryContext.SetLongLivedTransactions(true);
            queryContext.OpenReadTransaction();

            using var tx = indexContext.OpenWriteTransaction();

            var enumerator = new CoraxDocumentTrainEnumerator(indexContext, _index, _index.Type, documentStorage, queryContext, _index.Collections, _index.Configuration.DocumentsLimitForCompressionDictionaryCreation);
            var testEnumerator = new CoraxDocumentTrainEnumerator(indexContext, _index, _index.Type, documentStorage, queryContext, _index.Collections, IndexingCompressionMaxTestDocuments);

            var llt = tx.InnerTransaction.LowLevelTransaction;
            var defaultDictionaryId = PersistentDictionary.CreateDefault(llt);
            var defaultDictionary = new PersistentDictionary(llt.GetPage(defaultDictionaryId));

            PersistentDictionary.ReplaceIfBetter(llt, enumerator, testEnumerator, defaultDictionary);

            tx.Commit();
        }
    }

    public override void PublishIndexCacheToNewTransactions(IndexTransactionCache transactionCache)
    {
        //lucene method
    }

    internal override IndexTransactionCache BuildStreamCacheAfterTx(Transaction tx)
    {
        //lucene method

        return null;
    }

    internal override void RecreateSearcher(Transaction asOfTx)
    {
        //lucene method
    }

    internal override void RecreateSuggestionsSearchers(Transaction asOfTx)
    {
        //lucene method
    }

    public override void DisposeWriters()
    {
        //lucene method
    }
    #endregion
    
    public override IndexWriteOperationBase OpenIndexWriter(Transaction writeTransaction, JsonOperationContext indexContext)
    {
        if (_index.Type == IndexType.MapReduce || _index.Type == IndexType.JavaScriptMapReduce)
        {
            var mapReduceIndex = (MapReduceIndex)_index;
            if (string.IsNullOrWhiteSpace(mapReduceIndex.Definition.OutputReduceToCollection) == false)
                return new OutputReduceCoraxIndexWriteOperation(mapReduceIndex, writeTransaction, _converter, _logger, indexContext);
        }
        
        return new CoraxIndexWriteOperation(
            _index,
            writeTransaction,
            _converter,
            _logger
        );
    }
    
}
