using Raven.Client.Http;

namespace Raven.Client.Documents.Session
{
    public enum TransactionMode
    {
        SingleNode,
        ClusterWide
    }

    public class SessionOptions
    {
        public string Database { get; set; }
        public RequestExecutor RequestExecutor { get; set; }

        /// <summary>
        /// Once TransactionMode set 'ClusterWide' it  will perform the SaveChanges as a transactional cluster wide operation.
        /// Any document load / store / delete will be part of this session's cluster transaction.
        /// </summary>
        public TransactionMode TransactionMode { get; set; }
    }
}
