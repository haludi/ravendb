using Raven.Client.Documents.Indexes;

namespace Raven.Client.Documents.Operations;

public class EssentialIndexInformation
{
    public string Name { get; set; }

    public IndexLockMode LockMode { get; set; }

    public IndexPriority Priority { get; set; }

    public IndexType Type { get; set; }

    public IndexSourceType SourceType { get; set; }
    
    public ArchivedDataProcessingBehavior? ArchivedDataProcessingBehavior { get; set; }
}
