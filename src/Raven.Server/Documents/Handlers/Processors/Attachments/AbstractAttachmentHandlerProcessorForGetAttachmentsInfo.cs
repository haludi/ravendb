using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Client.Documents.Attachments;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Server.Documents.Handlers.Processors.Attachments
{
    internal abstract class AbstractAttachmentHandlerProcessorForGetAttachmentsInfo<TRequestHandler, TOperationContext> : AbstractDatabaseHandlerProcessor<TRequestHandler, TOperationContext>
        where TOperationContext : JsonOperationContext
        where TRequestHandler : AbstractDatabaseRequestHandler<TOperationContext>
    {

        protected AbstractAttachmentHandlerProcessorForGetAttachmentsInfo([NotNull] TRequestHandler requestHandler) : base(requestHandler)
        {
        }
    }

    public class AttachmentInfo
    {
        public string DocumentId { get; set; }
        public string Name { get; set; }
        public long Size { get; set; }
        public long Etag { get; set; }
        public AttachmentType Type { get; set; }
        public string Hash { get; internal set; }
        public string ChangeVector { get; internal set; }

        internal DynamicJsonValue ToJson()
        {
            return new DynamicJsonValue
            {
                [nameof(DocumentId)] = DocumentId,
                [nameof(Name)] = Name,
                [nameof(Size)] = Size,
                [nameof(Etag)] = Etag,
                [nameof(Type)] = Type,
                [nameof(Hash)] = Hash,
                [nameof(ChangeVector)] = ChangeVector,
            };
        }
    }}
