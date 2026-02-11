using System.Threading.Tasks;
using JetBrains.Annotations;
using NuGet.Protocol;
using Raven.Server.ServerWide.Context;
using Raven.Server.Web;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Server.Documents.Handlers.Processors.Attachments;

internal abstract class AbstractAttachmentHandlerProcessorForGetAttachmentsByEtag<TRequestHandler, TOperationContext> : AbstractDatabaseHandlerProcessor<TRequestHandler, TOperationContext>
    where TOperationContext : JsonOperationContext
    where TRequestHandler : AbstractDatabaseRequestHandler<TOperationContext>
{

    protected AbstractAttachmentHandlerProcessorForGetAttachmentsByEtag([NotNull] TRequestHandler requestHandler) : base(requestHandler)
    {
    }

        
}

internal abstract class AttachmentHandlerProcessorForGetAttachmentsByEtag : AbstractAttachmentHandlerProcessorForGetAttachmentsByEtag<DatabaseRequestHandler, DocumentsOperationContext>
{

    protected AttachmentHandlerProcessorForGetAttachmentsByEtag([NotNull] DatabaseRequestHandler requestHandler) : base(requestHandler)
    {
    }

    public override async ValueTask ExecuteAsync()
    {
        var startEtag = RequestHandler.GetLongQueryString("etag", required: false) ?? 0L;
            
        using (ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
        using (context.OpenReadTransaction())
        {
            
            await using (var writer = new AsyncBlittableJsonTextWriter(context, RequestHandler.ResponseBodyStream()))
            {
                var attachment = RequestHandler.Database.DocumentsStorage.AttachmentsStorage.GetAttachmentByEtag(context, startEtag);

                var result = new DynamicJsonValue
                {
                    [nameof(attachment.Key)] = attachment.Key,
                };
            
                context.Write(writer, result);
            }
        }
    }
        
}

