using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using CsvHelper;
using CsvHelper.Configuration;
using JetBrains.Annotations;
using Raven.Client;
using Raven.Client.Documents.Operations.Attachments;
using Raven.Client.Exceptions;
using Raven.Client.Util;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Server.Documents.Handlers.Processors.Attachments
{
    internal sealed class AttachmentHandlerProcessorForGetAttachmentsInfo : AbstractAttachmentHandlerProcessorForGetAttachmentsInfo<DatabaseRequestHandler, DocumentsOperationContext>
    {
        public AttachmentHandlerProcessorForGetAttachmentsInfo([NotNull] DatabaseRequestHandler requestHandler) : base(requestHandler)
        {
        }

        public override async ValueTask ExecuteAsync()
        {
            var startEtag = RequestHandler.GetLongQueryString("startEtag", required: false) ?? 0L;
            var format = RequestHandler.GetStringQueryString("format", required: false) ?? "csv";

            using (ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
            using (context.OpenReadTransaction())
            {
                var attachments = RequestHandler.Database.DocumentsStorage.AttachmentsStorage.GetAttachments(context, startEtag)
                    .Select(x => x.ToJson());

                if (string.Equals(format, "csv", StringComparison.OrdinalIgnoreCase))
                {
                    var columns = new[]
                    {
                        nameof(AttachmentInfo.DocumentId),
                        nameof(AttachmentInfo.Name),
                        nameof(AttachmentInfo.Size),
                        nameof(AttachmentInfo.Hash),
                        nameof(AttachmentInfo.Type),
                        nameof(AttachmentInfo.ChangeVector),
                        nameof(AttachmentInfo.Etag)
                    };
                    await WriteCsvAttachments(columns, attachments);
                }
                else if (string.Equals(format, "json", StringComparison.OrdinalIgnoreCase))
                {
                    await WriteJsonAttachments(context, attachments);
                }
                else
                {
                    throw new BadRequestException($"Unknown format: '{format}'. Supported formats are 'json' and 'csv'.");
                }
            }
        }
        
        private async Task WriteCsvAttachments(string[] columns, IEnumerable<DynamicJsonValue> attachments)
        {
             var encodedCsvFileName = Uri.EscapeDataString($"attachments-info_{SystemTime.UtcNow.ToString("yyyyMMdd_HHmm", CultureInfo.InvariantCulture)}.csv");

            HttpContext.Response.Headers.ContentDisposition = $"attachment; filename=\"{encodedCsvFileName}\"; filename*=UTF-8''{encodedCsvFileName}";
            HttpContext.Response.Headers[Constants.Headers.ContentType] = "text/csv";

            await using (var writer = new StreamWriter(HttpContext.Response.Body, Encoding.UTF8))
            await using (var csvWriter = new CsvWriter(writer, new CsvConfiguration(CultureInfo.InvariantCulture) { Delimiter = "," }))
            {
                foreach (var column in columns)
                {
                    csvWriter.WriteField(column);
                }
                
                await csvWriter.NextRecordAsync();

                foreach (var attachment in attachments)
                {
                    foreach (var column in columns)
                    {
                        csvWriter.WriteField(attachment[column]);
                    }
                    await csvWriter.NextRecordAsync();
                }
            }
        }

        private async Task WriteJsonAttachments(DocumentsOperationContext context, IEnumerable<DynamicJsonValue> attachments)
        {
            await using (var writer = new AsyncBlittableJsonTextWriter(context, RequestHandler.ResponseBodyStream()))
            {
                context.Write(writer, new DynamicJsonValue
                {
                    ["Results"] = new DynamicJsonArray(attachments),
                });
            }
        }
    }
}
