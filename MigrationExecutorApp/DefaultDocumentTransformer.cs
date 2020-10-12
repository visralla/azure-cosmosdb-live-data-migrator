using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Azure.Storage.Blobs;
using Microsoft.Azure.Documents;

namespace MigrationConsoleApp
{
    public class DefaultDocumentTransformer : IDocumentTransformer
    {
        public Task<IEnumerable<Document>> TransformDocument(Document sourceDoc, BlobContainerClient containerClient)
        {
            var docs = OneServiceDocumentTransformer.TransformDocument(sourceDoc, containerClient);

            return Task.FromResult(docs.AsEnumerable());
        }
    }
}
