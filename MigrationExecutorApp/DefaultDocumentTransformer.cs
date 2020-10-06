using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.Azure.Documents;

namespace MigrationConsoleApp
{
    public class DefaultDocumentTransformer : IDocumentTransformer
    {
        public Task<IEnumerable<Document>> TransformDocument(Document sourceDoc)
        {
            var docs = OneServiceDocumentTransformer.TransformDocument(sourceDoc);

            return Task.FromResult(docs.AsEnumerable());
        }
    }
}
