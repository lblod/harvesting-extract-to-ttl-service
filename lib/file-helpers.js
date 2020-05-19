import fs from 'fs-extra';

export async function getFileContent(remoteFileUri) {
    const q = `
    PREFIX nie: <http://www.semanticdesktop.org/ontologies/2007/01/19/nie#>

    SELECT ?file
    WHERE {
      ?file nie:dataSource ${sparqlEscapeUri(remoteFileUri)} .
    } LIMIT 1
  `;

    const result = await query(q);
    if (result.results.bindings.length) {
        const file = result.results.bindings[0]['file'].value;
        console.log(`Getting contents of file ${file}`);
        const path = file.replace('share://', '/share/');
        const content = await fs.readFile(path);
        return content;
    } else {
        return null;
    }
};