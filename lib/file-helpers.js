import {sparqlEscapeUri} from 'mu';

import fs from 'fs-extra';

import {querySudo as query} from '@lblod/mu-auth-sudo';

// TODO maybe add retry if the file is not yet processed?
export async function getFileContent(remoteFileUri) {
  const result = await query(`
    PREFIX nie: <http://www.semanticdesktop.org/ontologies/2007/01/19/nie#>

    SELECT ?file
    WHERE {
      ?file nie:dataSource ${sparqlEscapeUri(remoteFileUri)} .
    } LIMIT 1`);
  if (result.results.bindings.length) {
    const file = result.results.bindings[0]['file'].value;
    console.log(`Getting contents of file ${file}`);
    const path = file.replace('share://', '/share/');
    return fs.readFile(path);
  } else {
    return null;
  }
}