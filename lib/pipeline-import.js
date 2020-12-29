import { sparqlEscapeUri, sparqlEscapeString, uuid } from 'mu';
import { querySudo as query, updateSudo as update } from '@lblod/mu-auth-sudo';
import { TASK_HARVESTING_IMPORTING,
         PREFIXES,
         STATUS_BUSY,
         STATUS_SUCCESS,
         STATUS_FAILED,
       } from "../constants";

import RDFAextractor from "./rdfa-extractor";
import { getFileContent, getFileMetadata, writeTtlFile } from "./file-helpers";
import { loadTask, isTask, updateTaskStatus, appendTaskError } from './task';

export async function run( deltaEntry ){
  if(! await isTask(deltaEntry) ) return;

  const task = await loadTask(deltaEntry);

  if(!(await isHarvestingTask(task))) return;

  try {
    updateTaskStatus(task, STATUS_BUSY);

    let pages = await getPages(task);
    let extractor = await constructExtractor(pages);
    const ttl = extractor.ttl();
    const fileUri = await writeTtlFile( task.graph , ttl.join('\n'));
    await appendTaskResultFile(task, fileUri);

    updateTaskStatus(task, STATUS_SUCCESS);
  }
  catch(e){
    console.error(e);
    await appendTaskError(task, e.message);
    await updateTaskStatus(task, STATUS_FAILED);
  }
}

async function isHarvestingTask( task ){
  return task.operation == TASK_HARVESTING_IMPORTING;
}

/**
 * Returns extractor containing all the triples that could be harvested for the given pages/publications.
 *
 * @param pages to be harvested
 */
async function constructExtractor(pages) {
  const extractor = new RDFAextractor();
  for (let page of pages) {
    const html = await getFileContent(page);
    const metaData = await getFileMetadata(page);
    extractor.addPage(html, metaData);
  }
  return extractor;
}

/**
 * Returns all the linked html-pages/publications from the given harvesting-task URI.
 *
 * @param taskURI the URI of the harvesting-task to import.
 */
async function getPages(task) {
  const result = await query(`
  ${PREFIXES}
  SELECT ?page
  WHERE {
     GRAPH ?g {
        ${sparqlEscapeUri(task.task)} task:inputContainer ?container.
        ?container task:hasHarvestingCollection ?collection .
        ?collection terms:hasPart ?page .
     }
  }
  `);
  if (result.results.bindings.length) {
    return result.results.bindings.map(binding => binding['page'].value);
  } else {
    return [];
  }
}

async function appendTaskResultFile(task, fileUri){
  const id = uuid();
  const containerUri = `http://redpencil.data.gift/id/dataContainers/${id}`;
  const queryStr = `
    PREFIX dct: <http://purl.org/dc/terms/>
    PREFIX task: <http://redpencil.data.gift/vocabularies/tasks/>
    PREFIX nfo: <http://www.semanticdesktop.org/ontologies/2007/03/22/nfo#>
    PREFIX mu: <http://mu.semte.ch/vocabularies/core/>
    INSERT DATA {
      GRAPH ${sparqlEscapeUri(task.graph)} {
        ${sparqlEscapeUri(containerUri)} a nfo:DataContainer.
        ${sparqlEscapeUri(containerUri)} mu:uuid ${sparqlEscapeString(id)}.
        ${sparqlEscapeUri(containerUri)} task:hasFile ${sparqlEscapeUri(fileUri)}.

        ${sparqlEscapeUri(task.task)} task:resultsContainer ${sparqlEscapeUri(containerUri)}.
      }
    }
  `;

  await update(queryStr);

}
