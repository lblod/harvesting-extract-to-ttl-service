import { sparqlEscapeUri, sparqlEscapeString, uuid } from 'mu';
import { querySudo as query, updateSudo as update } from '@lblod/mu-auth-sudo';
import {
  TASK_HARVESTING_IMPORTING,
  PREFIXES,
  STATUS_BUSY,
  STATUS_SUCCESS,
  STATUS_FAILED,
} from '../constants';

import RDFAextractor from './rdfa-extractor';
import { getFilePath, getFileMetadata, writeTtlFile } from './file-helpers';
import { loadTask, updateTaskStatus, appendTaskError } from './task';

import validateTriple from './validateTriple';
import fixTriple from './fixTriple';

export async function run(deltaEntry) {
  const task = await loadTask(deltaEntry);
  if (!task) return;

  if (!(await isHarvestingTask(task))) return;

  try {
    updateTaskStatus(task, STATUS_BUSY);

    let pages = await getPages(task);
    const extractor = new RDFAextractor();
    for (const page of pages) {
      const metaData = await getFileMetadata(page);
      const path = await getFilePath(page);
      await extractor.addPage(path, metaData);
    }
    const ttl = await extractor.ttl();
    const fileUri = await writeTtlFile(task.graph, (ttl || []).join('\n'), 'original.ttl');

    const fileContainer = { id: uuid() };
    fileContainer.uri = `http://redpencil.data.gift/id/dataContainers/${fileContainer.id}`;
    await appendTaskResultFile(task, fileContainer, fileUri);

    const { validTriples, invalidTriples, correctedTriples } = await correctAndRepairTriples((ttl || []));

    const validFile = await writeTtlFile(task.graph, validTriples.join('\n'), 'valid-triples.ttl');
    await appendTaskResultFile(task, fileContainer, validFile);

    const inValidFile = await writeTtlFile(task.graph, invalidTriples.join('\n'), 'invalid-triples.ttl');
    await appendTaskResultFile(task, fileContainer, inValidFile);

    const correctedFile = await writeTtlFile(task.graph, correctedTriples.join('\n'),
      'corrected-triples-[original].ttl');
    await appendTaskResultFile(task, fileContainer, correctedFile);

    const importGraph = { id: uuid() };
    importGraph.uri = `http://mu.semte.ch/graphs/harvesting/tasks/import/${task.id}`;
    await appendTaskResultFile(task, importGraph, validFile);

    const graphContainer = { id: uuid() };
    graphContainer.uri = `http://redpencil.data.gift/id/dataContainers/${graphContainer.id}`;
    await appendTaskResultGraph(task, graphContainer, importGraph.uri);

    updateTaskStatus(task, STATUS_SUCCESS);
  } catch (e) {
    console.error(e);
    if (task) {
      await appendTaskError(task, e.message);
      await updateTaskStatus(task, STATUS_FAILED);
    }
  }
}

async function isHarvestingTask(task) {
  return task.operation == TASK_HARVESTING_IMPORTING;
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
        ?container task:hasFile ?page.
     }
  }
  `);
  if (result.results.bindings.length) {
    return result.results.bindings.map(binding => binding['page'].value);
  } else {
    return [];
  }
}

async function appendTaskResultFile(task, container, fileUri) {
  const queryStr = `
    PREFIX dct: <http://purl.org/dc/terms/>
    PREFIX task: <http://redpencil.data.gift/vocabularies/tasks/>
    PREFIX nfo: <http://www.semanticdesktop.org/ontologies/2007/03/22/nfo#>
    PREFIX mu: <http://mu.semte.ch/vocabularies/core/>
    INSERT DATA {
      GRAPH ${sparqlEscapeUri(task.graph)} {
        ${sparqlEscapeUri(container.uri)} a nfo:DataContainer.
        ${sparqlEscapeUri(container.uri)} mu:uuid ${sparqlEscapeString(container.id)}.
        ${sparqlEscapeUri(container.uri)} task:hasFile ${sparqlEscapeUri(fileUri)}.

        ${sparqlEscapeUri(task.task)} task:resultsContainer ${sparqlEscapeUri(container.uri)}.
      }
    }
  `;

  await update(queryStr);

}

async function appendTaskResultGraph(task, container, graphUri) {
  const queryStr = `
    PREFIX dct: <http://purl.org/dc/terms/>
    PREFIX task: <http://redpencil.data.gift/vocabularies/tasks/>
    PREFIX nfo: <http://www.semanticdesktop.org/ontologies/2007/03/22/nfo#>
    PREFIX mu: <http://mu.semte.ch/vocabularies/core/>
    INSERT DATA {
      GRAPH ${sparqlEscapeUri(task.graph)} {
        ${sparqlEscapeUri(container.uri)} a nfo:DataContainer.
        ${sparqlEscapeUri(container.uri)} mu:uuid ${sparqlEscapeString(container.id)}.
        ${sparqlEscapeUri(container.uri)} task:hasGraph ${sparqlEscapeUri(graphUri)}.

        ${sparqlEscapeUri(task.task)} task:resultsContainer ${sparqlEscapeUri(container.uri)}.
      }
    }
  `;

  await update(queryStr);

}

async function correctAndRepairTriples(ttlTriples) {
  const validTriples = [];
  const invalidTriples = [];
  const correctedTriples = [];

  for (const triple of ttlTriples) {
    if (await validateTriple(triple)) {
      validTriples.push(triple);
    } else {
      invalidTriples.push(triple);
    }
  }

  for (const triple of invalidTriples) {
    const fixedTriple = await fixTriple(triple);
    if (fixedTriple) {
      validTriples.push(fixedTriple);
      correctedTriples.push(triple);
    }
  }
  return { validTriples, invalidTriples, correctedTriples };
}
