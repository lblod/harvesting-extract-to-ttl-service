import { sparqlEscapeUri, sparqlEscapeString, uuid } from 'mu';
import { querySudo as query, updateSudo as update } from '@lblod/mu-auth-sudo';
import {
  PREFIXES,
  STATUS_BUSY,
  STATUS_SUCCESS,
  STATUS_FAILED,
} from '../constants';

import RDFAextractor from './rdfa-extractor';
import { getFilePath, getFileMetadata, writeTtlFile, appendTempFile } from './file-helpers';
import { loadExtractionTask, updateTaskStatus, appendTaskError } from './task';

import validateTriple from './validateTriple';
import fixTriple from './fixTriple';

export async function run(deltaEntry) {
  const task = await loadExtractionTask(deltaEntry);
  if (!task) return;

  try {
    updateTaskStatus(task, STATUS_BUSY);

    let pages = await getPages(task);
    const extractor = new RDFAextractor();
    const orginalTempFilePath = `/share/original-${uuid()}.ttl`;
    const validTempFilePath = `/share/valid-triples-${uuid()}.ttl`;
    const invalidTempFilePath = `/share/invalid-triples-${uuid()}.ttl`;
    const correctedTempFilePath = `/share/corrected-triples-${uuid()}.ttl`;
    for (const page of pages) {
      const metaData = await getFileMetadata(page);
      const path = await getFilePath(page);
      const store = await extractor.extractPage(path, metaData);
      const ttl = await extractor.ttl(store);
      await appendTempFile((ttl || []).join('\n'), orginalTempFilePath);
      const { validTriples, invalidTriples, correctedTriples } = await correctAndRepairTriples((ttl || []));
      await appendTempFile(validTriples.join('\n'), validTempFilePath);
      await appendTempFile(invalidTriples.join('\n'), invalidTempFilePath);
      await appendTempFile(correctedTriples.join('\n'), correctedTempFilePath);
    }
    const fileUri = await writeTtlFile(task.graph, orginalTempFilePath, 'original.ttl');

    const fileContainer = { id: uuid() };
    fileContainer.uri = `http://redpencil.data.gift/id/dataContainers/${fileContainer.id}`;
    await appendTaskResultFile(task, fileContainer, fileUri);

    const validFile = await writeTtlFile(task.graph, validTempFilePath, 'valid-triples.ttl');
    await appendTaskResultFile(task, fileContainer, validFile);

    const inValidFile = await writeTtlFile(task.graph, invalidTempFilePath, 'invalid-triples.ttl');
    await appendTaskResultFile(task, fileContainer, inValidFile);

    const correctedFile = await writeTtlFile(task.graph, correctedTempFilePath, 'corrected-triples-[original].ttl');
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
