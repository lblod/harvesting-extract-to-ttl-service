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
import { getFileContent, getFileMetadata, writeTtlFile } from './file-helpers';
import { loadTask, isTask, updateTaskStatus, appendTaskError } from './task';

import validateTriple from './validateTriple';
import fixTriple from './fixTriple';

export async function run(deltaEntry) {
  const task = await loadTask(deltaEntry);
  if (!task) return;

  if (!(await isHarvestingTask(task))) return;

  try {
    updateTaskStatus(task, STATUS_BUSY);

    let pages = await getPages(task);
    let extractor = await constructExtractor(pages);
    const ttl = extractor.ttl();
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

    //const importGraph = `http://mu.semte.ch/graphs/harvesting/tasks/import/${task.id}`;
    //await importTriples(importGraph, validTriples);
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
 * Returns extractor containing all the triples that could be harvested for the given pages/publications.
 *
 * @param pages to be harvested
 */
async function constructExtractor(pages) {
  const extractor = new RDFAextractor();
  for (let page of pages) {
    const html = await getFileContent(page);
    const metaData = await getFileMetadata(page);
    await extractor.addPage(html, metaData);
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

async function importTriples(graph, ttlTriples, batchSize = 100) {
  const triples = [...ttlTriples]; //duplicate so we can splice
  while (triples.length) {
    const batch = triples.splice(0, batchSize);
    const queryStr = `
      INSERT DATA {
        GRAPH ${sparqlEscapeUri(graph)} {
          ${batch.join('\n')}
        }
      }
    `;
    try {
      await update(queryStr);
    } catch (e) {
      /**
       * NOTE:  If the query failed, their is a high probability that this is due to a large triple.
       *        Therefore we keep trying by batching the query into small chunks until we get stuck on an indigestible triple.
       */
      if (batchSize !== 1) {
        await importTriples(graph, batch, Math.ceil(batchSize / 2));
      } else {
        /**
         * NOTE: log the failing query for debugging.
         */
        console.warn('INSERT of a triple failed:');
        console.warn(queryStr);
        throw e;
      }
    }
  }
}
