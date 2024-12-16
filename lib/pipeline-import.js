import { sparqlEscapeUri, sparqlEscapeString, uuid } from "mu";
import { querySudo as query, updateSudo as update } from "@lblod/mu-auth-sudo";
import {
  PREFIXES,
  STATUS_BUSY,
  STATUS_SUCCESS,
  STATUS_FAILED,
  HIGH_LOAD_DATABASE_ENDPOINT,
  WRITE_DEBUG_TTLS,
} from "../constants";
const connectionOptions = {
  sparqlEndpoint: HIGH_LOAD_DATABASE_ENDPOINT,
  mayRetry: true,
};
import RDFAextractor from "./rdfa-extractor";
import {
  getFilePath,
  getFileMetadata,
  writeFileToTriplestore,
  extractBasenameFromPath,
} from "./file-helpers";
import { loadExtractionTask, updateTaskStatus, appendTaskError } from "./task";

import { getHeapStatistics } from "v8";
import validateTriple from "./validateTriple";
import fixTriple from "./fixTriple";

export async function run(deltaEntry) {
  const task = await loadExtractionTask(deltaEntry);
  if (!task) return;

  try {
    await updateTaskStatus(task, STATUS_BUSY);

    let pages = await getPages(task);

    const fileContainer = { id: uuid() };
    fileContainer.uri = `http://redpencil.data.gift/id/dataContainers/${fileContainer.id}`;
    const importGraph = { id: uuid() };
    importGraph.uri = `http://mu.semte.ch/graphs/harvesting/tasks/import/${task.id}`;

    const graphContainer = { id: uuid() };
    graphContainer.uri = `http://redpencil.data.gift/id/dataContainers/${graphContainer.id}`;
    for (let i = 0; i < pages.length; i++) {
      const extractor = new RDFAextractor();
      const metaData = await getFileMetadata(pages[i]);
      const path = await getFilePath(pages[i]);
      // optimization
      // give a chance for  gc to do its thing in case memory usage >= 70%
      // this is due to memory leaks in jsdom
      const memoryUsage = process.memoryUsage();
      const memoryUsagePrc =
        (memoryUsage.heapUsed / getHeapStatistics().heap_size_limit) * 100;
      console.log("consumed already ", memoryUsagePrc, "% of memory");

      if (memoryUsagePrc > 70) {
        console.log(
          "use more than 70% memory, wait a lil bit to allow gc to cleanup stuff",
        );
        await new Promise((r) => setTimeout(r, 5000));
      }
      // end optimization
      let store;
      try {
        // if we cannot extract the html page, we continue
        store = await extractor.extractPage(task, path, metaData);
      } catch (e) {
        console.log(e?.message);
        continue;
      }
      const ttl = await extractor.ttl(store);
      const basename = extractBasenameFromPath(path);
      const { validTriples, invalidTriples, correctedTriples } =
        await correctAndRepairTriples(ttl || []);
      const validFile = await writeFileToTriplestore(
        task.graph,
        validTriples,
        `${basename}-valid.ttl`,
        pages[i],
      );
      await appendTaskResultFile(task, fileContainer, validFile);
      await appendTaskResultFile(task, importGraph, validFile);
      await appendTaskResultGraph(task, graphContainer, importGraph.uri);
      if (WRITE_DEBUG_TTLS) {
        const originalFile = await writeFileToTriplestore(
          task.graph,
          ttl,
          `${basename}-original.ttl`,
          pages[i],
        );
        await appendTaskResultFile(task, fileContainer, originalFile, pages[i]);
        const invalidFile = await writeFileToTriplestore(
          task.graph,
          invalidTriples,
          `${basename}-invalid.ttl`,
          pages[i],
        );
        await appendTaskResultFile(task, fileContainer, invalidFile);
        const correctedFile = await writeFileToTriplestore(
          task.graph,
          correctedTriples,
          `${basename}-corrected.ttl`,
          pages[i],
        );
        await appendTaskResultFile(task, fileContainer, correctedFile);
      }
    }

    await updateTaskStatus(task, STATUS_SUCCESS);
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
  const count = await countPages(task);
  const defaultLimitSize = 1000;
  let res = new Set();
  const queryFn = async (limitSize, offset) => {
    const q = `
    ${PREFIXES}
    SELECT ?page where {
      SELECT distinct ?page
      WHERE {
        GRAPH ?g {
          ${sparqlEscapeUri(task.task)} task:inputContainer ?container.
          ?container task:hasFile ?page.
       }
      } order by ?page
    } limit ${limitSize} offset ${offset}`;
    const result = await query(q, {}, connectionOptions);
    if (result.results.bindings.length) {
      return result.results.bindings.map((binding) => binding["page"].value);
    } else {
      return [];
    }
  };
  const pagesCount =
    count > defaultLimitSize ? Math.ceil(count / defaultLimitSize) : 1;
  for (let page = 0; page <= pagesCount; page++) {
    const pageRes = await queryFn(defaultLimitSize, page * defaultLimitSize);
    pageRes.forEach((element) => {
      res.add(element);
    });
  }
  console.log(`res.size: ${res.size}, count: ${count}`);
  return [...res.values()];
}
async function countPages(task) {
  const result = await query(
    `
  ${PREFIXES}
  SELECT (count(distinct ?page) as ?count)
  WHERE {
     GRAPH ?g {
        ${sparqlEscapeUri(task.task)} task:inputContainer ?container.
        ?container task:hasFile ?page.
     }
  }
  `,
    {},
    connectionOptions,
  );
  if (result.results.bindings.length) {
    return result.results.bindings[0].count.value;
  } else {
    return 0;
  }
}

async function appendTaskResultFile(task, container, fileUri) {
  // prettier-ignore
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

  await update(queryStr, {}, connectionOptions);
}

async function appendTaskResultGraph(task, container, graphUri) {
  // prettier-ignore
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

  await update(queryStr, {}, connectionOptions);
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
