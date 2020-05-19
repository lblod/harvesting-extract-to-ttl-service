import {sparqlEscapeUri} from 'mu';

import {querySudo as query, updateSudo as update} from '@lblod/mu-auth-sudo';

import {getFileContent} from "./file-helpers";
import RdfaExtractor from "./rdfa-extractor";

export const TASK_READY = 'http://lblod.data.gift/harvesting-statuses/ready-for-importing';
export const TASK_ONGOING = 'http://lblod.data.gift/harvesting-statuses/importing';
export const TASK_SUCCESS = 'http://lblod.data.gift/harvesting-statuses/success';
export const TASK_FAILURE = 'http://lblod.data.gift/harvesting-statuses/failure';

const PREFIXES = `
PREFIX harvesting: <http://lblod.data.gift/vocabularies/harvesting/>
PREFIX terms: <http://purl.org/dc/terms/>
PREFIX prov: <http://www.w3.org/ns/prov#>
PREFIX nie: <http://www.semanticdesktop.org/ontologies/2007/01/19/nie#>
`

// PUBLIC METHODS

/**
 * Imports the given harvesting task into the db by harvesting the linked pages/publications.
 *
 * @param taskURI the URI of the harvesting-task to import.
 */
export async function importHarvestingTask(taskURI) {
  let pages = await getPublicationPages(taskURI);
  let extractor = await extractTTL(pages);
  await importTTL(extractor);
}

/**
 * Updates the state of the given task to the specified status.
 *
 * @param uri URI of the task.
 * @param status URI of the new status.
 */
export async function updateTaskStatus(uri, status) {
  const q = `
    PREFIX melding: <http://lblod.data.gift/vocabularies/automatische-melding/>
    PREFIX adms: <http://www.w3.org/ns/adms#>

    DELETE {
      GRAPH ?g {
        ${sparqlEscapeUri(uri)} adms:status ?status .
      }
    } WHERE {
      GRAPH ?g {
        ${sparqlEscapeUri(uri)} adms:status ?status .
      }
    }

    ;

    INSERT {
      GRAPH ?g {
        ${sparqlEscapeUri(uri)} adms:status ${sparqlEscapeUri(status)} .
      }
    } WHERE {
      GRAPH ?g {
        ${sparqlEscapeUri(uri)} a melding:HarvestingTask .
      }
    }

  `;

  await update(q);
}

// PRIVATE METHODS

/**
 * Returns all the linked html-pages/publications from the given harvesting-task URI.
 *
 * @param taskURI the URI of the harvesting-task to import.
 */
async function getPublicationPages(taskURI) {
  const result = await query(`
  ${PREFIXES}
  
  SELECT ?page
  WHERE {
     GRAPH <http://mu.semte.ch/graphs/public> {
        ${sparqlEscapeUri(taskURI)} prov:generated ?collection .
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

/**
 * Returns TTL containing all the triples that could be harvested for the given pages/publications.
 *
 * @param pages to be harvested
 */
async function extractTTL(pages) {
  const extractor = new RdfaExtractor();
  for (let page of pages) {
    extractor.html = await getFileContent(page);
    extractor.rdfa();
  }
  return extractor;
}

/**
 * Imports the given TTL in to the db.
 *
 * @param extractor contains everything we need to write to the db.
 */
async function importTTL(extractor) {
  if (extractor.triples.length === 0) {
    console.log("No data dump will occur, no triples where found.")
  } else {
    console.log("Starting triples dump into the db:")
    update(`
  INSERT DATA {
    GRAPH <http://mu.semte.ch/graphs/public> {
        ${extractor.ttl()}
    }
  }
  `);
  }
}