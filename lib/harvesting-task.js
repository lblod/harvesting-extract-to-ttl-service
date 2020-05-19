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

export async function importHarvestingTask(task) {
  let pages = await getPublicationPages(task);
  let ttl = await extractTTL(pages);
  await importTTL(ttl);
}

/**
 * Updates the state of the given task to the specified status
 *
 * @param uri URI of the task
 * @param status URI of the new status
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

async function getPublicationPages(taskURI) {
  const result = await query(`
  ${PREFIXES}
  
  SELECT ?page
  WHERE {
     ${sparqlEscapeUri(taskURI)} prov:generated ?collection .
     ?collection terms:hasPart ?page .
  }
  `);
  if (result.results.bindings.length) {
    return result.results.bindings.map(binding => binding['page'].value);
  } else {
    return [];
  }
}

async function extractTTL(pages) {
  const extractor = new RdfaExtractor();
  for (let page of pages) {
    extractor.html = await getFileContent(page);
    extractor.rdfa();
  }
  return extractor.ttl();
}

async function importTTL(ttl) {
  console.log("Dumping triples into the db:")
  console.log(ttl);
  update(`
  INSERT DATA {
    ${ttl}
  }
  `);
}