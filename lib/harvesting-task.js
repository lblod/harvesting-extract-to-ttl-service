import {sparqlEscapeUri} from 'mu';

import {querySudo as query} from '@lblod/mu-auth-sudo';

import {getFileContent} from "./file-helpers";
import RdfaExtractor from "./rdfa-extractor";

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
  console.log(ttl);
}