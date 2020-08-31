import {sparqlEscapeUri} from 'mu';

import {querySudo as query, updateSudo as update} from '@lblod/mu-auth-sudo';

import {getFileContent} from "./file-helpers";
import RDFAextractor from "./rdfa-extractor";
const N3 = require('n3')

export const TASK_READY = 'http://lblod.data.gift/harvesting-statuses/ready-for-importing';
export const TASK_ONGOING = 'http://lblod.data.gift/harvesting-statuses/importing';
export const TASK_SUCCESS = 'http://lblod.data.gift/harvesting-statuses/success';
export const TASK_FAILURE = 'http://lblod.data.gift/harvesting-statuses/failure';

const TARGET_GRAPH = process.env.TARGET_GRAPH || 'http://mu.semte.ch/graphs/public';

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
  let pages = await getPages(taskURI);
  let extractor = await constructExtractor(pages);
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
async function getPages(taskURI) {
  const result = await query(`
  ${PREFIXES}
  
  SELECT ?page
  WHERE {
     GRAPH ?g {
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
 * Returns extractor containing all the triples that could be harvested for the given pages/publications.
 *
 * @param pages to be harvested
 */
async function constructExtractor(pages) {
  const extractor = new RDFAextractor();
  for (let page of pages) {
    const html = await getFileContent(page);
    extractor.addPage(html);
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
    console.log("No data dump will occur, no triples where found.");
  } else {
    console.log("Starting triples dump into the db:");
    console.log('This is dev');
    try {
      const ttlArray = await Promise.all(extractor.ttl().map(async (triple) => {
        if(await validateTriple(triple)) {
          return triple;
        } else {
          return '';
        }
      }));
      while(ttlArray.length) {
        const batch = ttlArray.splice(0, 100);
        await update(`
          INSERT DATA {
            GRAPH ${sparqlEscapeUri(TARGET_GRAPH)} {
                ${batch.join('\n')}
            }
          }
        `);
      }
  }catch(e) {
    console.log(e);
  }
  }
}

function validateTriple(triple) {
  return new Promise((resolve, reject) => {
    const parser = new N3.Parser();
    parser.parse(triple, (error, quad, prefixes) => {
      if(error) {
        console.log(error);
        resolve(false);
      } else {
        const valid = quad && validatePart(quad.subject) && validatePart(quad.predicate) && validatePart(quad.object);
        resolve(valid);
      }
    })
  })
}

function validatePart(part) {
  if(!part.datatype) {
    return true;
  } else {
    if(part.datatype.value === 'http://www.w3.org/2001/XMLSchema#string') {
      return true;
    } else if(part.datatype.value === 'http://www.w3.org/2000/01/rdf-schema#Literal') {
      return true;
    } else if(part.datatype.value === 'http://www.w3.org/2001/XMLSchema#boolean') {
      return validateBoolean(part.value);
    } else if(part.datatype.value === 'http://www.w3.org/2001/XMLSchema#date') {
      return validateDate(part.value);
    } else if(part.datatype.value === 'http://www.w3.org/2001/XMLSchema#dateTime') {
      return validateDateTime(part.value);
    } else {
      return false;
    }
  }
}

function validateBoolean(value) {
  return value === "true" || value === "false";
}

function validateDate(value) {
  const dateRegex = /^-?[0-9][0-9][0-9][0-9]+-[0-9][0-9]-[0-9][0-9](([-+][0-9][0-9]:[0-9][0-9])|Z)?$/;
  //TODO invalid dates are not checked like 1997-99-99
  return dateRegex.test(value);
}

function validateDateTime(value) {
  const dateTimeRegex = /^-?[0-9][0-9][0-9][0-9]+-[0-9][0-9]-[0-9][0-9]T[0-9][0-9]:[0-9][0-9]:[0-9][0-9](\.[0-9]+)?(([-+][0-9][0-9]:[0-9][0-9])|Z)?$/;
  //TODO invalid dates are not checked like 1997-99-99 or invalid times like 26:78:98
  return dateTimeRegex.test(value);
}