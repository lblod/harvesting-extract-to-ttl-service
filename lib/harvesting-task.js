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
      const ttlArray = extractor.ttl()
      const validTriples = []
      const invalidTriples = []
      for(let i = 0; i < ttlArray.length; i++) {
        const triple = ttlArray[i];
        if(await validateTriple(triple)){
          validTriples.push(triple)
        } else {
          invalidTriples.push(triple)
        }
      }
      for(let j = 0; j < invalidTriples.length; j++) {
        const invalidTriple = invalidTriples[j]
        const fixedTriple = await fixTriple(invalidTriple)
        if(fixedTriple) {
          validTriples.push(fixedTriple)
        }
      }
      while(validTriples.length) {
        const batch = validTriples.splice(0, 100);
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
    const datatype = part.datatype.value
    if(datatype === 'http://www.w3.org/2001/XMLSchema#string' || datatype === 'http://www.w3.org/1999/02/22-rdf-syntax-ns#langString') {
      return true;
    } else if(datatype === 'http://www.w3.org/2000/01/rdf-schema#Literal') {
      return true;
    } else if(datatype === 'http://www.w3.org/2001/XMLSchema#boolean') {
      return validateBoolean(part.value);
    } else if(datatype === 'http://www.w3.org/2001/XMLSchema#date') {
      return validateDate(part.value);
    } else if(datatype === 'http://www.w3.org/2001/XMLSchema#dateTime') {
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


function fixTriple(triple) {
  return new Promise((resolve, reject) => {
    const parser = new N3.Parser();
    parser.parse(triple, (error, quad, prefixes) => {
      if(error) {
        console.log(error);
        resolve(undefined);
      } else {
        if(!quad) return resolve(undefined);
        const subject = fixPart(quad.subject);
        if(!subject) return resolve(undefined);
        const predicate = fixPart(quad.predicate);
        if(!predicate) return resolve(undefined);
        const object = fixPart(quad.object);
        if(!object) return resolve(undefined);
        const writer = new N3.Writer();
        writer.addQuad(subject, predicate, object);
        writer.end((err, result) => {
          if(err) {
            console.log(err);
            resolve(undefined);
          } else {
            resolve(result);
          }
        })
      }
    })
  })
}

function fixPart(part) {
  if(validatePart(part)) {
    return part;
  } else {
    if(part.datatype) {
      if(part.datatype.value === 'http://www.w3.org/2001/XMLSchema#boolean') {
        return fixBoolean(part);
      } else if(part.datatype.value === 'http://www.w3.org/2001/XMLSchema#date') {
        return fixDate(part);
      } else if(part.datatype.value === 'http://www.w3.org/2001/XMLSchema#dateTime') {
        return fixDateTime(part);
      }
    }
    return undefined;
  }
}

function fixBoolean(part) {
  const lowercaseValue = part.value.toLowerCase();
  if(lowercaseValue === 'true' || lowercaseValue === 'false') {
    return N3.DataFactory.literal(lowercaseValue, 'http://www.w3.org/2001/XMLSchema#boolean');
  } else {
    return undefined;
  }
}

function fixDate(part) {
  const value = part.value;
  const date = new Date(value);
  if(isValidDate(date)) {
    const year = date.getFullYear();
    const month = date.getMonth() < 9 ? `0${date.getMonth()+1}` : date.getMonth()+1;
    const day = date.getDate();
    const newValue = `${year}-${month}-${day}`;
    return N3.DataFactory.literal(newValue, 'http://www.w3.org/2001/XMLSchema#date');
  }
}

function isValidDate(d) {
  return d instanceof Date && !isNaN(d);
}

function fixDateTime(part) {
  const value = part.value;
  const date = new Date(value);
  if(isValidDate(date)) {
    const year = date.getFullYear();
    const month = date.getMonth() < 9 ? `0${date.getMonth()+1}` : date.getMonth()+1;
    const day = date.getDate();
    const hour = date.getHours();
    const minute = date.getMinutes();
    const seconds = date.getSeconds();
    const newValue = `${year}-${month}-${day}T${hour}:${minute}:${seconds}`;
    return N3.DataFactory.literal(newValue, 'http://www.w3.org/2001/XMLSchema#dateTime');
  }
}