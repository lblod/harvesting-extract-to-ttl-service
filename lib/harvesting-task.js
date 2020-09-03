import {sparqlEscapeUri, uuid} from 'mu';

import {querySudo as query, updateSudo as update} from '@lblod/mu-auth-sudo';

import {getFileContent} from "./file-helpers";
import RDFAextractor from "./rdfa-extractor";
const N3 = require('n3');

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
`;

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
      const ttlArray = extractor.ttl();
      const validTriples = [];
      const toBeFixedTriples = [];
      for(let i = 0; i < ttlArray.length; i++) {
        const triple = ttlArray[i];
        if(await validateTriple(triple)){
          validTriples.push(triple);
        } else {
          toBeFixedTriples.push(triple);
        }
      }
      const invalidTriples = [];
      for(let j = 0; j < toBeFixedTriples.length; j++) {
        const toBeFixed = toBeFixedTriples[j];
        const fixedTriple = await fixTriple(toBeFixed);
        if(fixedTriple) {
          validTriples.push(fixedTriple);
        } else {
          invalidTriples.push(toBeFixed);
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
      await insertInvalidTriples(invalidTriples);
  }catch(e) {
    console.log(e);
  }
  }
}

function validateTriple(triple) {
  return new Promise((resolve) => {
    const parser = new N3.Parser();
    parser.parse(triple, (error, quad) => {
      if(error) {
        console.log(error);
        resolve(false);
      } else {
        const valid = quad && validateTerm(quad.subject) && validateTerm(quad.predicate) && validateTerm(quad.object);
        resolve(valid);
      }
    });
  });
}

function validateTerm(term) {
  if(!term.datatype) {
    return true;
  } else {
    const datatype = term.datatype.value;
    if(
        datatype === 'http://www.w3.org/2001/XMLSchema#string' || 
        datatype === 'http://www.w3.org/1999/02/22-rdf-syntax-ns#langString' ||
        datatype === 'http://www.w3.org/2000/01/rdf-schema#Literal'
      ) {
      return true;
    } else if(datatype === 'http://www.w3.org/2000/01/rdf-schema#Literal') {
      return true;
    } else if(datatype === 'http://www.w3.org/2001/XMLSchema#boolean') {
      return validateBoolean(term.value);
    } else if(datatype === 'http://www.w3.org/2001/XMLSchema#date') {
      return validateDate(term.value);
    } else if(datatype === 'http://www.w3.org/2001/XMLSchema#dateTime') {
      return validateDateTime(term.value);
    } else if(datatype === 'http://www.w3.org/2001/XMLSchema#integer'){
      return validateNumber(term.value);
    } else {
      return false;
    }
  }
}

function validateBoolean(value) {
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

function validateNumber(value) {
  const numberValue = Number(value);
  return !isNaN(numberValue);
}


function fixTriple(triple) {
  return new Promise((resolve) => {
    const parser = new N3.Parser();
    parser.parse(triple, (error, quad) => {
      if(error) {
        console.log(error);
        resolve(undefined);
      } else {
        if(!quad) return resolve(undefined);

        const subject = fixTerm(quad.subject);
        if(!subject) return resolve(undefined);

        const predicate = fixTerm(quad.predicate);
        if(!predicate) return resolve(undefined);

        const object = fixTerm(quad.object);
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
        });
      }
    });
  });
}

function fixTerm(term) {
  if(validateTerm(term)) {
    return term;
  } else {
    if(term.datatype) {
      if(term.datatype.value === 'http://www.w3.org/2001/XMLSchema#boolean') {
        return fixBoolean(term);
      } else if(term.datatype.value === 'http://www.w3.org/2001/XMLSchema#date') {
        return fixDate(term);
      } else if(term.datatype.value === 'http://www.w3.org/2001/XMLSchema#dateTime') {
        return fixDateTime(term);
      }
    }
    return undefined;
  }
}

function fixBoolean(term) {
  const lowercaseValue = term.value.toLowerCase();
  if(lowercaseValue === 'true' || lowercaseValue === 'false') {
    return N3.DataFactory.literal(lowercaseValue, 'http://www.w3.org/2001/XMLSchema#boolean');
  } else {
    return undefined;
  }
}

function fixDate(term) {
  const value = term.value;
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

function fixDateTime(term) {
  const value = term.value;
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

async function insertInvalidTriples(triples) {
  const triplesToWrite = [];
  for(let i = 0; i < triples.length ; i++){
    try {
      const triple = triples[i];
      const parsedTriple = await parseTriple(triple);
      const unparsedPred = await getUnparsedPred(parsedTriple.predicate);
      const tripleString = await getStringRepresentation(parsedTriple.subject, N3.DataFactory.namedNode(unparsedPred), N3.DataFactory.literal(parsedTriple.object.value));
      triplesToWrite.push(tripleString);
    } catch(e) {
      console.log(e);
    }
  } 
  while(triplesToWrite.length) {
    const batch = triplesToWrite.splice(0, 100);
    await update(`
      INSERT DATA {
        GRAPH ${sparqlEscapeUri(TARGET_GRAPH)} {
            ${batch.join('\n')}
        }
      }
    `);
  }
}

function parseTriple(triple) {
  return new Promise((resolve, reject) => {
    const parser = new N3.Parser();
    parser.parse(triple, (error, quad) => {
      if(error) {
        reject(error);
      } else {
        resolve(quad);
      }
    });
  });
}



async function getUnparsedPred(predicate) {
  const queryResult = await query(`
    SELECT ?predicate WHERE {
      ?predicate <http://data.lblod.info/ns/predicates/unparsedFormOf> ${sparqlEscapeUri(predicate.value)}.
    }
  `);
  if(queryResult.results.bindings && queryResult.results.bindings[0]) {
    return queryResult.results.bindings[0].predicate.value;
  } else {
    const unparsedPred = `http://data.lblod.info/ns/predicates/${uuid()}`;
    const labelQueryResult = await query(`
      SELECT ?label WHERE {
        ${sparqlEscapeUri(predicate.value)} <http://www.w3.org/1999/02/22-rdf-syntax-ns#label> ?label.
      }
    `);
    if(labelQueryResult.results.bindings && labelQueryResult.results.bindings[0]) {
      const label = labelQueryResult.results.bindings[0].label.value;
      await query(`
        INSERT DATA {
          GRAPH ${sparqlEscapeUri(TARGET_GRAPH)} {
            ${sparqlEscapeUri(unparsedPred)} <http://data.lblod.info/ns/predicates/unparsedFormOf> ${sparqlEscapeUri(predicate.value)};
              <http://www.w3.org/1999/02/22-rdf-syntax-ns#label> """Unparsed of: ${label}"""
          }
        }
      `);
    } else {
      await query(`
        INSERT DATA {
          GRAPH ${sparqlEscapeUri(TARGET_GRAPH)} {
            ${sparqlEscapeUri(unparsedPred)} <http://data.lblod.info/ns/predicates/unparsedFormOf> ${sparqlEscapeUri(predicate.value)}.
          }
        }
      `);
    }
    return unparsedPred;
  }
}

function getStringRepresentation(subject, predicate, object) {
  return new Promise((resolve, reject) => {
    const writer = new N3.Writer();
    writer.addQuad(subject, predicate, object);
    writer.end((err, result) => {
      if(err) {
        console.log(err);
        reject(err);
      } else {
        resolve(result);
      }
    });
  });
}