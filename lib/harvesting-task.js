import {sparqlEscapeUri, uuid, update} from 'mu';

import {querySudo} from '@lblod/mu-auth-sudo';

import {getFileContent} from "./file-helpers";
import RDFAextractor from "./rdfa-extractor";
import validateTriple from './validateTriple';
import fixTriple from './fixTriple';

const N3 = require('n3');

export const TASK_READY = 'http://lblod.data.gift/harvesting-statuses/ready-for-importing';
export const TASK_ONGOING = 'http://lblod.data.gift/harvesting-statuses/importing';
export const TASK_READY_FOR_SAMEAS = 'http://lblod.data.gift/harvesting-statuses/ready-for-sameas';
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
    PREFIX harvesting: <http://lblod.data.gift/vocabularies/harvesting/>
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
        ${sparqlEscapeUri(uri)} a harvesting:HarvestingTask .
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
  const result = await querySudo(`
  ${PREFIXES}
  
  SELECT ?page ?parentUrl
  WHERE {
     GRAPH ?g {
        ${sparqlEscapeUri(taskURI)} prov:generated ?collection .
        ?collection terms:hasPart ?page .
        ?page nie:url ?parentUrl
     }
  }
  `);
  if (result.results.bindings.length) {
    return result.results.bindings.map(binding => ({page: binding.page.value, parentUrl: binding.parentUrl.value}));
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
  for (let pageObject of pages) {
    const {page, parentUrl} = pageObject;
    const html = await getFileContent(page);
    extractor.addPage(html, parentUrl);
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

/**
 * Inserts invalid triples with a unparsedFormOf predicate so we don't have problems with virtuoso
 *
 * @param triples the invalid triples that needs to be inserted
 */
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

/**
 * Parses a triple and returns its parts in an object (subject, predicate, object)
 *
 * @param triple the triple to be parsed
 */
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


/**
 * Check if that predicate already has an unparsedFormOf predicate saved in the database, if
 * it founds anything it returns that predicate, if not it creates a new one and returns it
 *
 * @param predicate the predicate to find an unparsedFormOf
 */
async function getUnparsedPred(predicate) {
  const queryResult = await querySudo(`
    SELECT ?predicate WHERE {
      ?predicate <http://centrale-vindplaats.lblod.info/ns/predicates/unparsedFormOf> ${sparqlEscapeUri(predicate.value)}.
    }
  `);
  if(queryResult.results.bindings && queryResult.results.bindings[0]) {
    return queryResult.results.bindings[0].predicate.value;
  } else {
    const unparsedPred = `http://centrale-vindplaats.lblod.info/ns/predicates/${uuid()}`;
    const labelQueryResult = await querySudo(`
      SELECT ?label WHERE {
        ${sparqlEscapeUri(predicate.value)} <http://www.w3.org/1999/02/22-rdf-syntax-ns#label> ?label.
      }
    `);
    if(labelQueryResult.results.bindings && labelQueryResult.results.bindings[0]) {
      const label = labelQueryResult.results.bindings[0].label.value;
      await querySudo(`
        INSERT DATA {
          GRAPH ${sparqlEscapeUri(TARGET_GRAPH)} {
            ${sparqlEscapeUri(unparsedPred)} <http://centrale-vindplaats.lblod.info/ns/predicates/unparsedFormOf> ${sparqlEscapeUri(predicate.value)};
              <http://www.w3.org/1999/02/22-rdf-syntax-ns#label> """Unparsed of: ${label}"""
          }
        }
      `);
    } else {
      await querySudo(`
        INSERT DATA {
          GRAPH ${sparqlEscapeUri(TARGET_GRAPH)} {
            ${sparqlEscapeUri(unparsedPred)} <http://centrale-vindplaats.lblod.info/ns/predicates/unparsedFormOf> ${sparqlEscapeUri(predicate.value)}.
          }
        }
      `);
    }
    return unparsedPred;
  }
}

/**
 * Returns the string representation of a triple
 *
 * @param subject the subject of the triple
 * @param predicate the predicate of the triple
 * @param object the object of the triple
 */
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