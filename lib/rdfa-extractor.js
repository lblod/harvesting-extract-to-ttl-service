import streamToArray from 'stream-to-array';
import { RdfaParser } from '@lblod/rdfa-streaming-parser';
import { createUnzip } from 'zlib';
import rdfSerializer from 'rdf-serialize';
import { createReadStream } from 'fs';
import { DataFactory, Store } from 'n3';
const { namedNode, quad } = DataFactory;
import { pipeline as pipelineAsync } from 'stream/promises';
import { Writable } from 'stream';
import { writeFileToTriplestore, appendTempFile } from './file-helpers';

import { uuid } from 'mu';
const PROV_VALUE_PRED = "http://www.w3.org/ns/prov#value";


class SourceAwareStoreWriter extends Writable {
  constructor(task, url, store) {
    super({ objectMode: true });
    this.subjects = [];
    this.store = store;
    this.url = namedNode(url);
    this.task = task;
  }

  async _write(data, _enc, next) {
    if (data.predicate?.value === PROV_VALUE_PRED &&
      data.object.datatype?.value === 'http://www.w3.org/1999/02/22-rdf-syntax-ns#HTML') {
      // store in a file
      // TODO I didn't know what predicate should I use (could be problematic to 
      // have a list of prov:value ?
      // thus I just add a HTML suffix to the result
      const htmlTempFilePath = `/share/html-${uuid()}.html`;
      await appendTempFile(data.object.value, htmlTempFilePath);
      const htmlUri = await writeFileToTriplestore(this.task.graph, htmlTempFilePath, "content.html", "text/html", "html");
      data.predicate.value += "HTML";
      data.object = namedNode(htmlUri);
      console.log('goes in');
    }
    this.store.addQuad(data);
    if (!this.subjects.includes(data.subject.value)) {
      this.subjects.push(data.subject.value);
      this.store.addQuad(
        quad(
          data.subject,
          namedNode('http://www.w3.org/ns/prov#wasDerivedFrom'),
          this.url
        )
      );
    }
    return next();
  }
}
export default class RDFAextractor {
  async extractPage(task, filePath, fileMetadata) {
    const store = new Store();
    const parser = new RdfaParser({ baseIRI: fileMetadata.url, htmlPredicates: [PROV_VALUE_PRED] });
    const writer = new SourceAwareStoreWriter(task, fileMetadata.url, store);
    try {
      if (filePath.endsWith('.gz')) {
        await pipelineAsync(
          createReadStream(filePath),
          createUnzip(),
          parser,
          writer
        );
      }
      else {
        await pipelineAsync(
          createReadStream(filePath),
          parser,
          writer
        );
      }
      return store;
    } catch (e) {
      console.error(`ERROR extracting file with path ${filePath}`, e);
      throw e;
    }
  }

  async ttl(store) {
    if (store.size === 0) {
      console.log('No triples found. Did you extract RDFa already?');
      return null;
    } else {
      const turtleStream = rdfSerializer.serialize(store.match(), { contentType: 'application/n-triples' });
      const turtle = await streamToArray(turtleStream);
      return turtle.map(s => s.trim()).filter(s => s.length);
    }
  }
}
