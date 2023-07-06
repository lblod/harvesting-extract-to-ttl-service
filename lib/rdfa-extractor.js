import streamToArray from 'stream-to-array';
import { RdfaParser } from 'rdfa-streaming-parser';
import { createUnzip } from 'zlib';
import rdfSerializer from 'rdf-serialize';
import { createReadStream } from 'fs';
import { DataFactory, Store } from 'n3';
const { namedNode, quad } = DataFactory;
import { pipeline as pipelineAsync } from 'stream/promises';
import { Writable } from 'stream';



class SourceAwareStoreWriter extends Writable {
  constructor(url, store) {
    super({ objectMode: true });
    this.subjects = [];
    this.store = store;
    this.url = namedNode(url);
  }

  _write(data, _enc, next) {
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
  async extractPage(filePath, fileMetadata) {
    const store = new Store();
    const parser = new RdfaParser({ baseIRI: fileMetadata.url });
    const writer = new SourceAwareStoreWriter(fileMetadata.url, store);
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
