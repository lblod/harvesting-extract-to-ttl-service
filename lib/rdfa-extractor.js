import streamToArray from 'stream-to-array';
import { RdfaParser } from 'rdfa-streaming-parser';
import { createUnzip } from 'zlib';
import rdfSerializer from 'rdf-serialize';
import { createReadStream } from 'fs';
import { DataFactory, Store } from 'n3';
const { namedNode, quad } = DataFactory;
import { pipeline as pipelineAsync } from 'stream/promises';
import { Transform } from 'stream';


class SourceAnnotator extends Transform {
  constructor(url) {
    super({objectMode: true});
    this.subjects = [];
    this.url = namedNode(url);
  }

  _transform(data, _enc, next) {
    this.push(data);
    if (! this.subjects.includes(data.subject.value)) {
      this.subjects.push(data.subject.value);
      this.push(
        quad(
          data.subject,
          namedNode('http://www.w3.org/ns/prov#wasDerivedFrom') ,
          this.url
        ));
    }
    next();
  }
}

export default class RDFAextractor {

  constructor() {
    this.triples = new Store();
  }

  async addPage(filePath, fileMetadata) {
    const parser = new RdfaParser({baseIRI: fileMetadata.url});
    const sourceAnnotator = new SourceAnnotator(fileMetadata.url);
    try {
      if (filePath.endsWith('.gz')) {
        await pipelineAsync(
          createReadStream(filePath),
          createUnzip(),
          parser,
          sourceAnnotator,
          this.triples.import
        );
      }
      else {
        await pipelineAsync(
          createReadStream(filePath),
          parser,
          sourceAnnotator,
          this.triples.import
        );
      }
    } catch(e) {
      console.error(`ERROR extracting file with path ${filePath}`,e);
      throw e;
    }
  }

  async ttl() {
    if (this.triples.size === 0) {
      console.log('No triples found. Did you extract RDFa already?');
      return null;
    } else {
      const turtleStream =  rdfSerializer.serialize(this.triples.match(), { contentType: 'application/n-triples' });
      const turtle = await streamToArray(turtleStream);
      return turtle.map(s => s.trim()).filter(s => s.length);
    }
  }
}
