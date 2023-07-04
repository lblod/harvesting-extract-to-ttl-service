import streamToArray from 'stream-to-array';
import { RdfaParser } from 'rdfa-streaming-parser';
import { createUnzip } from 'zlib';
import rdfSerializer from 'rdf-serialize';
import { createReadStream } from 'fs';
import { DataFactory, Store } from 'n3';
const { namedNode } = DataFactory;
import { pipeline as pipelineAsync } from 'stream/promises';

function rdfStreamToStore(stream) {
  const dataset = new Store();
  return new Promise((resolve, reject) =>
    stream.on('data', quad => dataset.addQuad(quad))
      .on('error', reject)
      .once('finish', () => resolve(dataset)));
}

export default class RDFAextractor {

  async addPage(filePath, fileMetadata) {
    const parser = new RdfaParser({baseIRI: fileMetadata.url});
    let rdfDataset;
    try {
      if (filePath.endsWith('.gz')) {
        rdfDataset = await pipelineAsync(
          createReadStream(filePath),
          createUnzip(),
          parser,
          rdfStreamToStore
        );
      }
      else {
        rdfDataset = await pipelineAsync(
          createReadStream(filePath),
          parser,
          rdfStreamToStore
        );
      }
    } catch(e) {
      console.error(`ERROR extracting file with path ${filePath}`,e);
      throw e;
    }
    const extractedSubjects = rdfDataset.getSubjects();
    for (const subject of extractedSubjects) {
      rdfDataset.addQuad(subject, namedNode('http://www.w3.org/ns/prov#wasDerivedFrom') , namedNode(fileMetadata.url) );
    }
    if (! this.triples ) {
      this.triples = new Store();
    }
    this.triples.addQuads(rdfDataset.getQuads());
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
