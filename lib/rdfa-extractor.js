import streamToArray from 'stream-to-array';
import rdfParser from 'rdf-parse';
import rdfSerializer from 'rdf-serialize';
import { DataFactory, Store } from 'n3';
const { namedNode } = DataFactory;

function rdfStreamToStore(stream) {
  const dataset = new Store();
  return new Promise((resolve, reject) =>
    stream.on('data', quad => dataset.addQuad(quad))
      .on('error', reject)
      .once('finish', () => resolve(dataset)));
}

export default class RDFAextractor {

  async addPage(htmlStream, fileMetadata) {
    const rdfaStream = rdfParser.parse(htmlStream, { contentType: 'text/html', baseIRI: fileMetadata.url });
    const rdfDataset = await rdfStreamToStore(rdfaStream);
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
