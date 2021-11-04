import jsdom from 'jsdom';

import streamify from 'streamify-string';
import streamToArray from 'stream-to-array';
import rdfParser from 'rdf-parse';
import rdfSerializer from 'rdf-serialize';

class RDFAextractor {

  async addPage(html, fileMetadata) {
    const dom = new jsdom.JSDOM(html);
    const domNode = dom.window.document.querySelector('body');
    const textStream = streamify(domNode.innerHTML);
    const rdfaStream = rdfParser.parse(textStream, { contentType: 'text/html', baseIRI: fileMetadata.url });
    const turtleStream = rdfSerializer.serialize(rdfaStream, { contentType: 'application/n-triples' });
    const triples = await streamToArray(turtleStream);
    this.add(triples);
  }

  add(triples) {
    this.triples = (this.triples || []).concat(triples);
  }

  ttl() {
    if (this.triples === undefined) {
      console.log('No triples found. Did you extract RDFa already?');
      return null;
    } else {
      return this.triples
        .map(s => s.trim())
        .filter(s => s.length)
        .map(t => t.endsWith('.') ? t : t + '.');
    }
  }
}

export default RDFAextractor;
