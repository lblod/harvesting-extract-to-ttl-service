import jsdom from 'jsdom';
import { analyse } from '@lblod/marawa/rdfa-context-scanner';
import flatten from 'lodash.flatten';
import uniqWith from 'lodash.uniqwith';
import Triple from './triple';

const streamify = require("streamify-string");
const streamToArray = require('stream-to-array');
const rdfParser = require("rdf-parse").default;
const rdfSerializer = require("rdf-serialize").default;

class RDFAextractor {

  addPage(html, fileMetadata) {
    const dom = new jsdom.JSDOM(html);
    const domNode = dom.window.document.querySelector('body');
    const textStream = streamify(domNode.innerHTML);
    const rdfaStream = rdfParser.parse(textStream, { contentType: 'text/html', baseIRI: fileMetadata.url });
    const turtleStream = rdfSerializer.serialize(rdfaStream, { contentType: 'text/n3' });
    const triples = await streamToArray(turtleStream);
    this.add(triples.map(t => t + '.'));
  }

  add(triples) {
    const allTriples = (this.triples || []).concat(triples);
    this.triples = uniqWith(allTriples, (a, b) => a.isEqual(b));
  }

  ttl() {
    if (this.triples === undefined) {
      console.log('No triples found. Did you extract RDFa already?');
      return null;
    } else {
      return this.triples.map(t => {
        try {
          return t.toNT();
        }
        catch (e) {
          console.log(`rdfa extractor WARNING: invalid statement: <${t.subject}> <${t.predicate}> ${t.object}\n` + e);
          return "";
        }
      });
    }
  }
}

export default RDFAextractor;
