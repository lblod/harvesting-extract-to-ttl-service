import jsdom from 'jsdom';
import { analyse } from '@lblod/marawa/rdfa-context-scanner';
import flatten from 'lodash.flatten';
import uniqWith from 'lodash.uniqwith';
import Triple from './triple';

class RDFAextractor {

  addPage(html, fileMetaData) {
    const dom = new jsdom.JSDOM(html);
    const domNode = dom.window.document.querySelector('body');

    const blocks = analyse(domNode, [], { documentUrl: fileMetaData.url });
    const triples = flatten(blocks.map(b => b.context)).map(t => new Triple(t));
    this.add(triples);
  }

  add(triples) {
    const allTriples = (this.triples || []).concat(triples);
    this.triples = uniqWith(allTriples, (a, b) => a.isEqual(b));
  }

  NTArray() {
    if (this.triples === undefined) {
      console.log('No triples found. Did you extract RDFa already?');
      return [];
    }
    else {
      const ntTriples = [];
      for(const t of this.triples){
        try {
          ntTriples.push(t.toNT());
        }
        catch(e) {
          console.log(`rdfa extractor WARNING: invalid statement: <${t.subject}> <${t.predicate}> ${t.object}\n` + e);
        }
      }
      return ntTriples;
    }
  }
}

export default RDFAextractor;
