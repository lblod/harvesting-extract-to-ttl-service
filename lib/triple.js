import { sparqlEscapeUri, sparqlEscapeString } from 'mu';

export default class Triple {
  constructor({ subject, predicate, object, datatype }) {
    this.subject = subject;
    this.predicate = predicate;
    this.object = object;
    this.datatype = datatype;
  }

  isEqual(other) {
    return this.subject === other.subject
      && this.predicate === other.predicate
      && this.object === other.object
      && this.datatype === other.datatype;
  }

  toNT() {
    const predicate = this.predicate === 'a' ? this.predicate : sparqlEscapeUri(this.predicate);
    let obj;
    if (this.datatype === 'http://www.w3.org/2000/01/rdf-schema#Resource') {
      obj = sparqlEscapeUri(this.object);
    } else {
      obj = `${sparqlEscapeString(this.object)}`;
      if (this.datatype)
        obj += `^^${sparqlEscapeUri(this.datatype)}`;
    }

    return `${sparqlEscapeUri(this.subject)} ${predicate} ${obj} .`;
  }
}
