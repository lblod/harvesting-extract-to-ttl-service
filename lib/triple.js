import { sparqlEscapeUri, sparqlEscapeString } from 'mu';

export default class Triple {
  constructor({ subject, predicate, object, datatype }, parentUrl) {
    this.subject = subject;
    this.predicate = predicate;
    this.object = object;
    this.datatype = datatype;
    this.parentUrl = parentUrl;
  }

  isEqual(other) {
    return this.subject === other.subject
      && this.predicate === other.predicate
      && this.object === other.object
      && this.datatype === other.datatype;
  }

  toNT() {
    const predicate = this.predicate === 'a' ? this.predicate : this.fixRelativeURI(this.predicate);
    let obj;
    if (this.datatype === 'http://www.w3.org/2000/01/rdf-schema#Resource') {
      obj = this.fixRelativeURI(this.object);
    } else {
      obj = `${sparqlEscapeString(this.object)}`;
      if (this.datatype)
        obj += `^^${sparqlEscapeUri(this.datatype)}`;
    }

    return `${this.fixRelativeURI(this.subject)} ${predicate} ${obj} .`;
  }
  fixRelativeURI(uri) {
    if(uri.startsWith('#') || uri.startsWith('/')) {
      return sparqlEscapeUri(this.parentUrl + uri);
    } else {
      return sparqlEscapeUri(uri);
    }
  }
}
