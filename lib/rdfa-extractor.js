import streamToArray from "stream-to-array";
import { JSDOM } from "jsdom";
import getRDFaGraph from "@lblod/graph-rdfa-processor";
import { createUnzip } from "zlib";
import rdfSerializer from "rdf-serialize";
import { createReadStream } from "fs";
import { DataFactory, Store, StreamParser } from "n3";
const { namedNode, quad } = DataFactory;
import { pipeline as pipelineAsync } from "stream/promises";
import { Readable, Writable } from "stream";
import streamToString from "stream-to-string";
import { uuid } from "mu";
import { writeFileToTriplestore } from "./file-helpers";
const EXTRACTED_DECISION_CONTENT =
  "http://lblod.data.gift/vocabularies/besluit/extractedDecisionContent";
class SourceAwareStoreWriter extends Writable {
  constructor(task, url, store) {
    super({ objectMode: true });
    this.subjects = [];
    this.store = store;
    this.url = namedNode(url);
    this.task = task;
  }

  async _write(data, _enc, next) {
    if (
      data.predicate.value === EXTRACTED_DECISION_CONTENT &&
      data.object.datatype.value ===
      "http://www.w3.org/1999/02/22-rdf-syntax-ns#HTML"
    ) {
      const htmlUri = await writeFileToTriplestore(
        this.task.graph,
        data.object.value,
        "content.html",
        this.url,
        "text/html",
        "html",
      );
      this.store.addQuad(
        quad(data.subject, data.predicate, namedNode(htmlUri)),
      );
    } else {
      this.store.addQuad(data);
    }
    if (!this.subjects.includes(data.subject.value)) {
      this.subjects.push(data.subject.value);
      this.store.addQuad(
        quad(
          data.subject,
          namedNode("http://www.w3.org/ns/prov#wasDerivedFrom"),
          this.url,
        ),
      );
    }
    return next();
  }
}
export default class RDFAextractor {
  async extractPage(task, filePath, fileMetadata) {
    const store = new Store();

    const writer = new SourceAwareStoreWriter(task, fileMetadata.url, store);
    try {
      let html;
      if (filePath.endsWith(".gz")) {
        html = await streamToString(
          createReadStream(filePath).pipe(createUnzip()),
        );
      } else {
        html = await streamToString(createReadStream(filePath));
      }
      let window = new JSDOM(html).window;
      let document = window.document;
      html = null;
      let graph = getRDFaGraph(document, {
        baseURI: fileMetadata.url,
        specialHtmlPredicates: [
          {
            source: "http://www.w3.org/ns/prov#value",
            target: EXTRACTED_DECISION_CONTENT,
          },
        ],
      });

      let stream = Readable.from([graph.toString()]);
      // fix all the mess produced by jsdom
      // this is important to reduce memory leaks
      document?.close();
      window.close();

      // end optimization

      await pipelineAsync(stream, new StreamParser(), writer);

      return store;
    } catch (e) {
      console.error(`ERROR extracting file with path ${filePath}`, e);
      throw e;
    }
  }

  async ttl(store) {
    if (store.size === 0) {
      console.log("No triples found. Did you extract RDFa already?");
      return [];
    } else {
      const turtleStream = rdfSerializer.serialize(store.match(), {
        contentType: "application/n-triples",
      });
      const turtle = await streamToArray(turtleStream);
      return turtle.map((s) => s.trim()).filter((s) => s.length);
    }
  }
}
