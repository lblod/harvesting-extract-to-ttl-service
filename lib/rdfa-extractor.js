import streamToArray from "stream-to-array";
import { jsdom } from "jsdom";
import getRDFaGraph from "@lblod/graph-rdfa-processor";
import { createUnzip } from "zlib";
import rdfSerializer from "rdf-serialize";
import { createReadStream } from "fs";
import { DataFactory, Store, StreamParser } from "n3";
const { namedNode, quad } = DataFactory;
import { pipeline as pipelineAsync } from "stream/promises";
import { Readable, Writable } from "stream";
import streamToString from "stream-to-string";
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
          namedNode("http://www.w3.org/ns/prov#wasDerivedFrom"),
          this.url,
        ),
      );
    }
    return next();
  }
}
export default class RDFAextractor {
  async extractPage(filePath, fileMetadata) {
    const store = new Store();

    const writer = new SourceAwareStoreWriter(fileMetadata.url, store);
    try {
      let html;
      if (filePath.endsWith(".gz")) {
        html = await streamToString(
          createReadStream(filePath).pipe(createUnzip()),
        );
      } else {
        html = await streamToString(createReadStream(filePath));
      }
      let { document } = jsdom(html).defaultView.window;
      let graph = getRDFaGraph(document, {
        baseURI: fileMetadata.url,
        specialHtmlPredicates: [
          {
            source: "http://www.w3.org/ns/prov#value",
            target:
              "http://lblod.data.gift/vocabularies/besluit/extractedDecisionContent",
          },
        ],
      });
      let stream = Readable.from([graph.toString()]);
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
      return null;
    } else {
      const turtleStream = rdfSerializer.serialize(store.match(), {
        contentType: "application/n-triples",
      });
      const turtle = await streamToArray(turtleStream);
      return turtle.map((s) => s.trim()).filter((s) => s.length);
    }
  }
}
