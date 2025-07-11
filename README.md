# harvesting-extract-to-ttl-service
Microservice that extracts rdfa knowledge from html documents and stores the information in turtle files for later processing.
This service is meant to be used in combination with a [job-controller](https://github.com/lblod/job-controller-service). It requires in input container with html files to process.

## Installation

To add the service to your stack, add the following snippet to docker-compose.yml:

```yml
services:
  harvesting-extract:
    image: lblod/harvesting-extract-to-ttl:x.x.x
    volumes:
      - ./data/files:/share
```

### Job controller config
```js
{
  "http://lblod.data.gift/id/jobs/concept/JobOperation/lblodHarvesting": {
    "tasksConfiguration": [
      {
        "currentOperation": null,
        "nextOperation": "http://lblod.data.gift/id/jobs/concept/TaskOperation/extracting",
        "nextIndex": "0"
      },
      {
        "currentOperation": "http://lblod.data.gift/id/jobs/concept/TaskOperation/collecting",
        "nextOperation": "http://lblod.data.gift/id/jobs/concept/TaskOperation/extracting",
        "nextIndex": "1"
      },
      // ...
  }
```

### Delta

```
  {
    match: {
      predicate: {
        type: 'uri',
        value: 'http://www.w3.org/ns/adms#status'
      },
      object: {
        type: 'uri',
        value: 'http://redpencil.data.gift/id/concept/JobStatus/scheduled'
      }
    },
    callback: {
      method: 'POST',
      url: 'http://harvesting-extract/delta'
    },
    options: {
      resourceFormat: 'v0.0.1',
      gracePeriod: 1000,
      ignoreFromSelf: true
    }
  },
```

This service will filter out  <http://redpencil.data.gift/vocabularies/tasks/Task> with operation <http://lblod.data.gift/id/jobs/concept/TaskOperation/extracting>.
NOTE: For historic reasons, this service will also respond to jobs with operation <http://lblod.data.gift/id/jobs/concept/TaskOperation/importing>

### Environment variables
 - WRITE_DEBUG_TTLS: (default: `true`) whether to also write original, corrected and invalid triples to files

## Validation and correction
The service will validate the triples to import and will try its best to correct the ones that it founds invalid. Valid, invalid and corrected triples are written to a file.

## REST API

### POST /delta

Starts the import of the given harvesting-tasks into the db

- Returns `204 NO-CONTENT` if no harvesting-tasks could be extracted.

- Returns `200 SUCCESS` if the harvesting-tasks where successfully processes.

- Returns `500 INTERNAL SERVER ERROR` if something unexpected went wrong while processing the harvesting-tasks.


## Model
See [lblod/job-controller-service](https://github.com/lblod/job-controller-service)
