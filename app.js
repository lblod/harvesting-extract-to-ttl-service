import {app, errorHandler} from 'mu';

import flatten from 'lodash.flatten';
import bodyParser from 'body-parser';

import {
  importHarvestingTask,
  TASK_ONGOING,
  TASK_READY,
  updateTaskStatus
} from "./lib/harvesting-task";

app.use(bodyParser.json({
  type: function (req) {
    return /^application\/json/.test(req.get('content-type'));
  }
}));

app.get('/', function (req, res) {
  res.send('Hello harvesting-import-service');
});

app.post('/delta', async function (req, res, next) {

  const tasks = getTasks(req.body);
  if (!tasks.length) {
    console.log("Delta does not contain new harvesting tasks  with status 'ready-for-importing'. Nothing should happen.");
    return res.status(204).send();
  }
  try {
    console.log(`Successfully started import for harvesting tasks ${tasks.join(`, `)}`);
    for (let task of tasks) {

      await updateTaskStatus(task, TASK_ONGOING);
      await importHarvestingTask(task); // async processing of import
    }
    return res.status(200).send().end();
  } catch (e) {
    console.log(`Something went wrong while handling deltas for harvesting tasks ${tasks.join(`, `)}`);
    console.log(e);
    return next(e);
  }
});

// async function importHarvestingTask
/**
 * Returns the inserted ready-for-import harvesting task URIs
 * from the delta message. An empty array if there are none.
 *
 * @param delta body as received from the delta notifier
 */
function getTasks(delta) {
  const inserts = flatten(delta.map(changeSet => changeSet.inserts));
  return inserts.filter(isTriggerTriple).map(t => t.subject.value);
}

/**
 * Returns whether the passed triple is a trigger for an import process
 *
 * @param triple as received from the delta notifier
 */
function isTriggerTriple(triple) {
  return triple.predicate.value === 'http://www.w3.org/ns/adms#status'
    && triple.object.value === TASK_READY;
}

app.use(errorHandler);