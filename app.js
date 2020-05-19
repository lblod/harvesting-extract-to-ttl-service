import {app, errorHandler} from 'mu';

import flatten from 'lodash.flatten';
import {importHarvestingTask} from "./lib/harvesting-task";

const TASK_READY = 'http://lblod.data.gift/harvesting-statuses/ready-for-importing';
app.get('/', function (req, res) {
  res.send('Hello harvesting-import-service');
});

app.post('/delta', async function (req, res) {

  const tasks = getTasks(req.delta);
  if (!tasks.length) {
    console.log("Delta does not contain new harvesting tasks  with status 'ready-for-importing'. Nothing should happen.");
    return res.status(204).send();
  }

  // TODO update state
  for (let task of tasks) {
    try {
      await importHarvestingTask(task);
    } catch (e) {
      // TODO update task state
      console.error(e);
    }
  }
  // TODO update task state
});


// TODO doc
function getTasks(delta) {
  const inserts = flatten(delta.map(changeSet => changeSet.inserts));
  return inserts.filter(isTriggerTriple).map(t => t.subject.value);
}

// TODO doc
function isTriggerTriple(triple) {
  return triple.predicate.value === 'http://www.w3.org/ns/adms#status'
    && triple.object.value === TASK_READY;
}

app.use(errorHandler);