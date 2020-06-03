import {app, errorHandler} from 'mu';

import bodyParser from 'body-parser';

import {
  importHarvestingTask,
  TASK_FAILURE,
  TASK_ONGOING,
  TASK_READY, TASK_SUCCESS,
  updateTaskStatus
} from "./lib/harvesting-task";
import {Delta} from "./lib/delta";

app.use(bodyParser.json({
  type: function (req) {
    return /^application\/json/.test(req.get('content-type'));
  }
}));

app.get('/', function (req, res) {
  res.send('Hello harvesting-import-service');
});

app.post('/delta', async function (req, res, next) {
  try {
    const tasks = new Delta(req.body).getInsertsFor('http://www.w3.org/ns/adms#status', TASK_READY);
    if (!tasks.length) {
      console.log('Delta dit not contain harvesting-tasks that are ready for import, awaiting the next batch!');
      return res.status(204).send();
    }
    console.log(`Starting import for harvesting-tasks: ${tasks.join(`, `)}`);
    for (let task of tasks) {
      try {
        await updateTaskStatus(task, TASK_ONGOING);
        await importHarvestingTask(task);
        await updateTaskStatus(task, TASK_SUCCESS);
      }catch (e){
        console.log(`Something unexpected went wrong while handling delta harvesting-task <${task}>`);
        console.error(e);
        try {
          await updateTaskStatus(task, TASK_FAILURE);
        } catch (e) {
          console.log(`Failed to update state of task <${task}> to failure state. Is the connection to the database broken?`);
          console.error(e);
        }
      }
    }
    return res.status(200).send().end();
  } catch (e) {
    console.log(`Something unexpected went wrong while handling delta harvesting-tasks!`);
    console.error(e);
    return next(e);
  }
});

app.use(errorHandler);