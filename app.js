import {app, errorHandler} from 'mu';

app.get('/', function (req, res) {
  res.send('Hello harvesting-import-service');
});

app.use(errorHandler);