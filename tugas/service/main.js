const orm = require('./lib/orm');
const storage = require('./lib/storage');
const kv = require('./lib/kv');
const bus = require('./lib/bus');
const { TaskSchema } = require('./tasks/task.model');
const { WorkerSchema } = require('./worker/worker.model');
const workerServer = require('./worker/server');
const tasksServer = require('./tasks/server');
const performanceServer = require('./performance/server');
const { config } = require('./config');
const { LoggerAction } = require('./lib/logger')
const { TracerAction } = require('./lib/tracker')

async function init(ctx) {
  try {
    ctx.logger.info('connect to database');
    await orm.connect([WorkerSchema, TaskSchema], config.database);
    ctx.logger.info('database connected');
  } catch (err) {
    ctx.logger.error('database connection failed');
    process.exit(1);
  }
  try {
    ctx.logger.info('connect to object storage');
    await storage.connect('task-manager', config.minio);
    ctx.logger.info('object storage connected');
  } catch (err) {
    ctx.logger.error('object storage connection failed');
    process.exit(1);
  }
  try {
    ctx.logger.info('connect to message bus');
    await bus.connect();
    ctx.logger.info('message bus connected');
  } catch (err) {
    ctx.logger.error('message bus connection failed');
    process.exit(1);
  }
  try {
    ctx.logger.info('connect to key value store');
    await kv.connect();
    ctx.logger.info('key value store connected');
  } catch (err) {
    ctx.logger.error('key value store connection failed');
    process.exit(1);
  }
}

async function onStop() {
  bus.close();
  kv.close();
}

async function main(command) {
  let logger;
  let tracker;
  let ctx;
  switch (command) {
    case 'performance':
      logger = LoggerAction('info','performance-service')
      tracker = TracerAction('performance-service');
      ctx = {
        logger,
        tracer,
      };
      await init(ctx);
      performanceServer.run(ctx, onStop);
      break;
    case 'task':
      logger = LoggerAction('info','task-service')
      tracker = TracerAction('task-service');
      ctx = {
        logger,
        tracer,
      };
      await init(ctx);
      tasksServer.run(onStop);
      break;
    case 'worker':
      logger = LoggerAction('info','worker-service')
      tracker = TracerAction('worker-service');
      ctx = {
        logger,
        tracer,
      };
      await init(ctx);
      workerServer.run(onStop);
      break;
    default:
      command = (typeof(command) == 'undefined' || typeof(command) == 'null') ? 'command':command;
      logger.info(`${command} tidak dikenali`);
      logger.info('command yang valid: task, worker, performance');
  }
}

main(process.argv[2]);
