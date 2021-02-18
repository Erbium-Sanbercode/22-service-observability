const { createLogger, format, transports } = require('winston');

const myFormat = format.printf(({ level, message, timestamp }) => {
  return `${timestamp} ${level}: ${message}`;
});

function LoggerAction (level, serviceName) {
  const logger = createLogger({
    level: level || 'info',
    format: format.combine(format.timestamp(), myFormat),
    defaultMeta: { service: serviceName },
    transports: [
      new transports.File({
        filename: `log/${serviceName}_error.log`,
        level: 'error',
      }),
      new transports.Console({
        format: format.combine(format.timestamp(), myFormat),
      }),
    ],
  });

  return logger;
}

module.exports = {
    LoggerAction,
}
