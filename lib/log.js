var debug = require('debug');

var supportedLevel = ['trace', 'debug', 'info', 'warn', 'error'];

function log(nameSpace) {
  var baseNameSpace = 'agenda:' + nameSpace;
  var loggerFactory = {};

  supportedLevel.forEach(function(level) {
    loggerFactory[level] = debug(baseNameSpace + ':' + level);
  });

  return loggerFactory;
}

module.exports = log;
