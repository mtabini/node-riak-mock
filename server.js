var RiakMock = require('./index');
var bunyan = require('bunyan');
var restify = require('restify');

var log = bunyan.createLogger({
    name: 'myapp',
    level: 'info'
});

var server = new RiakMock(8087, null, log);

server.start(function() {
    log.info('Server listening.');
    server.server.on('after', restify.auditLogger({ log: log }));
});

server.server.on('uncaughtException', function(req, res, route, error) {
    log(error.trace);
    res.send(error);
});