var restify = require('restify');
var crypto = require('crypto');

var RiakMock = function(port, host) {
    this.host = host;
    this.port = port;
    this.server = restify.createServer({
        name: 'Riak',
        version: '1.4.0',
        acceptable: [ '*/*' ],
    });
    
    this.server.formatters['multipart/mixed'] =require('./lib/util/multipart');

    this.server.use(restify.conditionalRequest());
    this.server.use(restify.queryParser({ mapParams: false }));
    this.server.use(restify.bodyParser({ mapParams: false }));

    require('./lib/routes/bucket')(this);
    require('./lib/routes/object')(this);
    require('./lib/routes/query')(this);
    require('./lib/routes/link')(this);
    require('./lib/routes/mapreduce')(this);
};

RiakMock.prototype.start = function(callback) {
    this.server.listen(this.port, this.host, callback);
}

RiakMock.prototype.stop = function(callback) {
    this.server.close(callback);
}

module.exports = RiakMock;