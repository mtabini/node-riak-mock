var restify = require('restify');
var Bucket = require('../models/bucket');

function buckets(server) {
    
    server.server.get('/riak', function listBuckets(req, res, next) {
        if (req.query.buckets === 'true') {
            return res.send({ buckets : Bucket.keys() });
        }
        
        return next(new restify.BadRequestError('Invalid request'));
    });
    
    server.server.get('/riak/:bucket/keys?keys=true', function listKeys(req, res, next) {
        var bucket = Bucket.get(req.params.bucket);
        
        if (!req.params.bucket) {
            return next(new restify.BadRequestError('Missing bucket name'));
        }

        return bucket.objectKeys();
    });
    
}

module.exports = buckets;