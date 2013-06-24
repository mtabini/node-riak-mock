var restify = require('restify');
var Bucket = require('../models/bucket');

function objects(server) {
    
    function getBucketAndKey(req, ignoreKey) {
        var bucketName = req.params.bucket;
        var keyName = req.params.key;
        
        if (bucketName === undefined) {
            return { err : new restify.BadRequestError('Missing bucket name') };
        }
        
        if (!ignoreKey && keyName === undefined) {
            return { err : new restify.BadRequestError('Missing key') };
        }

        return {
            bucket: Bucket.get(bucketName),
            key: keyName
        };
    }
    
    server.server.get('/riak/:bucket/:key', function(req, res, next) {
        var coords = getBucketAndKey(req);
        
        if (coords.err) {
            return next(coords.err);
        }
        
        var object = coords.bucket.getObject(coords.key);
        
        if (!object) {
            return next(new restify.NotFoundError('Key not found'));
        }
        
        res.send(object.contents);
        return next();
    });
    
    server.server.post('/riak/:bucket/', function(req, res, next) {
        var coords = getBucketAndKey(req, true);
        
        if (coords.err) {
            return next(coords.err);
        }
        
        var contents = req.body;
        
        if (!contents) {
            res.send(204, 'No content');
            return next();
        }
        
        var obj = coords.bucket.setObject(coords.key, {}, contents, {}, {});
        
        res.header('Location', '/riak/' + coords.bucket + '/' + obj.key);
        
        if (req.query['returnbody'] === 'true') {
            res.send(obj.contents);
        } else {
            res.statusCode = 201;
            res.end();
        }
        
        return next();
    });
    
    server.server.put('/riak/:bucket/:key', function(req, res, next) {
        var coords = getBucketAndKey(req);
        
        if (coords.err) {
            return next(coords.err);
        }
        
        var contents = req.body;
        
        if (!contents) {
            res.send(204, 'No content');
            return next();
        }
        
        var obj = coords.bucket.setObject(coords.key, {}, contents, {}, {});
        
        res.header('Location', '/riak/' + coords.bucket + '/' + obj.key);
        
        if (req.query['returnbody'] === 'true') {
            res.send(obj.contents);
        } else {
            res.statusCode = 201;
        }
        
        return next();
    });

    server.server.del('/riak/:bucket/:key', function(req, res, next) {
        var coords = getBucketAndKey(req);
        
        if (coords.err) {
            return next(coords.err);
        }
        
        var obj = coords.bucket.deleteObject(coords.key);
        
        if (!obj) {
            return next(new restify.NotFoundError('Key not found'));
        } else {
            res.end();
        }
        
        return next();
    });
    
};

module.exports = objects;