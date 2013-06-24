var Bucket = require('../models/bucket');

var restify = require('restify');
var crypto = require('crypto');

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
    
    function sendObject(obj, res) {
        Object.keys(obj.meta).forEach(function(key) {
            res.header(key, obj.meta[key]);
        });
    
        Object.keys(obj.indices).forEach(function(key) {
            res.header('X-Riak-Index-' + key, obj.indices[key]);
        });
        
        res.header('etag', obj.etag);
        res.header('Last-Modified-Date', obj.lastModifiedDate.toUTCString());
        
        var md5 = crypto.createHash('md5');
        md5.update((new Date()).toGMTString());
        
        res.header('X-Riak-Vclock', md5.digest('hex'));
        
        res.send(obj.contents);
    }
    
    function performObjectSet(req, res, next) {
        var coords = getBucketAndKey(req, true);
        
        if (coords.err) {
            return next(coords.err);
        }
        
        var contents = req.body;
        
        if (!contents) {
            res.send(204, 'No content');
            return next();
        }

        var meta = {};
        var indices = {};
        
        Object.keys(req.headers).forEach(function(key) {
            if (key.match(/^x-riak-meta-/)) {
                meta[key] = req.headers[key];
            }
            
            if (key.match(/^x-riak-index-/)) {
                indices[key.replace(/^x-riak-index-/, '')] = req.headers[key];
            }
            
            if (key.match('link')) {
                //TODO Link
            }
        });
        
        var obj = coords.bucket.setObject(coords.key, meta, contents, {}, indices);
        
        res.header('Location', '/riak/' + coords.bucket.bucketName + '/' + obj.key);
        
        if (req.query['returnbody'] === 'true') {
            sendObject(obj, res);
        } else {
            res.statusCode = 201;
            res.end();
        }
        
        return next();
    }
    
    server.server.post('/riak/:bucket/', performObjectSet);
    server.server.put('/riak/:bucket/:key', performObjectSet);

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
    
    server.server.get('/riak/:bucket/:key', function(req, res, next) {
        var coords = getBucketAndKey(req);
        
        if (coords.err) {
            return next(coords.err);
        }
        
        var object = coords.bucket.getObject(coords.key);
        
        if (!object) {
            return next(new restify.NotFoundError('Key not found'));
        }
        
        sendObject(object, res);
        return next();
    });
    
};

module.exports = objects;