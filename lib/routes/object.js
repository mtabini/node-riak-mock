var Bucket = require('../models/bucket');
var Link = require('../models/link');

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
        
        var linksHeaders = [];
        
        Object.keys(obj.links).forEach(function(tag) {
            obj.links[tag].forEach(function(link) {
                linksHeaders.push('</riak/' + link.bucket + '/' + link.key + '>; riaktag="' + link.tag + '"');
            });
        });
        
        if (linksHeaders.length) {
            res.header('Link', linksHeaders.join(', '));
        }
        
        res.header('content-type', obj.contentType);
        res.header('etag', obj.etag);
        res.header('Last-Modified-Date', obj.lastModifiedDate.toUTCString());
        
        var md5 = crypto.createHash('md5');
        md5.update((new Date()).toGMTString());
        
        res.header('X-Riak-Vclock', md5.digest('hex'));
        
        res.send(obj.formattedValue());
        res.end();
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
        var links = {};

        Object.keys(req.headers).forEach(function(key) {
            if (key.match(/^x-riak-meta-/)) {
                meta[key] = req.headers[key];
            }
            
            if (key.match(/^x-riak-index-/)) {
                indices[key.replace(/^x-riak-index-/, '')] = req.headers[key];
            }
            
            if (key.match('link')) {
                var success = req.headers[key].split(',').every(function(linkSpec) {
                    var matches = linkSpec.match(/<\/riak\/([^/]+)\/([^>]+)>\s*;\s*riaktag="([^"]+)"/);
                    
                    if (!matches) {
                        return false;
                    }
                    
                    var linksForTag = links[matches[3]];
                    
                    if (!linksForTag) {
                        linksForTag = [];
                        links[matches[3]] = linksForTag;
                    }
                    
                    linksForTag.push(new Link(matches[1], matches[2], matches[3]));
                    
                    return true;
                });
                
                if (!success) {
                    return next(new restify.BadRequestError('Invalid link format in headers.'));
                }
            }
        });
        
        var obj = coords.bucket.setObject(coords.key, meta, req.contentType(), contents, links, indices);
        
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