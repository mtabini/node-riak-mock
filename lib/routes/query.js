var Bucket = require('../models/bucket');

var restify = require('restify');
var crypto = require('crypto');

function queries(server) {
    
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
    
    server.server.get('/buckets/:bucket/index/:index/:value', function(req, res, next) {
        var coords = getBucketAndKey(req, true);
        
        if (coords.err) {
            return next(coords.err);
        }
        
        var index = req.params.index.trim();
        
        if (index.length === 0) {
            return next(new restify.BadRequestError('Missing index'));
        }
        
        var query = req.params.value.trim();
        
        if (query.length === 0) {
            return next(new restify.BadRequestError('Missing search value'));
        }
        
        res.send({ keys : coords.bucket.query(index, query) });
        
        next();
    });
    
    server.server.get('/buckets/:bucket/index/:index/:start/:end', function(req, res, next) {
        var coords = getBucketAndKey(req, true);
        
        if (coords.err) {
            return next(coords.err);
        }
        
        var index = req.params.index.trim();
        
        if (index.length === 0) {
            return next(new restify.BadRequestError('Missing index'));
        }
        
        var start = parseInt(req.params.start);
        
        if (isNaN(start)) {
            return next(new restify.BadRequestError('Invalid range start'));
        }
        
        var end = parseInt(req.params.end);
        
        if (isNaN(end)) {
            return next(new restify.BadRequestError('Invalid range start'));
        }
        
        res.send({ keys : coords.bucket.rangeQuery(index, start, end) });
        
        next();
    });
    
};

module.exports = queries;