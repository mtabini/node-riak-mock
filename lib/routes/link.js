var Bucket = require('../models/bucket');
var Link = require('../models/link');

var restify = require('restify');
var crypto = require('crypto');

var multipart = require('../util/multipart');

function links(server) {
    
    server.server.get(/^\/riak\/([^\/]+)\/([^\/]+)\/(([^,]+)\,([^,]+)\,([a-zA-Z0-9_\.~-]+)\/?)+/, function(req, res, next) {
        var url = req.url.split('/');
        
        var bucket = Bucket.get(url[2]);
        var obj = bucket.getObject(url[3]);
        
        if (!obj) {
            return next(new restify.NotFoundError('Object not found'));
        }
        
        var specs = [];
        var spec;
        
        for (var index = 4; index < url.length; index++) {
            spec = url[index].split(',').map(Function.prototype.call, String.prototype.trim);
        
            spec = {
                bucket : spec[0],
                tag: spec[1],
                keep: spec[2]
            };
            
            spec.keep = spec.keep === '_' ? ((index < url.length - 1) ? '0' : '1') : spec.keep;
            
            specs.push(spec);
        }
        
        var result = Link.walk([obj], specs);
        
        multipart(result, res);

        return next();
    });
    
}

module.exports = links;
