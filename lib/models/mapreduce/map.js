var restify = require('restify');
var vm = require('vm');

var MapPhase = function MapPhase(spec) {
    if (spec.language !== 'javascript') {
        throw new restify.BadRequestError('Riak-mock only supports JavaScript map/reduce jobs');
    }
    
    this.script = vm.createScript('var f = ' + spec.source + '; var result = f(object.object, object.auxData, arg)');
    this.arg = spec.arg;
};

function objectMetadata(obj) {
    var result = {};
    
    Object.keys(obj.meta).forEach(function(key) {
        result[key] = obj.meta[key];
    });

    Object.keys(obj.indices).forEach(function(key) {
        result['X-Riak-Index-' + key] = obj.indices[key];
    });
    
    var linkHeaders = [];
    
    Object.keys(obj.links).forEach(function(tag) {
        obj.links[tag].forEach(function(link) {
            linkHeaders.push[link.bucket, link.key, link.tag];
        });
    });
    
    if (linkHeaders.length) {
        result['Links'] = linksHeaders;
    }
    
    result['Content-type'] = obj.contentType;
    result['etag'] = obj.etag;
    result['Last-Modified-Date'] = obj.lastModifiedDate.toUTCString();
    result['X-Riak-Last-Modified'] = obj.lastModifiedDate.toUTCString();
    
    return result;
}

MapPhase.prototype.execute = function execute(objects) {
    var results = [];
    
    objects.forEach(function(object) {
        var sandbox = {
            arg: this.arg,
            object: {
                object: {
                    bucket: object.object.bucket.bucketName,
                    key: object.object.key,
                    vclock: Math.random(100000),
                    values: [
                        {
                            metadata: objectMetadata(object.object),
                            data: object.object.contents
                        }
                    ]
                },
                auxData: object.auxData
            }
        };
        
        this.script.runInNewContext(sandbox);
        
        results.push({
            object: sandbox.result,
            auxData: null
        });
    }, this);
    
    return results;
};

module.exports = MapPhase;