var restify = require('restify');
var vm = require('vm');

var MapPhase = function MapPhase(spec) {
    if (spec.language !== 'javascript') {
        throw new restify.BadRequestError('Riak-mock only supports JavaScript map/reduce jobs');
    }
    
    if (spec.source) {
        this.script = vm.createScript('var f = ' + spec.source + '; var result = f(value, keyData, arg)');
    } else if (spec.name) {
        this.script = vm.createScript('var f = ' + spec.name + '; var result = f(value, keyData, arg)');
    } else {
        throw new restify.BadRequestError('The map phase ' + JSON.stringify(spec) + ' has no script name or source.');
    }
    
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
        var keyData, value;
        
        if (object.constructor.name === 'RiakMapReduceInput') {
            keyData = object.keyData;
            value = {
                bucket: object.value.bucket.bucketName,
                key: object.value.key,
                vclock: Math.random(100000),
                values: [
                    {
                        metadata: objectMetadata(object.value),
                        data: object.value.contents
                    }
                ]
            };
        } else {
            value = object;
        }
        
        var sandbox = {
            arg: this.arg,
            value: value,
            keyData: keyData,
            console: console,
            Riak: require('./riak_builtin_functions')
        };
        
        this.script.runInNewContext(sandbox);
        
        results = results.concat(sandbox.result);
    }, this);
    
    return results;
};

module.exports = MapPhase;