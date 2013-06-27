var Bucket = require('../bucket');
var restify = require('restify');

var filterKey = require('./filter');

var processInput = function processInput(spec) {
    switch (spec.constructor.name) {
        case 'Array':
            
            // Individual bucket/key coordinate
            
            if (spec.length < 2 || spec.length > 3) {
                throw new restify.BadRequestError('Bucket/key inputs must have at least two and no more than three values');
            }
            
            var bucket = Bucket.get(spec[0]);
            
            return [{
                object: bucket.getObject(spec[1]),
                auxData: spec.length === 3 ? spec[3] : null
            }];
            
        case 'String':
            
            // Whole bucket
            
            var bucket = Bucket.get(spec);
            
            var result = [];
            
            bucket.allObjects().forEach(function(object) {
                result.push({
                    object: object,
                    auxData: null
                });
            });
            
            return result;
            
        case 'Object':
            
            // Key filters
            
            if (!spec.bucket || !spec.key_filters) {
                throw new restify.BadRequestError('Key filter inputs must have both a bucket and key_filter item');
            }
            
            var bucket = Bucket.get(spec.bucket);
            var objects = bucket.allObjects();
            
            var result = [];
            
            objects.forEach(function(object) {
                if (filterKey(object.key, spec.key_filters)) {
                    result.push({
                        object: object,
                        auxData: null
                    });
                }
            });
            
            return result;
            
        default:
            
            throw new restify.BadRequestError('Invalid input');
    }
    
    return result;
}

module.exports = processInput;