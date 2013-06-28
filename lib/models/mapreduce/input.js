var Bucket = require('../bucket');
var restify = require('restify');

var filterKey = require('./filter');

var Input = function RiakMapReduceInput(value, keyData) {
    this.value = value;
    this.keyData = keyData;
};

var processInput = function processInput(spec) {
    switch (spec.constructor.name) {
        case 'Array':
            
            // Individual bucket/key coordinate
            
            if (spec.length < 2 || spec.length > 3) {
                throw new restify.BadRequestError('Bucket/key inputs must have at least two and no more than three values');
            }
            
            var bucket = Bucket.get(spec[0]);
            
            return new Input(bucket.getObject(spec[1]), spec.length === 3 ? spec[3] : null);
            
        case 'String':
            
            // Whole bucket
            
            var bucket = Bucket.get(spec);
            
            var result = [];
            
            bucket.allObjects().forEach(function(object) {
                result.push(new Input(object, null));
            });
            
            return result;
            
        case 'Object':
            
            // 2i searches
            
            if (spec.index) {
                if ((!spec.bucket) ||
                    (spec.start && !spec.end) ||
                    (spec.end && !spec.start) ||
                    (!spec.start && !spec.end && !spec.key)) {
                        throw new restify.BadRequestError('Invalid 2i query');
                    }
                    
                var keys;
                var bucket = Bucket.get(spec.bucket);
                    
                if (spec.start) {
                    keys = bucket.rangeQuery(spec.index, spec.start, spec.end);
                } else {
                    keys = bucket.query(spec.index, spec.key);
                }
                
                return keys.map(function(key) {
                    return new Input(bucket.getObject(key));
                });
            }
            
            // Key filters
            
            if (!spec.bucket || !spec.key_filters) {
                throw new restify.BadRequestError('Key filter inputs must have both a bucket and key_filter item');
            }
            
            var bucket = Bucket.get(spec.bucket);
            var objects = bucket.allObjects();
            
            var result = [];
            
            objects.forEach(function(object) {
                if (filterKey(object.key, spec.key_filters)) {
                    result.push(new Input(object, null));
                }
            });
            
            return result;
            
        default:
            
            throw new restify.BadRequestError('Invalid input');
    }
    
    return result;
}

processInput.RiakInput = Input;

module.exports = processInput;