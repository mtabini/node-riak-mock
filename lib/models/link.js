var Bucket = require('./bucket');

var Link = function RiakLink(bucket, key, tag) {
    this.bucket = bucket;
    this.key = key;
    this.tag = tag;
};

function walk(objs, spec, walked) {
    var result = [];
    walked = walked || [];
    
    objs.forEach(function(obj) {
        Object.keys(obj.links).forEach(function(tag) {
            if (spec.tag === '_' || spec.tag === tag) {
                obj.links[tag].forEach(function(link) {
                    if (spec.bucket === '_' || link.bucket === spec.bucket) {
                        var bucket = Bucket.get(link.bucket);
                        var obj = bucket.getObject(link.key);
                        
                        if (obj && result.indexOf(obj) === -1 && walked.indexOf(obj) == -1) {
                            result.push(obj);
                        }
                    }
                });
            }
        });
    });
    
    if (result.length) {
        result = result.concat(walk(result, spec, walked.concat(result)));
    }
    
    return result;
}

Link.walk = function(objs, specs) {
    var result = [];
    var objs;
    
    specs.forEach(function(spec) {
        objs = walk(objs, spec);
        
        if (spec.keep === '1') {
            result.push(objs);
        }
    });
    
    return result;
};

module.exports = Link;