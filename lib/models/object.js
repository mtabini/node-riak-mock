var crypto = require('crypto');

var RiakObject = function(bucket, key, meta, contentType, contents, links, indices) {
    this.bucket = bucket;
    this.key = key || RiakObject.generateKey();
    this.meta = meta || {};
    this.contentType = contentType;
    this.contents = contents || '';
    this.links = links || {};
    this.indices = indices || {};
    this.etag = '';
    this.lastModifiedDate = new Date();
};

RiakObject.generateKey = function generateObjectKey() {
    var md5 = crypto.createHash('md5');
    md5.update(String((new Date()).getTime()) + String(Math.random() * 1000000));
    md5 = md5.digest('hex');
    
    return md5;
};

RiakObject.set = function setObject(bucket, key, meta, contentType, contents, links, indices) {
    var obj = bucket.getObject(key);
    
    if (!obj) {
        obj = new RiakObject(bucket, key, meta, contentType, contents, links, indices);
    } else {
        function overwriteData(source, dest) {
            Object.keys(source || {}).forEach(function(key) {
                dest[key] = source[key];
            });
        }
        
        overwriteData(meta, obj.meta);
        overwriteData(links, obj.links);
        overwriteData(indices, obj.indices);
        
        obj.contents = contents;
    }
    
    var md5 = crypto.createHash('md5');
    
    md5.update(JSON.stringify(obj.meta) + JSON.stringify(obj.contents));
    
    obj.etag = md5.digest('hex');
    obj.lastModifiedDate = new Date();
    
    return obj;
};

module.exports = RiakObject;