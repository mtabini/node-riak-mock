var RiakObject = require('./object');

var Bucket = function Bucket(bucketName) {
    this.bucketName = bucketName;
    this.objects = {};
};

var buckets = {};

Bucket.get = function getBucket(bucketName) {
    if (!bucketName) {
        return null;
    }
    
    var result = buckets[bucketName];
    
    if (!result) {
        result = buckets[bucketName] = new Bucket(bucketName);
    }
    
    return result;
};

Bucket.keys = function getBucketKeys() {
    return Object.keys(buckets);
}

Bucket.prototype.getObject = function getBucketObject(key) {
    return this.objects[key];
};

Bucket.prototype.setObject = function setBucketObject(key, meta, contents, links, indices) {
    var object = RiakObject.set(this, key, meta, contents, links, indices);
    
    return this.objects[object.key] = object;
};

Bucket.prototype.deleteObject = function deleteBucketObject(key) {
    var obj = this.getObject(key);
    
    delete this.objects[key];
    
    return obj;
};

Bucket.prototype.objectKeys = function getBucketObjectKeys() {
    return Object.keys(this.objects);
};

Bucket.prototype.query = function queryBucketObjects(index, value) {
    var results = [];
    
    Object.keys(this.objects).forEach(function(key) {
        var obj = this.objects[key];
        
        if (obj.indices[index] == value) {
            results.push(key);
        }
        
    }, this);
    
    return results;
}

Bucket.prototype.rangeQuery = function queryBucketObjects(index, start, end) {
    var results = [];
    
    Object.keys(this.objects).forEach(function(key) {
        var obj = this.objects[key];
        var indexValue = parseInt(obj.indices[index]);
        
        if (indexValue >= start && indexValue <= end) {
            results.push(key);
        }
        
    }, this);
    
    return results;
}

module.exports = Bucket;