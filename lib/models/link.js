var Link = function RiakLink(bucket, key, tag) {
    this.bucket = bucket;
    this.key = key;
    this.tag = tag;
};

module.exports = Link;