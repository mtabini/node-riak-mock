var crypto = require('crypto');

function createBoundary() {
    var hash = crypto.createHash('sha256');
    hash.update((new Date()).toGMTString() + String(Math.random() * 10000000));
    return hash.digest('hex');
}

function header(key, value) {
    return (key + ': ' + value || '') + '\r\n';
}

function renderObject(obj) {
    var result = '';
    
    Object.keys(obj.meta).forEach(function(key) {
        result += header(key, obj.meta[key]);
    });

    Object.keys(obj.indices).forEach(function(key) {
        result += header('X-Riak-Index-' + key, obj.indices[key]);
    });
    
    var linksHeaders = [];
    
    Object.keys(obj.links).forEach(function(tag) {
        obj.links[tag].forEach(function(link) {
            linksHeaders.push('</riak/' + link.bucket + '/' + link.key + '>; riaktag="' + link.tag + '"');
        });
    });
    
    if (linksHeaders.length) {
        result += header('Link', linksHeaders.join(', '));
    }

    result += header('Content-type', obj.contentType);
    
    result += header('etag', obj.etag);
    result += header('Last-Modified-Date', obj.lastModifiedDate.toUTCString());
    
    var md5 = crypto.createHash('md5');
    md5.update((new Date()).toGMTString());
    
    result += header('X-Riak-Vclock', md5.digest('hex'));
    
    result += '\r\n' + (typeof obj.contents === 'object' ? JSON.stringify(obj.contents) : obj.contents);
    
    return result;
}

function multipart(data, res) {
    var mainBoundary = createBoundary();
    
    var parts = [];
    
    data.forEach(function(part) {
        var internalBoundary = createBoundary();
        
        var result = [];
        
        part.forEach(function(obj) {
            result.push(renderObject(obj));
        });
        
        result = result.join('\r\n--' + internalBoundary + '\r\n');

        var output = header('Content-type', 'multipart/mixed; boundary=' + internalBoundary);
        output += '\r\n';
        
        output += '--' + internalBoundary + '\r\n' + result + '\r\n--' + internalBoundary +'--\r\n';
        
        parts.push(output);
    });
    
    res.setHeader('Content-type', 'multipart/mixed; boundary=' + mainBoundary);
    
    res.write('--' + mainBoundary + '\r\n' + parts.join('\r\n--' + mainBoundary + '\r\n') + '\r\n--' + mainBoundary + '--');
    res.end();
}

module.exports = multipart;