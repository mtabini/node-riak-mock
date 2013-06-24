var Riak = require('../index');
var RiakClient = require('riak-js');
var request = require('request');

var server = new Riak(7172);
var client;

var expect = require('chai').expect;
var async = require('async');

describe('The object manipulation functionality', function() {
   
    before(function (done) {
        server.start(function() {
            client = RiakClient.getClient({
                port: 7172
            });
            
            done();
        });
    });
    
    it('should return an error when requesting a non-existing object', function(done) {
        client.get('non', 'existing', function(err, document, meta) {
            expect(err).to.be.an('object');
            expect(err.statusCode).to.equal(404);
            
            done();
        });
    });
    
    it('should allow saving a new object', function(done) {
        var payload = { name : 'Marco' , email : 'marcot@tabini.ca' };
        
        client.save('people', null, payload, { returnbody : true }, function(err, doc) {
            expect(err).to.be.null;
            expect(doc).to.deep.equal(payload);
            
            done();
        });
    });
    
    it('should allow saving an object with an explicit key', function(done) {
        var payload = { name : 'Marco' , email : 'marcot@tabini.ca' };
        var key = 'randomkey123';
        
        client.save('people', key, payload, { returnbody : true }, function(err, doc) {
            expect(err).to.be.null;
            expect(doc).to.deep.equal(payload);
            
            done();
        });
    });
    
    it('should allow retrieving an existing object', function(done) {
        var payload = { name : 'Marco' , email : 'marcot@tabini.ca' };
        var bucket = 'people';
        var key = 'randomkey124';
        
        client.save(bucket, key, payload, { returnbody : true }, function(err, doc) {
            expect(err).to.be.null;
            expect(doc).to.deep.equal(payload);
            
            client.get(bucket, key, function(err, document, meta) {
                expect(err).to.be.null;
                expect(document).to.deep.equal(payload);
                
                done();
            });
        });
    });
    
    it('should allow deleting an existing object', function(done) {
        var payload = { name : 'Marco' , email : 'marcot@tabini.ca' };
        var bucket = 'people';
        var key = 'randomkey125';
        
        client.save(bucket, key, payload, { returnbody : true }, function(err, doc) {
            expect(err).to.be.null;
            expect(doc).to.deep.equal(payload);
            
            client.remove(bucket, key, function(err) {
                expect(err).to.be.null;
                
                done();
            });
        });
    });
    
    it('should disallow deleting a non-existing object', function(done) {
        var bucket = 'people';
        var key = 'randomkey126';
        
        client.remove(bucket, key, function(err) {
            expect(err).to.be.an('object');
            expect(err.statusCode).to.equal(404);
            
            done();
        });
    });
    
    it('should allow setting and retrieving meta tags', function(done) {
        var payload = { name : 'Marco' , email : 'marcot@tabini.ca' };
        var bucket = 'people';
        var key = 'randomkey124';
        
        async.waterfall(
            [
            
                function initialSave(callback) {
                    client.save(bucket, key, payload, { returnbody : true , headers : { 'x-riak-meta-test' : '123' } }, callback);
                },
                
                function retrieve(doc, meta, callback) {
                    client.get(bucket, key, callback);
                },
                
                function checkMeta(doc, meta, callback) {
                    expect(doc).to.be.an('object');
                    expect(doc).to.deep.equal(payload);
                    
                    expect(meta).to.be.an('object');
                    expect(meta._headers['x-riak-meta-test']).to.equal('123');
                    
                    callback();
                }
            
            ],
            
            function(err) {
                expect(err).to.be.undefined;
                
                done();
            }
        );
    });
                
    after(function (done) {
        server.stop(done);
    });
    
});