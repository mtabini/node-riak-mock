var Riak = require('../index');
var RiakClient = require('riak-js');
var request = require('request');

var server = new Riak(7172);
var client;

var expect = require('chai').expect;
var async = require('async');

describe('The link manipulation functionality', function() {
   
    before(function (done) {
        server.start(function() {
            client = RiakClient.getClient({
                port: 7172
            });
            
            done();
        });
    });
    
    it('should allow setting and retrieving links', function(done) {
        var payload = { name : 'Marco' , email : 'marcot@tabini.ca' };
        var links = [{ bucket : 'friends' , key : '123' , tag : 'friend' } , { bucket : 'friends' , key : '124' , tag : 'friend' }];
        var bucket = 'people';
        var key = 'randomkey124';
    
        async.waterfall(
            [
        
                function initialSave(callback) {
                    client.save(bucket, key, payload, { links : links }, callback);
                },
            
                function retrieve(doc, meta, callback) {
                    client.get(bucket, key, callback);
                },
            
                function checkMeta(doc, meta, callback) {
                    expect(doc).to.be.an('object');
                    expect(doc).to.deep.equal(payload);
                
                    expect(meta).to.be.an('object');
                    
                    expect(meta.links).to.be.an('array');
                    expect(meta.links).to.deep.equal(links);
                
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