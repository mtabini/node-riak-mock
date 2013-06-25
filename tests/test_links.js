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
    
    it('should allow link walking', function(done) {
        var payloads = [ 
            {
                data : { name : 'Marco' , email : 'marcot@tabini.ca' } , 
                key : 'marco' , 
                links : [ 
                    { 
                        bucket : 'users' , 
                        key : 'daniel' ,
                        tag : 'friend'
                    },
                    {
                        bucket : 'users',
                        key: 'andrea',
                        tag : 'sibling'
                    }]
            },
            { data : { name : 'Daniel' , email : 'daniel@example.org' } , key : 'daniel' }, 
            { data : { name : 'Andrea' , email : 'andrea@example.org' } , key : 'andrea' } ];
            
        var bucket = 'users';
        
        async.each(
            payloads,
            
            function iterator(element, callback) {
                client.save(bucket, element.key, element.data, element.links ? { links : element.links } : null, callback);
            },
            
            function(err) {
                client.walk(
                    bucket,
                    
                    'marco',
                    
                    [
                        {
                            bucket: bucket,
                            tag: '_'
                        }
                    ],
                    
                    function(err, result) {
                        expect(err).to.be.null;
                        
                        expect(result).to.be.an('array');
                        expect(result).to.have.length(1);
                        
                        result = result[0];
                        
                        expect(result).to.be.an('array');
                        expect(result).to.have.length(2);
                        
                        result = result.map(function(obj) { return obj.data });
                        
                        expect(result[0]).to.deep.equal(payloads[1].data);
                        expect(result[1]).to.deep.equal(payloads[2].data);
                        
                        done();
                    }
                );
            }
        );

    });
                
    after(function (done) {
        server.stop(done);
    });
    
});