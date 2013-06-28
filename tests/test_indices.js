var Riak = require('../index');
var RiakClient = require('riak-js');
var request = require('request');

var server = new Riak(7172);
var client;

var expect = require('chai').expect;
var async = require('async');

describe('The secondary index facility', function() {
   
    before(function (done) {
        server.start(function() {
            client = RiakClient.getClient({
                port: 7172
            });
            
            done();
        });
    });
    
    it('should allow setting and retrieving secondary indices', function(done) {
        var payload = { name : 'Marco' , email : 'marcot@tabini.ca' };
        var bucket = 'people1';
        var key = 'randomkey124';
        
        async.waterfall(
            [
            
                function initialSave(callback) {
                    client.save(bucket, key, payload, { index : { index1 : 'a' , index2 : 'b' } }, callback);
                },
                
                function retrieve(doc, meta, callback) {
                    client.get(bucket, key, callback);
                },
                
                function checkMeta(doc, meta, callback) {
                    doc = JSON.parse(doc);
                    
                    expect(doc).to.be.an('object');
                    expect(doc).to.deep.equal(payload);
                    
                    expect(meta).to.be.an('object');
                    expect(meta._headers['x-riak-index-index1_bin']).to.equal('a');
                    expect(meta._headers['x-riak-index-index2_bin']).to.equal('b');
                    
                    callback();
                }
            
            ],
            
            function(err) {
                expect(err).to.be.undefined;
                
                done();
            }
        );
    });
                
    it('should allow searching by secondary indices', function(done) {
        var payloads = [ { name : 'Marco' , email : 'marcot@tabini.ca' } , { name : 'Daniel' , email : 'daniel@example.org' } , { name : 'Andrea' , email : 'andrea@example.org' } ];
        var bucket = 'things';
        
        async.each(
            payloads,
            
            function iterator(element, callback) {
                client.save(bucket, null, element, { index : { email_index : element.email } }, callback);
            },
            
            function(err) {
                expect(err).to.be.null;
                
                client.query(bucket, { email_index : 'daniel@example.org' }, function(err, list) {
                    expect(err).to.be.null;
                    
                    expect(list).to.be.an('array');
                    expect(list).to.have.length(1);
                    
                    client.get(bucket, list[0], function(err, doc, meta) {
                        expect(err).to.be.null;
                        
                        doc = JSON.parse(doc);
                    
                        expect(doc).to.be.an('object');
                        expect(doc).to.deep.equal(payloads[1]);
                    
                        done();
                    });
                });
            }
        );
    });
    
    it('should allow range matching by secondary indices', function(done) {
        var payloads = [ { name : 'Eggs' , price : 300 } , { name : 'bacon' , price : 350 } , { name : 'sausage' , price : 299 } ];
        var bucket = 'things1';
        
        async.each(
            payloads,
            
            function iterator(element, callback) {
                client.save(bucket, null, element, { index : { price : element.price } }, callback);
            },
            
            function(err) {
                expect(err).to.be.null;
                
                client.query(bucket, { price : [ 300 , 350 ] }, function(err, list) {
                    expect(err).to.be.null;
                    
                    expect(list).to.be.an('array');
                    expect(list).to.have.length(2);
                    
                    async.map(
                        list,
                        
                        function mapper(element, callback) {
                            client.get(bucket, element, callback);
                        },
                        
                        function finalCallback(err, result) {
                            expect(err).to.be.null,
                            
                            expect(result).to.be.an('array');
                            expect(result).to.have.length(2);
                            
                            result = result.map(JSON.parse);
                            
                            expect(result[0]).to.deep.equal(payloads[0]);
                            expect(result[1]).to.deep.equal(payloads[1]);
                            
                            done();
                        }
                    );
                });
            }
        );
    });
                
    after(function (done) {
        server.stop(done);
    });
    
});