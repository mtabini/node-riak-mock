var Riak = require('../index');
var RiakClient = require('riak-js');
var request = require('request');

var server = new Riak(7172);
var client;

var expect = require('chai').expect;
var async = require('async');

describe('The map/reduce functionality', function() {
   
    before(function (done) {
        server.start(function() {
            client = RiakClient.getClient({
                port: 7172
            });
            
            done();
        });
    });
    
    it('should allow running complex jobs', function(done) {
        var payloads = [ 
            {
                data : { 
                    name : 'Marco' , 
                    gender : 'male'
                } , 
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
                        tag : 'child'
                    }]
            },
            { data : { name : 'Daniel' , email : 'daniel@example.org' , gender : 'male' } , links : [ { bucket : 'users' , key : 'andrea' , tag : 'sibling' } ] , key : 'daniel' }, 
            { data : { name : 'Andrea' , email : 'andrea@example.org' , gender : 'female' } , key : 'andrea' } ];
    
        var bucket = 'users';

        async.each(
            payloads,
    
            function iterator(element, callback) {
                client.save(bucket, element.key, element.data, element.links ? { links : element.links } : null, callback);
            },
    
            function(err) {
                client.mapreduce
                    .add([[bucket, 'marco']])
                    .link({})
                    .map(function(v, args, arg) {
                        var data = JSON.parse(v.values[0].data);
                        var key = data.gender + arg.b;
                        var result = {};
                
                        result[key] = 1;
                
                        return result;
                    }, { b : '-g' })
                    .reduce(function(values, count, arg) {
                        var result = {};
                
                        values.forEach(function(values) {
                            Object.keys(values).forEach(function(value) {
                                if (value in result) {
                                    result[value] += values[value];
                                } else {
                                    result[value] = values[value];
                                }
                            });
                        });
                
                        return [ result ];
                    }, { c : '-d' })
                    .run(function(err, result) {
                        var expectedResult = [
                            {
                                'male-g' : 1,
                                'female-g' : 3
                            }
                        ];
                        
                        expect(err).to.be.null;
                        
                        expect(result).to.be.an('array');
                        expect(result).to.deep.equal(expectedResult);
                        
                        done();
                    });
            }
        );        
    });
    
    it('should support built-in named functions', function(done) {
        var payloads = [ 
            {
                data : { 
                    name : 'Marco' , 
                    gender : 'male'
                } , 
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
                        tag : 'child'
                    }]
            },
            { data : { name : 'Daniel' , email : 'daniel@example.org' , gender : 'male' } , links : [ { bucket : 'users' , key : 'andrea' , tag : 'sibling' } ] , key : 'daniel' }, 
            { data : { name : 'Andrea' , email : 'andrea@example.org' , gender : 'female' } , key : 'andrea' } ];
    
        var bucket = 'users';

        async.each(
            payloads,
    
            function iterator(element, callback) {
                client.save(bucket, element.key, element.data, element.links ? { links : element.links } : null, callback);
            },
    
            function(err) {
                client.mapreduce
                    .add([[bucket, 'marco']])
                    .map('Riak.mapValuesJson')
                    .run(function(err, result) {
                        expect(err).to.be.null;
                        
                        expect(result).to.be.an('array');
                        expect(result).to.have.length(1);
                        
                        expect(result).to.deep.equal([ { name: 'Marco', gender: 'male' } ]);
                        
                        done();
                    });
            }
        );        
    });
              
    after(function (done) {
        server.stop(done);
    });
    
});