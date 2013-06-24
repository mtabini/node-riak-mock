var Riak = require('../index');
var RiakClient = require('riak-js');
var request = require('request');

var server = new Riak(7172);
var client;

var expect = require('chai').expect;

describe('The bucket lister', function() {
   
    before(function (done) {
        server.start(function() {
            client = RiakClient.getClient({
                port: 7172
            });
            
            done();
        });
    });
    
    it('should return a list of buckets', function(done) {
        request.get('http://localhost:7172/riak?buckets=true', function(err, res, body) {
            var response = JSON.parse(body);
            
            expect(response).to.be.an('object');
            expect(response).to.have.key('buckets');
            expect(response.buckets).to.be.an('array');
            
            done();
        });
    });
    
    after(function (done) {
        server.stop(done);
    });
    
});