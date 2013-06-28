var Bucket = require('../models/bucket');
var Link = require('../models/link');
var MapReduce = require('../models/mapreduce');

var restify = require('restify');

function links(server) {
    
    function parseInputs(inputs) {
        if (inputs.constructor.name !== 'Array') {
            inputs = [inputs];
        }
        
        var result = [];
        
        try {
            inputs.forEach(function(input) {
                result = result.concat(MapReduce.processInput(input));
            });
        } catch (e) {
            return e;
        }
        
        return result;
    }
    
    server.server.post('/mapred', function(req, res, next) {
        if (typeof req.body !== 'object') {
            return next (new restify.BadRequestError('Invalid payload'));
        }
        
        try {
            var inputs = parseInputs(req.body.inputs);
            var query = new MapReduce(req.body.query);

            res.send(query.execute(inputs));
        } catch (e) {
            return next(e);
        }
                
        return next();
    });
    
        // res.setHeader('content-type', 'multipart/mixed');
        // res.send(result);
    
}

module.exports = links;
