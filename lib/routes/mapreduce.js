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
        
        inputs.forEach(function(input) {
            result = result.concat(MapReduce.processInput(input));
        });
        
        return result;
    }
    
    server.server.post('/mapred', function(req, res, next) {
        if (typeof req.body !== 'object') {
            return next (new restify.BadRequestError('Invalid payload'));
        }
        
        var inputs = parseInputs(req.body.inputs);
        var query = new MapReduce(req.body.query);

        res.send(query.execute(inputs));
                
        return next();
    });
    
}

module.exports = links;
