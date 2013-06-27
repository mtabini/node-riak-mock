var MapPhase = require('./map');
var ReducePhase = require('./reduce');
var LinkPhase = require('./link');

var restify = require('restify');

var MapReduce = function MapReduce(spec) {
    if (spec.constructor.name !== 'Array') {
        throw new restify.BadRequestError('The phases of a map/reduce request must be an array');
    }
    
    this.phases = spec.map(function(spec) {
        var result;
        
        if (spec.link) {
            result = new LinkPhase(spec.link);
        } else if (spec.map) {
            result = new MapPhase(spec.map);
        } else if (spec.reduce) {
            result = new ReducePhase(spec.reduce);
        } else {
            throw new restify.BadRequestError('Unknown phase ' + JSON.stringify(spec));
        }
        
        result.keep = spec.keep || false;
        
        return result;
    });
    
    this.phases[this.phases.length - 1].keep = true;
};

MapReduce.processInput = require('./input');

MapReduce.prototype.execute = function(objects) {
    var finalResult = [];
    
    this.phases.forEach(function(phase) {
        objects = phase.execute(objects);
        
        if (phase.keep) {
            finalResult = finalResult.concat(objects);
        }
    });
    
    return finalResult;
}

module.exports = MapReduce;