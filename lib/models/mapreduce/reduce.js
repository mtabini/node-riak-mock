var vm = require('vm');
var restify = require('restify');

var ReducePhase = function ReducePhase(spec) {
    if (spec.language !== 'javascript') {
        throw new restify.BadRequestError('Riak-mock only supports JavaScript map/reduce jobs');
    }
    
    if (spec.source) {
        this.script = vm.createScript('var f = ' + spec.source + '; var result = f(objects, arg)');
    } else if (spec.name) {
        this.script = vm.createScript('var f = ' + spec.name + '; var result = f(objects, arg)');
    } else {
        throw new restify.BadRequestError('The reduce phase ' + JSON.stringify(spec) + ' has no script name or source.');
    }
    
    this.arg = spec.arg;
};

ReducePhase.prototype.execute = function execute(objects) {
    var sandbox = {
        arg: this.arg,
        objects: objects.map(function(object) {
            if (object.constructor.name === 'RiakMapReduceInput') {
                return object.value;
            } else {
                return object;
            }
        }),
        Riak: require('./riak_builtin_functions')
    };
    
    this.script.runInNewContext(sandbox);
        
    return sandbox.result;
};

module.exports = ReducePhase;