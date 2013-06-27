var vm = require('vm');

var ReducePhase = function ReducePhase(spec) {
    if (spec.language !== 'javascript') {
        throw new restify.BadRequestError('Riak-mock only supports JavaScript map/reduce jobs');
    }
    
    this.script = vm.createScript('var f = ' + spec.source + '; var result = f(objects, arg)');
    this.arg = spec.arg;
};

ReducePhase.prototype.execute = function execute(objects) {
    var sandbox = {
        arg: this.arg,
        objects: objects.map(function(object) {
            return object.object;
        }),
    };
    
    this.script.runInNewContext(sandbox);
        
    return sandbox.result;
};

module.exports = ReducePhase;