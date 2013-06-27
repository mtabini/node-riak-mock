var Link = require('../link');

var LinkPhase = function LinkPhase(spec) {
    this.spec = {
        bucket: spec.bucket || '_',
        tag: spec.tag || '_',
        keep: '1'
    };
}

LinkPhase.prototype.execute = function execute(objects) {
    var results = [];
    
    objects = objects.map(function(object) {
        return {
            parent: null,
            obj: object.object
        };
    });
    
    var sections = Link.walk(objects, [this.spec]);
    
    sections.forEach(function(section) {
        section.forEach(function(object) {
            results.push({
                object: object.obj,
                auxData: null
            });
        });
    });
    
    return results;
};

module.exports = LinkPhase;