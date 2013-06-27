var restify = require('restify');
var levehnstein = require('../../util/levehnstein');

function keyFilterFromSpec(spec) {
    return '[' + spec.join(', ') + ']';
}

function validateSpec(spec, min, max) {
    max = max || min;
    
    if (spec.length < min || spec.length > max) {
        throw new restify.BadRequestError('The key filter ' + keyFilterFromSpec(spec) + ' is invalid');
    }
}

var transformFunctions = {
    int_to_string: function intToString(value, spec) {
        return String(value);
    },
    
    string_to_int: function stringToInt(value, spec) {
        return parseInt(value);
    },
    
    string_to_float: function stringToFloat(value, spec) {
        return parseFloat(value);
    },
    
    to_upper: function toUpper(value, spec) {
        return String(value).toUpperCase();
    },
    
    to_lower: function toLower(value, spec) {
        return String(value).toLowerCase();
    },
    
    tokenize: function tokenize(value, spec) {
        validateSpec(spec, 3);
        
        var components = value.split(spec[1]);
        
        return (spec[2] < components.length ? components[spec[2]] : null);
    },
    
    urldecode: function urldecode(value, spec) {
        return decodeURIComponent(value.replace(/\+/g, ' '));
    },
    
    greater_than: function greaterThan(value, spec) {
        validateSpec(spec, 2);
        
        return value > spec[1];
    },
    
    less_than: function lessThan(value, spec) {
        validateSpec(spec, 2);
        
        return value < spec[1];
    },
    
    greater_than_eq: function greaterThanEq(value, spec) {
        validateSpec(spec, 2);
        
        return value >= spec[1];
    },
    
    less_than_eq: function lessThanEq(value, spec) {
        validateSpec(spec, 2);
        
        return value <= spec[1];
    },
    
    between: function between(value, spec) {
        validateSpec(spec, 3, 4);

        var inclusive = (spec.length < 4) ? true : spec[3];
        
        if (inclusive) {
            return (value >= spec[1] && value <= spec[2]);
        } else {
            return (value > spec[1] && value < spec[2]);
        }
    },
    
    matches: function matches(value, spec) {
        validateSpec(spec, 2);
        
        return String(value).match(new RegExp(spec[1]));
    },
    
    neq: function neq(value, spec) {
        validateSpec(spec, 2);
        
        return value != spec[1];
    },
    
    eq: function eq(value, spec) {
        validateSpec(spec, 2);
        
        return value == spec[1];
    },
    
    set_member: function setMember(value, spec) {
        validateSpec(spec, 2, Number.MAX_VALUE);
    
        var set = spec.slice(1);
        
        return (set.indexOf(value) !== -1);
    },
    
    similar_to: function similarTo(value, spec) {
        validateSpec(spec, 3);
        
        return (levehnstein(value, spec[1]) <= spec[2]);
    },
    
    starts_with: function startsWith(value, spec) {
        validateSpec(spec, 2);
        
        return String(value).indexOf(spec[1]) == 0;
    },
    
    ends_with: function endsWith(value, spec) {
        validateSpec(spec, 2);
        
        return String(value).indexOf(spec[1]) == String(value).length - String(spec[1]).length;
    },
    
    and: function and(value, spec) {
        validateSpec(spec, 3);
        
        return evaluate(value, spec[1]) && transformFunctions.evaluate(value, spec[2]);
    },
    
    or: function or(value, spec) {
        validateSpec(spec, 3);
        
        return evaluate(value, spec[1]) && transformFunctions.evaluate(value, spec[2]);
    },
    
    not: function not(value, spec) {
        validateSpec(spec, 2);
        
        return evaluate(value, spec[1]);
    }
};

function evaluate(key, spec) {
    if (typeof spec !== 'array') {
        throw new restify.BadRequestError('Invalid key filter ' + JSON.stringify(spec));
    }
    
    var result = null;
    
    spec.forEach(function(command) {
        var f = transformFunctions[command[0]];
        
        if (!f) {
            throw new restify.BadRequestError('Invalid key filter command ' + command[0]);
        }
        
        result = f(key, spec);
    });
    
    return result;
}

module.exports = evaluate;