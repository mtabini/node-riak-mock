# Riak-mock: A pretend Riak server for your testing pleasure

Riak-mock is a Node.js module that mimics a real-life [Riak](http://basho.com/riak/) server cluster that can be initialized, started, and stopped at will. Its goal is to provide developers with a reasonable approximation of the real thing to facilitate testing operations without having to depend on the availability of a real cluster.

## Usage as a module

```javascript
var Riak = require('../index');

var server = new Riak(8087);

server.start();
```

This will start a new server on port 8087 and attach it to all available IPs. The full signature of the server constructor is:

```javascript
new Riak(port, [hostname], [log]);
```

Here, `log` is a [bunyan](https://github.com/trentm/node-bunyan) logging facility that is optionally passed to Riak-mock's underlying [restify](https://github.com/mcavage/node-restify) object.

You can `start([callback])` and `stop([callback])` the server as needed. Every time a new server is created, it starts with a blank data store that you can populate as needed.

## Usage as a server

You can also run Riak-mock as a standalone server; this can be handy when you're trying to track down a particular issue and don't want to create a full environment to deal with the problem. You can do so by installing the module standalone in a location of your choice, and then running `npm start`, which executes the `server.js` file. The server includes a built-in logging facility that dumps to stdout.

## Limitations

- It should go without saying—but I'll say it anyway—that Riak-mock is _not_ intended to be a replacement for Riak. Not only does it not support any of the features that make Riak great, like for clustering, load balancing, fault tolerance, and so on; it is also not optimized for standalone, single-server use _at all_. Its goal is simply to approximate a running instance of Riak to make writing unit tests easier.
- There are likely to be many subtle differences between the way Riak and Mock-Riak work. The long-term goal is to track those down and fix them, but _caveat emptor._
- Only JavaScript map/reduce jobs are supported.
- Overall, this project is very young; if your Riak usage goes beyond the basics, you're likely to encounter problems (in which case, bug reports—and, especially, patches—are welcome).

## Submitting patches and helping out

You are encouraged to submit patches for the bugs you find. However, you _must_ also provide at least one unit test to cover the patch (unless, of course, the patch is either not code-related or is, itself, a unit test). I will not merge pull requests without covering unit tests, and it's a fair bet that I won't have the time to write the tests myself.

Also, if you want to help out, the project needs much better test coverage, especially when it comes to map/reduce operations, which are a little sketchy at the moment.