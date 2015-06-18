var urlParse = require('url').parse;
var util = require('util');
var redis = require('redis');
var bufferEqual = require('buffer-equal');

module.exports = function(options, Source) {
    if (!Source) throw new Error('No source provided');
    if (!Source.prototype.get) throw new Error('No get method found on source');

    function Caching() { return Source.apply(this, arguments) };

    // Inheritance.
    util.inherits(Caching, Source);

    // References for testing, convenience, post-call overriding.
    Caching.redis = options;

    Caching.prototype.get = module.exports.cachingGet('TL', options, Source.prototype.get);

    return Caching;
};

module.exports.cachingGet = function(namespace, options, get) {
    if (!get) throw new Error('No get function provided');
    if (!namespace) throw new Error('No namespace provided');

    options = options || {};
    if (options.client) {
        options.client.options.return_buffers = true;
    } else {
        options.client = redis.createClient({return_buffers: true});
    }
    options.expires = ('expires' in options) ? options.expires : 300;
    options.mode = ('mode' in options) ? options.mode : 'readthrough';

    if (!options.client) throw new Error('No redis client');
    if (!options.expires) throw new Error('No expires option set');

    var caching;
    if (options.mode === 'readthrough') {
        caching = readthrough;
    } else if (options.mode === 'race') {
        caching = race;
    } else {
        throw new Error('Invalid value for options.mode ' + options.mode);
    }

    function race(url, callback) {
        var key = namespace + '-' + url;
        var source = this;
        var client = options.client;
        var expires;
        if (typeof options.expires === 'number') {
            expires = options.expires;
        } else {
            expires = options.expires[urlParse(url).hostname] || options.expires.default || 300;
        }
        var sent = false;
        var cached = null;
        var current = null;

        // GET upstream.
        get.call(source, url, function(err, buffer, headers) {
            current = encode(err, buffer);
            if (cached && current) finalize();
            if (sent) return;
            sent = true;
            callback(err, buffer, headers);
        });

        // GET redis.
        // Allow command_queue_high_water to act as throttle to prevent
        // back pressure from what may be an ailing redis-server
        if (client.command_queue.length < client.command_queue_high_water) {
            client.get(key, function(err, encoded) {
                // If error on redis, do not flip first flag.
                // Finalize will never occur (no cache set).
                if (err) return (err.key = key) && client.emit('error', err);

                cached = encoded || '500';
                if (cached && current) finalize();
                if (sent || !encoded) return;
                var data;
                try {
                    data = decode(cached);
                } catch(err) {
                    (err.key = key) && client.emit('error', err);
                    cached = '500';
                }
                if (data) {
                    sent = true;
                    if (data instanceof Error) return callback(data);
                    else return callback(null, data);
                }
            });
        } else {
            client.emit('error', new Error('Redis command queue at high water mark'));
        }

        function finalize() {
            if (bufferEqual(cached, current)) return;
            client.setex(key, expires, current, function(err) {
                if (!err) return;
                err.key = key;
                client.emit('error', err);
            });
        }
    };

    function readthrough(url, callback) {
        var key = namespace + '-' + url;
        var source = this;
        var client = options.client;
        var expires;
        if (typeof options.expires === 'number') {
            expires = options.expires;
        } else {
            expires = options.expires[urlParse(url).hostname] || options.expires.default || 300;
        }

        if (client.command_queue.length < client.command_queue_high_water) {
            client.get(key, function(err, encoded) {
                // If error on redis get, pass through to original source
                // without attempting a set after retrieval.
                if (err) {
                    err.key = key;
                    client.emit('error', err);
                    return get(url, callback);
                }

                // Cache hit.
                var data;
                if (encoded) try {
                    data = decode(encoded);
                } catch(err) {
                    err.key = key;
                    client.emit('error', err);
                }
                if (data && (data instanceof Error))
                    return callback(data);
                else if (data)
                    return callback(null, data);

                // Cache miss, error, or otherwise no data
                get.call(source, url, function(err, buffer, headers) {
                    if (err && !errcode(err)) return callback(err);
                    callback(err, buffer, headers);
                    // Callback does not need to wait for redis set to occur.
                    client.setex(key, expires, encode(err, buffer), function(err) {
                        if (!err) return;
                        err.key = key;
                        client.emit('error', err);
                    });
                });
            });
        } else {
            client.emit('error', new Error('Redis command queue at high water mark'));
            return get.call(source, url, callback);
        }
    };

    return caching;
};

module.exports.redis = redis;
module.exports.encode = encode;
module.exports.decode = decode;

function errcode(err) {
    if (!err) return;
    if (err.status === 404) return 404;
    if (err.status === 403) return 403;
    if (err.code === 404) return 404;
    if (err.code === 403) return 403;
    return;
}

function encode(err, buffer) {
    if (errcode(err)) return 'E' + errcode(err).toString();

    // Unhandled error.
    if (err) return null;

    // Turn objects into JSON string buffers.
    if (buffer && typeof buffer === 'object' && !(buffer instanceof Buffer)) {
        buffer = new Buffer(JSON.stringify(buffer));
        buffer = Buffer.concat([new Buffer('O'), buffer], buffer.length + 1);
    // Turn strings into buffers.
    } else if (buffer && !(buffer instanceof Buffer)) {
        buffer = new Buffer(buffer);
        buffer = Buffer.concat([new Buffer('S'), buffer], buffer.length + 1);
    } else {
    // Consider it binary
        buffer = Buffer.concat([new Buffer('B'), buffer], buffer.length + 1);
    }
    return buffer;
};

function decode(encoded) {
    var types = ['O','S','B'];
    var hint = encoded.slice(0, 1).toString();
    var buffer = encoded.slice(1);
    if (hint === 'E' && (buffer.toString() === '404' || buffer.toString() === '403')) {
        var err = new Error();
        err.code = parseInt(buffer, 10);
        err.status = parseInt(buffer, 10);
        err.redis = true;
        return err;
    }

    if (types.indexOf(hint) < 0) return new Error('Invalid cache value');

    if (hint === 'O') buffer = JSON.parse(buffer.toString());
    else if (hint === 'S') buffer = buffer.toString();
    return buffer;
};
