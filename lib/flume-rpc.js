var util = require('util'),
    os = require('os'),
    thrift = require('thrift'),
    flume = require('./gen-nodejs/ThriftSourceProtocol'),
    FlumeProcessor = flume.Processor,
    FlumeClient = flume.Client,
    ttypes = require('./gen-nodejs/flume_types'),
    EventEmitter = require("events").EventEmitter;

//console.log(util.inspect(ttypes));

exports.Priority = ttypes.Priority;
exports.EventStatus = ttypes.EventStatus;

// Depending upon npm version, it seems that one or the other is required
var ttransport = require('thrift/lib/nodejs/lib/thrift/transport');
var tprotocol = require('thrift/lib/nodejs/lib/thrift/protocol');

var Sink = exports.Sink = function Sink() {
    var self = this;
    EventEmitter.call(this);

    var handler = {
        append: function (event) {
            self.emit("message", event);
        },
        close: function (callWhenFinished) {
            self.emit("rpcClose", callWhenFinished);
        }
    };

    this.server = thrift.createServer(FlumeProcessor, handler,
                                      { transport: ttransport.TBufferedTransport });

    // Forward other events...
    this.server.on('listening', function onListening() { self.emit('listening'); });
    this.server.on('connection', function onConnection(socket) { self.emit('connection', socket); });
    this.server.on('close', function onClose() { self.emit('close'); });
    this.server.on('error', function onErr(err) { self.emit('error', err); });
};

util.inherits(Sink, EventEmitter);

Sink.prototype.listen = function listen(hostname, port, cb) {
    this.server.listen(hostname, port, cb);
};

Sink.prototype.close = function close(hostname, port) {
    this.server.close();
};

var Source = exports.Source = function Source(hostname, port, options) {
    var self = this;
    EventEmitter.call(this);

    options = options || {};

    var transport = ttransport.TFramedTransport;

    if (options.hasOwnProperty('transport')) {
        switch (options.transport){
            case 'buffered': transport = ttransport.TBufferedTransport; break;
            case 'framed': transport = ttransport.TFramedTransport; break;
        }
    }

    var protocol = tprotocol.TCompactProtocol;

    if (options.hasOwnProperty('protocol')) {
        switch (options.protocol){
            case 'compact': protocol = ttransport.TCompactProtocol; break;
        }
    }

    this.conn = thrift.createConnection(hostname, port, {
                                            transport: transport,
                                            protocol: protocol
                                        });
    this.client = thrift.createClient(FlumeClient, this.conn);

    this.conn.on('error', function onError(e) { self.emit('error', e); });
    this.conn.on('close', function onClose() { self.emit('close'); });
    this.conn.on('timeout', function onTimeout() { self.emit('timeout'); });
    this.conn.on('connect', function onConnect() { self.emit('connect'); });

    this.hostname = os.hostname();
};

util.inherits(Source, EventEmitter);

Source.prototype.log = function log(headers, body, cb) {
    var toSend = new ttypes.ThriftFlumeEvent({
        headers: headers || {},
        body: body
    });

    this.client.append(toSend, cb);
};

Source.prototype.close = function close()
{
    this.conn.end();
};