'use strict';
var EventEmitter = require('events');
var crypto = require('crypto');
var Socket = require('net').Socket;
var inherits = require('util').inherits;
var Stream = require('stream').Stream;
var Readable = require('stream').Readable;
var OSUsername = require('os').userInfo().username;

var Protocol = require('./Protocol');

var readableConfig = { objectMode: true, highWaterMark: 1 };
var terminateReq = {};
var defaultConfig = {
  host: 'localhost',
  port: 5432,
  user: OSUsername,
  keepalive: true
};

module.exports = Client;

function Client(cfg) {
  EventEmitter.call(this);
  this.connected = false;
  this.status = null;
  this.backendParams = null;
  this.key = null;
  this._queue = new List();
  this._cur = null;
  this._s = null;
  this._p = null;

  if (typeof cfg !== 'object' || cfg === null) {
    this._cfg = defaultConfig;
  } else {
    this._cfg = {
      host: (typeof cfg.host === 'string' && cfg.host) || defaultConfig.host,
      port: ((typeof cfg.port === 'string' || cfg.port === 'number')
             && isFinite(cfg.port) && cfg.port > 0)
            || defaultConfig.port,
      user: (typeof cfg.user === 'string' && cfg.user) || defaultConfig.user,
      password: (typeof cfg.password === 'string' ? cfg.user : ''),
      db: (typeof cfg.db === 'string' ? cfg.db : ''),
      keepalive: cfg.keepalive,
      streamType: cfg.streamType
    };
  }
}
inherits(Client, EventEmitter);

Client.prototype.query = function(sql, params, options, cb) {
  if (typeof params === 'function') {
    cb = params;
    params = undefined;
    options = undefined;
  } else if (typeof options === 'function') {
    cb = options;
    options = undefined;
  }
  if (params instanceof Array) {
    // noop
  } else if (typeof params === 'object' && params !== null) {
    options = params;
    params = undefined;
  } else {
    params = undefined;
  }
  if (typeof options !== 'object' || options === null)
    options = undefined;

  var req = {
    sql,
    params,
    options,
    cb,
    dest: (typeof cb === 'function'
           ? []
           : (this._cfg.streamType !== 'normal'
              ? new SimpleResultStream(this._s, options && options.hwm)
              : new ResultStream(this._s, options && options.hwm))),
    build: undefined
  };

  this._queue.push(req);

  if (!this._s || !this._s.writable) {
    this.connect();
  } else {
    if (typeof cb !== 'function') {
      process.nextTick(() => {
        this._processQueue();
      });
    } else {
      this._processQueue();
    }
  }

  return (typeof cb === 'function' ? undefined : req.dest);
};

Client.prototype.connect = function connect(cb) {
  if (this._s && this._s.writable) {
    if (typeof cb === 'function')
      process.nextTick(cb);
    return;
  }

  if (typeof cb === 'function')
    this.once('ready', cb);

  if (this._s && this._s.connecting)
    return;

  this._s = new Socket();
  this._s._pg = this;
  this._s.on('connect', onConnect);
  this._s.on('error', (err) => {
    this.emit('error', err);
  });
  this._s.on('close', onClose);
  this._s.connect(this._cfg.port, this._cfg.host);
};

function onClose() {
  var self = this._pg;
  self.connected = false;
  self.status = null;
  self.backendParams = null;
  self._s = null;
  self._p = null;
  // TODO: make cleanup configurable?
  self._cleanupQueue();
  self.emit('close');
}

function onConnect() {
  var self = this._pg;

  self.connected = true;
  if (self._cfg.keepalive === true)
    this.setKeepAlive(true);
  else if (typeof self._cfg.keepalive === 'number')
    this.setKeepAlive(true, self._cfg.keepalive);

  self._p = new Protocol(function onMsg(msg, data) {
    protoHandler(self, msg, data);
  });
  var params = [
    'client_encoding', 'utf-8',
    'application_name', 'pg2.js',
    'user', self._cfg.user
  ];
  if (self._cfg.db)
    params.push('database', self._cfg.db);
  self._p.startup(params);
  self._p.pipe(this).pipe(self._p);
}

function protoHandler(self, msg, data) {
  var req;
  switch (msg) {
    case 68: // DataRow(D)
      var row = self._cur.build(data);
      if (!self._cur.dest._rs) {
        var result = self._cur.dest[self._cur.dest.length - 1];
        result[result.length] = row;
      } else if (!self._cur.dest._rs.push(row)) {
        self._s.pause();
      }
      break;
    case 84: // RowDescription(T)
      if (self._cur.dest._s)
        self._cur.dest.createRowStream();
      else
        self._cur.dest[self._cur.dest.length] = [];
      if (!self._cur.options || !self._cur.options.arrays)
        self._cur.build = createRowBuilder(data);
      else
        self._cur.build = createArrayRowBuilder(data);
      /*if (self._cur.params)
        self._p.execute();*/
      break;
    case 67: // CommandComplete(C)
      if (self._cur.dest._rs) {
        self._cur.dest._rs.push(null);
        self._cur.dest._rs = null;
      }
      /*if (self._cur.params)
        self._p.close();*/
      break;
    case 90: // ReadyForQuery(Z)
      self.status = data;
      req = self._cur;
      if (req) {
        self._cur = undefined;
        if (typeof req.cb === 'function') {
          var rows = req.dest;
          if (rows.length === 1)
            rows = rows[0];
          req.cb(null, rows);
        } else {
          req.dest.push(null);
        }
      }
      self._processQueue();
      break;
    /*case 49: // ParseComplete(1)
      self._p.bind(self._cur.params);
      break;
    case 50: // BindComplete(2)
      self._p.describe();
      break;
    case 51: // CloseComplete(3)
      self._p.sync();
      break;
    case 110: // NoData(n)
      self._p.execute();
      break;
    case 73: // EmptyQueryResponse(I)
      if (self._cur.params)
        self._p.sync();
      break;*/
    case 69: // ErrorResponse(E)
      req = self._cur;
      if (req) {
        if (typeof req.cb === 'function') {
          self._cur = undefined;
          req.cb(data);
        } else if (req.dest._rs) {
          req.dest._rs.emit('error', data);
        } else {
          req.dest.emit('error', data);
        }
      } else {
        self.emit('error', data);
      }
      /*if (req.params) {
        self._p.sync();
      }*/
      break;
    case 78: // NoticeResponse(N)
      self.emit('notice', data);
      break;
    case 65: // NotificationResponse(A)
      self.emit('notification', data);
      break;
    case 83: // ParameterStatus(S)
      if (self.backendParams === null)
        self.backendParams = new BackendParams();
      self.backendParams[data.parameter] = data.value;
      break;
    case 75: // BackendKeyData(K)
      self.key = data;
      break;
    case 82: // Authentication*(R)
      switch (data.result) {
        case 0:
          self.emit('ready');
          break;
        case 3:
          self._p.auth(Buffer.from(self._cfg.password));
          break;
        case 5:
          var hash = Buffer.allocUnsafe(3 + 16);
          hash.write('md5', 0, 3, 'ascii');
          crypto.createHash('md5')
                .update(crypto.createHash('md5')
                              .update(self._cfg.password + self._cfg.user)
                              .digest())
                .update(data.auth)
                .digest()
                .copy(hash, 3);
          self._p.auth(hash);
          break;
        default:
          self.emit('error', new Error('Unsupported authentication method'));
          self._s.end();
      }
      break;
  }
}

Client.prototype.end = function end(cb) {
  if (this._s && this._s.writable) {
    if (this._cur || this._queue.length)
      this._queue.push(terminateReq);
    else
      this._p.terminate();
  } else {
    process.nextTick(cb);
  }
};

Client.prototype.destroy = function destroy(cb) {
  if (this._s && this._s.writable) {
    this._s.once('close', cb);
    this._s.destroy();
  } else {
    process.nextTick(cb);
  }
};

Client.prototype._processQueue = function processQueue() {
  if (!this._cur
      && this._s.writable
      && this._queue.length
      && this.status !== null) {
    this._cur = this._queue.shift();
    if (this._cur === terminateReq) {
      this.status = null;
      this._p.terminate();
    } else {
      if (this._cur.params) {
        this._s.cork();
        this._p.parse(this._cur.sql);
        this._p.bind(this._cur.params);
        this._p.describe();
        this._p.execute();
        this._p.close();
        this._p.sync();
        this._s.uncork();
      } else {
        this._p.query(this._cur.sql);
      }
    }
  }
};

Client.prototype._cleanupQueue = function cleanupQueue() {
  var req = this._cur;
  var err = new Error('Connection to database lost');
  if (req && req !== terminateReq) {
    if (typeof req.cb === 'function')
      req.cb(err);
    else if (req.dest._rs && req.dest._rs.listenerCount('error'))
      req.dest._rs.emit('error', err);
    else
      req.dest.emit('error', err);
  }
  var p = this._queue.head;
  while (p !== null) {
    req = p.data;
    if (req !== terminateReq) {
      if (typeof req.cb === 'function')
        req.cb(err);
      else if (req.dest._rs && req.dest._rs.listenerCount('error'))
        req.dest._rs.emit('error', err);
      else
        req.dest.emit('error', err);
    }
    p = p.next;
  }
  this._queue.clear();
};

function ResultStream(socket, hwm) {
  Readable.call(this, readableConfig);
  this._hwm = hwm;
  this._s = socket;
  this._rs = null;
}
inherits(ResultStream, Readable);
ResultStream.prototype._read = function(n) {};
ResultStream.prototype.createRowStream = function() {
  this.push(this._rs = new RowStream(this._s, this._hwm));
};
function RowStream(socket, hwm) {
  if (typeof hwm === 'number' && hwm > 0 && isFinite(hwm))
    Readable.call(this, { objectMode: true, highWaterMark: hwm });
  else
    Readable.call(this, readableConfig);
  this._desc = null;
  this._s = socket;
}
inherits(RowStream, Readable);
RowStream.prototype._read = function(n) {
  this._s.resume();
};

function SimpleResultStream(socket, hwm) {
  Stream.call(this);
  this._rs = null;
  this._s = socket;
  this._shwm = hwm;

  this._buf = new List();
  this._hwm = 1;
  this._paused = true;
  this._flowing = false;
  this.ended = false;
  this.readable = true;
}
inherits(SimpleResultStream, Stream);

SimpleResultStream.prototype.createRowStream = function() {
  this.push(this._rs = new SimpleRowStream(this._s, this._shwm));
};
SimpleResultStream.prototype.pause = function pause() {
  if (this.ended)
    return;
  this._paused = true;
  this._flowing = true;
};
SimpleResultStream.prototype.resume = function resume() {
  if (this._paused) {
    this._paused = false;
    while (this._buf.length) {
      this.emit('data', this._buf.shift());
      if (this._paused)
        return;
    }
    if (this.ended)
      this.emit('end');
  }
};
SimpleResultStream.prototype.on = function on(ev, fn) {
  var res = Stream.prototype.on.call(this, ev, fn);
  if (ev === 'data' && !this._flowing)
    this.resume();
  return res;
};
SimpleResultStream.prototype.addListener = SimpleResultStream.prototype.on;
SimpleResultStream.prototype.push = function push(row) {
  if (this.ended)
    return false;
  if (this._paused) {
    if (row === null) {
      this.ended = true;
      this.readable = false;
      return false;
    }
    this._buf.push(row);
    if (this._buf.length >= this._hwm)
      return false;
  } else {
    if (row === null) {
      this.ended = true;
      this.readable = false;
      this.emit('end');
      return false;
    }
    this.emit('data', row);
  }
  return true;
};
function SimpleRowStream(socket, hwm) {
  Stream.call(this);
  this._desc = null;
  this._s = socket;

  this._buf = new List();
  if (typeof hwm === 'number' && hwm > 0 && isFinite(hwm))
    this._hwm = hwm;
  else
    this._hwm = 16;
  this._paused = true;
  this._flowing = false;
  this.ended = false;
  this.readable = true;
}
inherits(SimpleRowStream, Stream);

SimpleRowStream.prototype.pause = function pause() {
  if (this.ended)
    return;
  this._s.pause();
  this._paused = true;
  this._flowing = true;
};
SimpleRowStream.prototype.resume = function resume() {
  if (this._paused) {
    this._paused = false;
    while (this._buf.length) {
      this.emit('data', this._buf.shift());
      if (this._paused)
        return;
    }
    if (this.ended)
      this.emit('end');
    this._s.resume();
  }
};
SimpleRowStream.prototype.on = function on(ev, fn) {
  var res = Stream.prototype.on.call(this, ev, fn);
  if (ev === 'data' && !this._flowing)
    this.resume();
  return res;
};
SimpleRowStream.prototype.addListener = SimpleRowStream.prototype.on;
SimpleRowStream.prototype.push = function push(row) {
  if (this.ended)
    return false;
  if (this._paused) {
    if (row === null) {
      this.ended = true;
      this.readable = false;
      return false;
    }
    this._buf.push(row);
    if (this._buf.length >= this._hwm)
      return false;
  } else {
    if (row === null) {
      this.ended = true;
      this.readable = false;
      this.emit('end');
      return false;
    }
    this.emit('data', row);
  }
  return true;
};

function List() {
  this.head = null;
  this.tail = null;
  this.length = 0;
}

List.prototype.push = function push(v) {
  var entry = { data: v, next: null };
  if (this.length > 0)
    this.tail.next = entry;
  else
    this.head = entry;
  this.tail = entry;
  ++this.length;
};

List.prototype.shift = function shift() {
  if (this.length === 0)
    return;
  var ret = this.head.data;
  if (this.length === 1)
    this.head = this.tail = null;
  else
    this.head = this.head.next;
  --this.length;
  return ret;
};

List.prototype.clear = function clear() {
  this.head = this.tail = null;
  this.length = 0;
};

function BackendParams() {}
BackendParams.prototype = Object.create(null);

function escapeString(s) {
  var r = '';
  var last = 0;
  for (var i = 0; i < s.length; ++i) {
    switch (s.charCodeAt(i)) {
      case 34:
      case 92:
        if (i - last)
          r += s.slice(last, i);
        r += '\\';
        last = i;
    }
  }
  if (last)
    r += s.slice(last);
  return r || s;
}

function createRowBuilder(cols) {
  var fn = 'return {';
  for (var i = 0; i < cols.length; ++i)
    fn += '"' + escapeString(cols[i]) + '":v[' + i + '],';
  fn += '}';
  return new Function('v', fn);
}

function createArrayRowBuilder(cols) {
  var fn = 'return [';
  for (var i = 0; i < cols.length; ++i)
    fn += 'v[' + i + '],';
  fn += ']';
  return new Function('v', fn);
}
