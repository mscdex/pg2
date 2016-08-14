'use strict';
var inherits = require('util').inherits;
var Transform = require('stream').Transform;
var StringDecoder = require('string_decoder').StringDecoder;

module.exports = Protocol;

var PACKET_TYPE = 0;
var PACKET_LEN = 1;
var PACKET_DATA = 2;
var BUF_TERMINATE = Buffer.from([ 88, 0, 0, 0, 4 ]);
var BUF_SSLREQ = Buffer.from([ 0, 0, 0, 8, 4, 210, 22, 47 ]);
var BUF_FLUSH = Buffer.from([ 72, 0, 0, 0, 4 ]);
var BUF_EXECUTE = Buffer.from([ 69, 0, 0, 0, 9, 0, 0, 0, 0, 0 ]);
var BUF_DESC_STMT = Buffer.from([ 68, 0, 0, 0, 6, 83, 0 ]);
var BUF_DESC_PORT = Buffer.from([ 68, 0, 0, 0, 6, 80, 0 ]);
var BUF_CLOSE = Buffer.from([ 67, 0, 0, 0, 6, 83, 0 ]);
var BUF_SYNC = Buffer.from([ 83, 0, 0, 0, 4 ]);

function Protocol(handler) {
  Transform.call(this);
  this._terminated = false;
  this._state = PACKET_TYPE;
  this._ptype = -1;
  this._len = 0;
  this._nb = 4;
  this._data = null;
  this._f = 0;
  this._sd = new StringDecoder();
  this._handler = (msg, data) => {
    handler(msg, data);
    this._len = 0;
    this._nb = 4;
    this._data = null;
    this._state = PACKET_TYPE;
  };
}
inherits(Protocol, Transform);

Protocol.prototype._transform = function onTransform(chunk, enc, cb) {
  var i = 0;
  var len = chunk.length;
  var end;
  while (i < len) {
    switch (this._state) {
      case PACKET_TYPE:
        this._ptype = chunk[i++];
        this._state = PACKET_LEN;
        if (i === len)
          return cb();
      // Fallthrough
      case PACKET_LEN:
        end = ((len - i) >= this._nb ? i + this._nb : len);
        this._nb -= (end - i);
        while (i < end) {
          this._len <<= 8;
          this._len += chunk[i++];
        }
        if (this._nb)
          return cb();
        this._len -= 4;
        this._state = PACKET_DATA;
      // Fallthrough
      case PACKET_DATA:
        switch (this._ptype) {
          case 49: this._handler(49, null); break;
          case 50: this._handler(50, null); break;
          case 51: this._handler(51, null); break;
          case 65: i = parseA(this, chunk, i, len); break;
          case 67: i = parseC(this, chunk, i, len); break;
          case 68: i = parseD(this, chunk, i, len); break;
          case 69: i = parseE(this, chunk, i, len); break;
          case 73: this._handler(73, null); break;
          case 75: i = parseK(this, chunk, i, len); break;
          case 78: i = parseN(this, chunk, i, len); break;
          case 82: i = parseR(this, chunk, i, len); break;
          case 83: i = parseS(this, chunk, i, len); break;
          case 84: i = parseT(this, chunk, i, len); break;
          case 90: i = parseZ(this, chunk, i, len); break;
          case 99: this._handler(99, null); break;
          case 110: this._handler(110, null); break;
          case 115: this._handler(115, null); break;
          default: var err = new Error('Unknown packet type: ' + this._ptype);
                   return this.emit('error', err);
        }
        if (this._len)
          return cb();
        break;
    }
  }
  cb();
};

Protocol.prototype.startup = function startup(kv) {
  if (!this.writable)
    return false;
  var kvlen = 0;
  for (var i = 0; i < kv.length;)
    kvlen += Buffer.byteLength(kv[i++]) + Buffer.byteLength(kv[i++]) + 2;
  var buf = Buffer.allocUnsafe(4 + 4 + kvlen + 1);
  buf.writeInt32BE(buf.length, 0, true);
  buf.writeInt32BE(196608, 4, true);
  for (var i = 0, p = 8; i < kv.length;) {
    p += buf.write(kv[i++], p);
    buf[p++] = 0;
    p += buf.write(kv[i++], p);
    buf[p++] = 0;
  }
  buf[p] = 0;
  return this.push(buf);
};

Protocol.prototype.query = function query(sql) {
  if (!this.writable)
    return false;
  var slen = Buffer.byteLength(sql) + 1;
  var buf = Buffer.allocUnsafe(5 + slen);
  buf[0] = 81;
  buf.writeInt32BE(slen + 4, 1, true);
  buf.write(sql, 5);
  buf[buf.length - 1] = 0;
  return this.push(buf);
};

Protocol.prototype.auth = function auth(data) {
  if (!this.writable)
    return false;
  var buf = Buffer.allocUnsafe(5 + data.length);
  buf[0] = 112;
  buf.writeInt32BE(data.length + 4, 1, true);
  data.copy(buf, 5);
  return this.push(buf);
};

Protocol.prototype.flush = function flush() {
  if (!this.writable)
    return false;
  return this.push(BUF_FLUSH);
};

Protocol.prototype.sslRequest = function sslRequest() {
  if (!this.writable)
    return false;
  return this.push(BUF_SSLREQ);
};

Protocol.prototype.parse = function parse(sql) {
  if (!this.writable)
    return false;
  var slen = Buffer.byteLength(sql) + 1;
  var buf = Buffer.allocUnsafe(8 + slen);
  buf[0] = 80;
  buf.writeInt32BE(slen + 7, 1, true);
  buf[5] = 0;
  buf.write(sql, 6);
  buf.fill(0, buf.length - 3);
  return this.push(buf);
};

Protocol.prototype.bind = function bind(vals) {
  if (!this.writable)
    return false;
  var vlen = 0;
  for (var i = 0; i < vals.length; ++i) {
    if (vals[i] == null)
      vlen += 4;
    else
      vlen += 4 + Buffer.byteLength(vals[i]);
  }
  var buf = Buffer.allocUnsafe(13 + vlen);
  buf[0] = 66;
  buf.writeInt32BE(buf.length - 1, 1, true);
  buf.fill(0, 5, 9);
  buf.writeInt16BE(vals.length, 9, true);
  var p = 11;
  for (var i = 0; i < vals.length; ++i) {
    if (vals[i] == null) {
      buf.writeInt32BE(-1, p, true);
      p += 4;
    } else {
      buf.writeInt32BE(Buffer.byteLength(vals[i]), p, true);
      p += 4;
      p += buf.write('' + vals[i], p);
    }
  }
  buf.fill(0, buf.length - 2);
  return this.push(buf);
};

Protocol.prototype.execute = function execute() {
  if (!this.writable)
    return false;
  return this.push(BUF_EXECUTE);
};

Protocol.prototype.describe = function describe() {
  if (!this.writable)
    return false;
  return this.push(BUF_DESC_PORT);
};

Protocol.prototype.close = function close() {
  if (!this.writable)
    return false;
  return this.push(BUF_CLOSE);
};

Protocol.prototype.sync = function sync() {
  if (!this.writable)
    return false;
  return this.push(BUF_SYNC);
};

Protocol.prototype.terminate = function terminate() {
  if (this._terminated || !this.writable)
    return false;
  this.push(BUF_TERMINATE);
  this.push(null);
  this._terminated = true;
  return false;
};

function parseA(self, chunk, i, len) {
  if (!self._data) {
    self._data = { pid: 0, channel: '', payload: '' };
    self._nb = 4;
    self._f = 0;
  }
  var idx;
  var end;
  while (i < len) {
    switch (self._f) {
      case 0:
        end = ((len - i) >= self._nb ? i + self._nb : len);
        self._nb -= (end - i);
        while (i < end) {
          self._data.pid <<= 8;
          self._data.pid += chunk[i++];
        }
        if (self._nb)
          return len;
        ++self._f;
        break;
      case 1:
        idx = chunk.indexOf(0, i);
        if (idx === -1) {
          self._data.channel += self._sd.write(chunk.slice(i));
          return len;
        } else if (self._data.channel) {
          self._data.channel += self._sd.end(chunk.slice(i, idx));
          self._sd.lastTotal = self._sd.lastNeed = 0;
        } else {
          self._data.channel = chunk.toString('utf8', i, idx);
        }
        i = idx + 1;
        ++self._f;
        break;
      case 2:
        idx = chunk.indexOf(0, i);
        if (idx === -1) {
          self._data.payload += self._sd.write(chunk.slice(i));
          return len;
        } else if (self._data.payload) {
          self._data.payload += self._sd.end(chunk.slice(i, idx));
          self._sd.lastTotal = self._sd.lastNeed = 0;
        } else {
          self._data.payload = chunk.toString('utf8', i, idx);
        }
        self._handler(65, self._data);
        return idx + 1;
    }
  }
  return i;
}

function parseC(self, chunk, i, len) {
  if (!self._data)
    self._data = { tag: '' };
  var idx = chunk.indexOf(0, i);
  if (idx === -1) {
    self._data.tag += self._sd.write(chunk.slice(i));
    return len;
  } else if (self._data.tag) {
    self._data.tag += self._sd.end(chunk.slice(i, idx));
    self._sd.lastTotal = self._sd.lastNeed = 0;
  } else {
    self._data.tag = chunk.toString('utf8', i, idx);
  }
  self._handler(67, self._data);
  return idx + 1;
}

function parseD(self, chunk, i, len) {
  if (!self._data) {
    self._data = { data: null, _cnt: 0, _size: 0 };
    self._nb = 2;
    self._f = 0;
  }
  var end;
  while (i < len) {
    switch (self._f) {
      case 0:
        end = ((len - i) >= self._nb ? i + self._nb : len);
        self._nb -= (end - i);
        while (i < end) {
          self._data._cnt <<= 8;
          self._data._cnt += chunk[i++];
        }
        if (self._nb)
          return len;
        self._data.data = new Array(self._data._cnt);
        if (self._data._cnt === 0) {
          self._handler(68, self._data.data);
          return i;
        }
        self._data._cnt = 0;
        self._nb = 4;
        ++self._f;
        break;
      case 1:
        end = ((len - i) >= self._nb ? i + self._nb : len);
        self._nb -= (end - i);
        while (i < end) {
          self._data._size <<= 8;
          self._data._size += chunk[i++];
        }
        if (self._nb)
          return len;
        if (self._data._size === -1) {
          self._data.data[self._data._cnt] = null;
          if (++self._data._cnt === self._data.data.length) {
            self._handler(68, self._data.data);
            return i;
          }
        } else {
          self._data.data[self._data._cnt] = '';
          if (self._data._size === 0) {
            if (++self._data._cnt === self._data.data.length) {
              self._handler(68, self._data.data);
              return i;
            }
          } else {
            ++self._f;
          }
        }
        self._nb = 4;
        break;
      case 2:
        end = ((len - i) >= self._data._size ? i + self._data._size : len);
        if ((self._data._size -= (end - i)) > 0) {
          self._data.data[self._data._cnt] += self._sd.write(chunk.slice(i, end));
          return len;
        } else if (self._data.data[self._data._cnt]) {
          self._data.data[self._data._cnt] += self._sd.end(chunk.slice(i, end));
          self._sd.lastTotal = self._sd.lastNeed = 0;
        } else {
          self._data.data[self._data._cnt] = chunk.toString('utf8', i, end);
        }
        if (++self._data._cnt === self._data.data.length) {
          self._handler(68, self._data.data);
          return end;
        }
        i = end;
        self._f = 1;
        break;
    }
  }
  return i;
}

function parseE(self, chunk, i, len) {
  if (!self._data) {
    self._data = { prop: null, err: new PostgresError() };
    self._f = 0;
  }
  var prop;
  var idx;
  while (i < len) {
    switch (self._f) {
      case 0:
        var type = chunk[i++];
        if (type === 0) {
          self._handler(69, self._data.err);
          return i;
        }
        switch (type) {
          case 67: prop = 'code'; break;
          case 68: prop = 'detail'; break;
          case 70: prop = 'file'; break;
          case 72: prop = 'hint'; break;
          case 76: prop = 'line'; break;
          case 77: prop = 'message'; break;
          case 80: prop = 'position'; break;
          case 82: prop = 'routine'; break;
          case 83: prop = 'severity'; break;
          case 87: prop = 'where'; break;
          case 99: prop = 'column'; break;
          case 100: prop = 'dataType'; break;
          case 110: prop = 'constraint'; break;
          case 112: prop = 'internalPosition'; break;
          case 113: prop = 'internalQuery'; break;
          case 115: prop = 'schema'; break;
          case 116: prop = 'table'; break;
          default: prop = null;
        }
        self._data.prop = prop;
        ++self._f;
        break;
      case 1:
        idx = chunk.indexOf(0, i);
        prop = self._data.prop;
        if (idx === -1) {
          if (prop !== null) {
            if (typeof self._data.err[prop] !== 'string')
              self._data.err[prop] = self._sd.write(chunk.slice(i));
            else
              self._data.err[prop] += self._sd.write(chunk.slice(i));
          }
          return len;
        } else if (prop !== null) {
          if (typeof self._data.err[prop] !== 'string') {
            self._data.err[prop] = chunk.toString('utf8', i, idx);
          } else {
            self._data.err[prop] += self._sd.end(chunk.slice(i, idx));
            self._sd.lastTotal = self._sd.lastNeed = 0;
          }
        }
        i = idx + 1;
        self._f = 0;
        break;
    }
  }
  return i;
}

function parseK(self, chunk, i, len) {
  if (!self._data) {
    self._data = { pid: 0, key: 0 };
    self._nb = 4;
    self._f = 0;
  }
  var end;
  while (i < len) {
    switch (self._f) {
      case 0:
        end = ((len - i) >= self._nb ? i + self._nb : len);
        self._nb -= (end - i);
        while (i < end) {
          self._data.pid <<= 8;
          self._data.pid += chunk[i++];
        }
        if (self._nb)
          return len;
        self._nb = 4;
        ++self._f;
        break;
      case 1:
        end = ((len - i) >= self._nb ? i + self._nb : len);
        self._nb -= (end - i);
        while (i < end) {
          self._data.key <<= 8;
          self._data.key += chunk[i++];
        }
        if (self._nb)
          return len;
        self._handler(75, self._data);
        return i;
    }
  }
  return i;
}

function parseN(self, chunk, i, len) {
  if (!self._data) {
    self._data = { prop: null, notice: new PostgresNotice() };
    self._f = 0;
  }
  var prop;
  var idx;
  while (i < len) {
    switch (self._f) {
      case 0:
        var type = chunk[i++];
        if (type === 0) {
          self._handler(78, self._data.notice);
          return i;
        }
        switch (type) {
          case 67: prop = 'code'; break;
          case 68: prop = 'detail'; break;
          case 70: prop = 'file'; break;
          case 72: prop = 'hint'; break;
          case 76: prop = 'line'; break;
          case 77: prop = 'message'; break;
          case 80: prop = 'position'; break;
          case 82: prop = 'routine'; break;
          case 83: prop = 'severity'; break;
          case 87: prop = 'where'; break;
          case 99: prop = 'column'; break;
          case 100: prop = 'dataType'; break;
          case 110: prop = 'constraint'; break;
          case 112: prop = 'internalPosition'; break;
          case 113: prop = 'internalQuery'; break;
          case 115: prop = 'schema'; break;
          case 116: prop = 'table'; break;
          default: prop = null;
        }
        self._data.prop = prop;
        ++self._f;
        break;
      case 1:
        idx = chunk.indexOf(0, i);
        prop = self._data.prop;
        if (idx === -1) {
          if (prop !== null) {
            if (typeof self._data.notice[prop] !== 'string')
              self._data.notice[prop] = self._sd.write(chunk.slice(i));
            else
              self._data.notice[prop] += self._sd.write(chunk.slice(i));
          }
          return len;
        } else if (prop !== null) {
          if (typeof self._data.notice[prop] !== 'string') {
            self._data.notice[prop] = chunk.toString('utf8', i, idx);
          } else {
            self._data.notice[prop] += self._sd.end(chunk.slice(i, idx));
            self._sd.lastTotal = self._sd.lastNeed = 0;
          }
        }
        i = idx + 1;
        self._f = 0;
        break;
    }
  }
  return i;
}

function parseR(self, chunk, i, len) {
  if (!self._data) {
    self._data = { result: 0, auth: null };
    self._nb = 4;
    self._f = 0;
  }
  var end;
  while (i < len) {
    switch (self._f) {
      case 0:
        end = ((len - i) >= self._nb ? i + self._nb : len);
        self._nb -= (end - i);
        while (i < end) {
          self._data.result <<= 8;
          self._data.result += chunk[i++];
        }
        if (self._nb)
          return len;
        switch (self._data.result) {
          case 0:
          case 2:
          case 3:
          case 6:
          case 7:
          case 9:
            self._handler(82, self._data);
            return i;           
          case 5:
            self._nb = 4;
            break;
          case 8:
            self._nb = self._len - 8;
            break;
        }
        self._data.auth = Buffer.allocUnsafe(self._nb);
        self._f = 1;
        break;
      case 1:
        if ((len - i) < self._nb) {
          chunk.copy(self._data.auth, 4 - self._nb, i, len);
          return len;
        } else {
          chunk.copy(self._data.auth, 4 - self._nb, i, i += self._nb);
          self._handler(82, self._data);
          return i;
        }
        break;
    }
  }
  return i;
}

function parseS(self, chunk, i, len) {
  if (!self._data) {
    self._data = { param: '', value: '' };
    self._nb = 4;
    self._f = 0;
  }
  var idx;
  while (i < len) {
    switch (self._f) {
      case 0:
        idx = chunk.indexOf(0, i);
        if (idx === -1) {
          self._data.param += self._sd.write(chunk.slice(i));
          return len;
        } else if (self._data.param) {
          self._data.param += self._sd.end(chunk.slice(i, idx));
          self._sd.lastTotal = self._sd.lastNeed = 0;
        } else {
          self._data.param = chunk.toString('utf8', i, idx);
        }
        i = idx + 1;
        ++self._f;
        break;
      case 1:
        idx = chunk.indexOf(0, i);
        if (idx === -1) {
          self._data.value += self._sd.write(chunk.slice(i));
          return len;
        } else if (self._data.value) {
          self._data.value += self._sd.end(chunk.slice(i, idx));
          self._sd.lastTotal = self._sd.lastNeed = 0;
        } else {
          self._data.value = chunk.toString('utf8', i, idx);
        }
        self._handler(83, self._data);
        return idx + 1;
    }
  }
  return i;
}

function parseT(self, chunk, i, len) {
  if (!self._data) {
    self._data = { fields: null, _i: 0 };
    self._nb = 2;
    self._f = 0;
  }
  var end;
  var idx;
  while (i < len) {
    switch (self._f) {
      case 0:
        end = ((len - i) >= self._nb ? i + self._nb : len);
        self._nb -= (end - i);
        while (i < end)
          self._data._i = (self._data._i << 8) + chunk[i++];
        if (self._nb)
          return len;
        self._data.fields = new Array(self._data._i);
        if (self._data._i === 0) {
          self._handler(84, self._data.fields);
          return i;
        }
        self._data._i = 0;
        self._f = 1;
        break;
      case 1:
        idx = chunk.indexOf(0, i);
        if (idx === -1) {
          self._data.fields[self._data._i] += self._sd.write(chunk.slice(i));
          return len;
        } else if (self._data.param) {
          self._data.fields[self._data._i] += self._sd.end(chunk.slice(i, idx));
          self._sd.lastTotal = self._sd.lastNeed = 0;
        } else {
          self._data.fields[self._data._i] = chunk.toString('utf8', i, idx);
        }
        i = idx + 1;
        self._nb = 18;
        self._f = 2;
        break;
      case 2:
        end = ((len - i) >= self._nb ? i + self._nb : len);
        if (self._nb -= (end - i))
          return len;
        i = end;
        if (++self._data._i === self._data.fields.length) {
          self._handler(84, self._data.fields);
          return i;
        }
        self._f = 1;
        break;
    }
  }
  return i;
}

function parseZ(self, chunk, i, len) {
  self._handler(90, chunk[i]);
  return i + 1;
}

function PostgresError() {
  Error.captureStackTrace(this, PostgresError);
  this.severity = undefined;
  this.code = undefined;
  this.message = undefined;
  this.detail = undefined;
  this.hint = undefined;
  this.position = undefined;
  this.internalPosition = undefined;
  this.internalQuery = undefined;
  this.where = undefined;
  this.schema = undefined;
  this.table = undefined;
  this.column = undefined;
  this.dataType = undefined;
  this.constraint = undefined;
  this.file = undefined;
  this.line = undefined;
  this.routine = undefined;
}
inherits(PostgresError, Error);
PostgresError.prototype.name = 'PostgresError';

function PostgresNotice() {
  this.severity = undefined;
  this.code = undefined;
  this.message = undefined;
  this.detail = undefined;
  this.hint = undefined;
  this.position = undefined;
  this.internalPosition = undefined;
  this.internalQuery = undefined;
  this.where = undefined;
  this.schema = undefined;
  this.table = undefined;
  this.column = undefined;
  this.dataType = undefined;
  this.constraint = undefined;
  this.file = undefined;
  this.line = undefined;
  this.routine = undefined;
}
PostgresNotice.prototype.name = 'PostgresNotice';
