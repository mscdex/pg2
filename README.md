
Description
===========

A PostgreSQL driver for [node.js](http://nodejs.org/) that focuses on performance.


Requirements
============

* [node.js](http://nodejs.org/) -- v6.3.0 or newer


Install
=======

    npm install pg2


Examples
========

* Buffered SELECT query:

```js
var Client = require('pg2');

var c = new Client({
  host: '127.0.0.1',
  user: 'foo',
  password: 'bar',
  db: 'test'
});

c.query('SELECT i FROM generate_series(1, 10) AS i', (err, rows) => {
  if (err)
    throw err;
  console.dir(rows);
});

c.end();
```

* Streamed SELECT query:

```js
var Client = require('pg2');

var c = new Client({
  host: '127.0.0.1',
  user: 'foo',
  password: 'bar',
  db: 'test'
});

c.query('SELECT i FROM generate_series(1, 10) AS i');
 .on('data', (result) => {
  result.on('data', (row) => {
    console.dir(row);
  });
});

c.end();
```

* Using arrays (faster) instead of objects for rows:

```javascript
var Client = require('pg2');

var c = new Client({
  host: '127.0.0.1',
  user: 'foo',
  password: 'bar',
  db: 'test'
});

c.query('SELECT i FROM generate_series(1, 10) AS i', { arrays: true }, (err, rows) => {
  if (err)
    throw err;
  console.dir(rows);
});

c.end();
```

* Using positional parameters in a query:

```javascript
var Client = require('pg2');

var c = new Client({
  host: '127.0.0.1',
  user: 'foo',
  password: 'bar',
  db: 'test'
});

c.query('SELECT i FROM generate_series($1::numeric, $2::numeric) AS i',
        [1, 10],
        (err, rows) => {
  if (err)
    throw err;
  console.dir(rows);
});

c.end();
```


API
===

`require('pg2')` returns a **_Client_** object


Client properties
-----------------

* **connected** - _boolean_ - `true` if the Client instance is currently connected to the server.

* **backendParams** - _object_ - Once authenticated, this value will contain any backend status values. If no such values have been received, the value of this property will be `null`.

* **key** - _object_ - Once authenticated, this object will contain two properties (`pid` and `key`) which is used to uniquely identify this connection on the server.

* **status** - _integer_ - Indicates the backend transaction status. Valid values:

    * `73` - Not in a transactional block
    * `84` - In a transactional block
    * `69` - In a *failed* transactional block (queries will be rejected until block is ended)


Client events
-------------

* **ready**() - A connection to the server has been established and authentication was successful.

* **error**(< _Error_ >err) - An error occurred at the connection level.

* **close**() - The connection has been closed.


Client methods
--------------

* **(constructor)**([< _object_ >config]) - Creates and returns a new Client instance. Valid `config` options include:

    * **user** - _string_ - Username for authentication. **Default:** (OS username of current logged in user)

    * **password** - _string_ - Password for authentication. **Default:** `''`

    * **host** - _string_ - Hostname or IP address of the server. **Default:** `'localhost'`

    * **port** - _integer_ - Port number of the server. **Default:** `5432`

    * **db** - _string_ - A database to automatically select after authentication. **Default:** `''`
    
    * **keepalive** - _mixed_ - If `true`, this enables TCP keepalive probes using the OS default initial delay value. If a number, this both enables TCP keepalive probes and sets the initial delay value to the given value. **Default:** `true`

    * **streamType** - _string_ - Set to `'normal'` to use node's full-featured streams instead of simpler streams. The main difference is that the simpler streams do not implement `.read()` support. **Default:** `'simple'`

* **query**(< _string_ >query[, < _array_ >values][, < _object_ >options][, < _function_ >callback]) - _mixed_ - Enqueues the given `query`. If `callback` is not supplied, a `ResultStream` instance is returned. `values` can be an array of values that correspond to positional placeholders inside `query`. Valid `options` are:

    * **arrays** - _boolean_ - When `true`, arrays are used to store row values instead of an object keyed on column names. (Note: using arrays performs better) **Default:** `false`

    * **hwm** - _integer_ - This is the `highWaterMark` to use for `RowStream` instances emitted by a `ResultStream` instance. This only applies when streaming rows. **Default:** (node's default) `16`

* **connect**([< _function_ >callback]) - _(void)_ - Explicitly attempts to connect to the server. Note that calling `query()` will implicitly attempt a connection if one is not already made. If not connected, `callback` is added as a one-time `'ready'` event listener.

* **end**() - _(void)_ - Closes the connection once all queries in the queue have been executed.

* **destroy**() - _(void)_ - Closes the connection immediately, even if there are other queries still in the queue. Any/all queries still in the queue are properly notified.


`ResultStream` is an object stream that emits `RowStream` instances. In the case of multiple statements/queries passed to `query()` (separated by `;`), there will be one `RowStream` instance for each statement/query.

`RowStream` is an object stream that emits rows.


TODO (in no particular order)
====

* Allow passing back of data type OIDs for columns sent by the server

* `client.abort()` that implicitly opens a new, temporary connection to kill the currently running query

* COPY data support (both in and out)
