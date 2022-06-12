'use strict'
const {
  AbstractLevel,
  AbstractIterator,
  AbstractKeyIterator,
  AbstractValueIterator
} = require('abstract-level')

const Redis = require('ioredis')
const { parseURL } = require('ioredis/built/utils')

const ModuleError = require('module-error')
const crypto = require('crypto')
const { connect } = require('./scripts-loader')

const rangeOptions = new Set(['gt', 'gte', 'lt', 'lte'])
const kClient = Symbol('client')
const kDb = Symbol('db')
const kExec = Symbol('exec')
const kCount = Symbol('count')
const kLimit = Symbol('limit')
const kOffset = Symbol('offset')
const kBuffered = Symbol('buffered')
const kReverse = Symbol('reverse')
const kPointer = Symbol('pointer')
const kDone = Symbol('done')
const kStart = Symbol('start')
const kEnd = Symbol('end')
const kOptions = Symbol('options')
const kFetch = Symbol('fetch')
const kMakeRangeArgs = Symbol('makeRangeArgs')
const kHighWaterMark = Symbol('highWaterMark')
const kInit = Symbol('init')
const kCmdName = Symbol('cmdName')
const kLocation = Symbol('location')
const kHashId = Symbol('hashId')
const kQuitDBOnClose = Symbol('quitOnClose')
const defaultHighWaterMark = 128

class RedisIterator extends AbstractIterator {
  constructor (db, options) {
    super(db, options)
    this[kInit](db, options)
  }
}

class RedisKeyIterator extends AbstractKeyIterator {
  constructor (db, options) {
    super(db, options)
    this[kInit](db, options)
  }
}

class RedisValueIterator extends AbstractValueIterator {
  constructor (db, options) {
    super(db, options)
    this[kInit](db, options)
  }
}

for (const Ctor of [RedisIterator, RedisKeyIterator, RedisValueIterator]) {
  const needKeys = (Ctor === RedisKeyIterator) || (Ctor === RedisIterator)
  const needValues = (Ctor === RedisValueIterator) || (Ctor === RedisIterator)
  const needBoth = needKeys && needValues

  Ctor.prototype[kInit] = function (db, options) {
    const reverse = this[kReverse] = !!options.reverse
    const cmdSuffix = isBufferEncoding(options.valueEncoding) ? 'Buffer' : ''
    if (!needValues) {
      // key streams: no need for lua
      this[kCmdName] = (reverse ? 'zrevrangebylex' : 'zrangebylex') + cmdSuffix
    } else {
      this[kCmdName] = (needKeys ? 'iterPairs' : 'iterValues') + cmdSuffix
    }

    this[kDb] = db
    this[kLocation] = db[kLocation]
    this[kClient] = db[kClient]
    this[kCount] = 0
    this[kOffset] = 0
    this[kLimit] = (options.limit || -1)
    this[kPointer] = 0
    this[kDone] = false
    this[kBuffered] = []
    this[kHighWaterMark] = options.highWaterMark || defaultHighWaterMark
    this[kOptions] = options

    const { start, end } = processRangeOptions(options)
    this[kStart] = start
    this[kEnd] = end
  }

  /**
   * Retrieve the next itemsfrom iterator buffer and fetch from server if needed
   * @param callback function
   */
  Ctor.prototype._next = function (callback) {
    const complete = () => {
      const buf = this[kBuffered]
      if (this[kPointer] >= buf.length) {
        return callback()
      }
      const val = buf[this[kPointer]++]
      const hasVal = Array.isArray(val) ? (val[0] !== undefined || val[1] !== undefined) : val !== undefined
      if (hasVal) {
        const count = this[kCount]++
        const limit = this[kLimit]
        if (limit > 0 && count >= limit) {
          this[kDone] = true
        }
      }
      if (needBoth) {
        // we have an array [key, value]
        callback(null, val[0], val[1])
      } else {
        callback(null, val)
      }
    }

    if (this[kDone]) {
      callback()
    } else if (this[kPointer] >= this[kBuffered].length) {
      this[kFetch](err => {
        if (err) {
          callback(err)
        } else {
          complete()
        }
      })
    } else {
      complete()
    }
  }

  Ctor.prototype._nextv = function (size, options, callback) {
    if (this[kDone]) {
      return this.nextTick(callback)
    }
    const entries = []

    const loop = () => {
      const buffer = this[kBuffered]
      while (!this[kDone] && this[kPointer] < buffer.length && entries.length < size) {
        const k = buffer[this[kPointer]++]
        entries.push(k)
      }
      if (entries.length < size && !this[kDone]) {
        this[kFetch]((err) => {
          this.nextTick(err ? callback : loop, err)
        })
      } else {
        return callback(null, entries)
      }
    }

    this.nextTick(loop)
  }

  Ctor.prototype._all = function (options, callback) {
    if (this[kDone]) {
      return this.nextTick(callback)
    }

    const highWaterMark = this[kHighWaterMark]
    const res = []

    const loop = () => {
      const buffer = this[kBuffered]
      while (!this[kDone] && this[kPointer] < buffer.length) {
        const k = buffer[this[kPointer]++]
        res.push(k)
      }
      if (!this[kDone]) {
        this[kFetch]((err) => {
          if (err) {
            this[kHighWaterMark] = highWaterMark
            return callback(err)
          }
          return this.nextTick(loop)
        })
      } else {
        this[kHighWaterMark] = highWaterMark
        return this.nextTick(callback, null, res)
      }
    }

    // Some time to breathe
    this.nextTick(loop)
  }

  Ctor.prototype._seek = function (target, options) {
    this[kStart] = target
    this[kOffset] = 0
    this[kPointer] = 0
    this[kBuffered] = []
    this[kDone] = false
  }

  /**
   * Gets a batch of key or values or pairs
   */
  Ctor.prototype[kFetch] = function (callback) {
    const db = this[kDb]
    const client = this[kClient]

    if (this[kDone] || db.status === 'closed') {
      this[kDone] = true
      return this.nextTick(callback)
    }
    const highWaterMark = this[kHighWaterMark]
    let size
    const limit = this[kLimit]
    if (limit > -1) {
      const remain = limit - this[kOffset]
      if (remain <= 0) {
        this[kDone] = true
        return this.nextTick(callback)
      }
      size = remain <= highWaterMark ? remain : highWaterMark
    } else {
      size = highWaterMark
    }
    const rangeArgs = this[kMakeRangeArgs](size)

    this[kOffset] += size

    const complete = (err, reply) => {
      if (err) {
        return this.nextTick(callback, err)
      }

      this[kPointer] = 0
      let values
      // simplify fetching logic in case of keys && values
      if (needBoth) {
        values = new Array(reply.length / 2)
        let k = 0
        for (let i = 0; i < reply.length; i += 2) {
          const v = reply[i + 1]
          values[k++] = [reply[i], v === null ? undefined : v]
        }
      } else if (needValues) {
        values = new Array(reply.length)
        for (let i = 0; i < reply.length; i++) {
          const v = reply[i]
          values[i] = v === null ? undefined : v
        }
      }
      const buffer = this[kBuffered] = values
      if (buffer.length) {
        // update _start for the next page fetch
        if (!needKeys) {
          this[kStart] = concatKey('(', reply.pop())
        } else if (!needValues) {
          this[kStart] = concatKey('(', reply[reply.length - 1])
        } else {
          this[kStart] = concatKey('(', reply[reply.length - 2])
        }
      } else {
        this[kStart] = ''
        this[kDone] = true
        return this.nextTick(callback)
      }
      this.nextTick(callback)
    }

    const method = this[kCmdName]
    return client[method](...rangeArgs, complete)
  }

  Ctor.prototype[kMakeRangeArgs] = function (count) {
    const prefix = this[kLocation]
    const zKey = prefix + ':z'
    const start = this[kStart]
    const end = this[kEnd]
    if (needValues) {
      const hKey = prefix + ':h'
      return [zKey, hKey, this[kReverse] ? 1 : 0, start, end, count]
    } else {
      return [zKey, start, end, 'LIMIT', 0, count]
    }
  }
}

/**
 * Map of connections to the RedisLevel instances sharing it
 * the key is a hash of the connection options
 * @type {Map<string, { client: Redis; levels: RedisLevel[]; }>}
 */
const connections = new Map()

class RedisLevel extends AbstractLevel {
  constructor (location, options, _) {
    let forward

    if (typeof location === 'object' && location !== null) {
      const { location: _location, ...forwardOptions } = location
      location = _location
      forward = forwardOptions
    } else {
      forward = options
    }

    if (typeof location !== 'string' || location === '') {
      throw new TypeError("The first argument 'location' must be a non-empty string")
    }

    super({
      seek: true,
      permanence: false,
      createIfMissing: false,
      errorIfExists: false,
      encodings: { buffer: true, utf8: true, view: true }
    }, forward)

    this[kLocation] = sanitizeLocation(location)
  }

  get location () {
    return this[kLocation]
  }

  /**
   * @param options OpenOptions  either one of
   *  - ioredis instance.
   *  - object with { connection: Redis }
   *  - object with { connection: redis_url }
   *  - object with { connection: { port: portNumber, host: host } ... other options passed to ioredis }
   *
   * When a client is created it is reused across instances of
   * RedisLevel unless the option `ownClient` is truthy.
   * For a client to be reused, it requires the same port, host and options.
   */
  _open (options, callback) {
    // todo: do we need to handle `passive` ?
    const opts = Object.assign({
      connection: {
        port: 6379,
        host: '127.0.0.1',
        db: 0
      }
    }, options)
    this[kHighWaterMark] = options.highWaterMark || defaultHighWaterMark
    if (typeof opts.connection === 'string') {
      opts.connection = parseURL(opts.connection)
    }
    const location = this.location
    const redis = opts.connection

    if (redis && typeof redis.hget === 'function') {
      this[kClient] = redis
      this[kQuitDBOnClose] = false
    } else if (!options.ownClient) {
      const hashOptions = _getBaseOptions(location, redis)
      const hashId = this[kHashId] = hash(hashOptions)
      const dbDesc = connections.get(hashId)
      if (dbDesc) {
        this[kClient] = dbDesc.client
        dbDesc.levels.push(this)
      }
    } else {
      this[kQuitDBOnClose] = true
    }

    let isNew = false
    let client = this[kClient]
    if (!client) {
      client = new Redis(redis)
      isNew = true
      this[kClient] = client
      if (!options.ownClient) {
        connections.set(this[kHashId], { client, levels: [this] })
      }
    }

    connect(client).then(() => {
      if (options.clearOnOpen === true && isNew) {
        this.destroy(false, callback)
      } else {
        this.nextTick(callback)
      }
    }).catch(callback)
  }

  _close (callback) {
    const quitOnClose = this[kQuitDBOnClose]
    if (quitOnClose === false) {
      return this.nextTick(callback)
    }
    let client
    if (quitOnClose !== true) {
      // close the client only if it is not used by others:
      const hashId = this[kHashId]
      const dbDesc = connections.get(hashId)
      if (dbDesc) {
        client = dbDesc.client
        dbDesc.levels = dbDesc.levels.filter(x => x !== this)
        if (dbDesc.levels.length !== 0) {
          // still used by another RedisLevel
          return this.nextTick(callback)
        }
        connections.delete(hashId)
      }
    } else {
      client = this[kClient]
    }
    if (client) {
      try {
        client.disconnect()
      } catch (x) {
        console.log('Error attempting to close the redis client', x)
      }
    }
    this.nextTick(callback)
  }

  _put (key, value, options, callback) {
    if (typeof value === 'undefined' || value === null) {
      value = ''
    }
    this[kExec](appendPutCmd([], key, value, this.location), callback)
  }

  _get (key, options, callback) {
    const complete = (err, value) => {
      if (err) {
        return callback(err)
      }

      // redis returns null for non-existent keys
      if (value === null) {
        return callback(new ModuleError('NotFound', { code: 'LEVEL_NOT_FOUND' }))
      }

      callback(null, value)
    }

    const client = this[kClient]
    const rkey = this.location + ':h'

    if (isBufferEncoding(options.valueEncoding)) {
      client.hgetBuffer(rkey, key, complete)
    } else {
      client.hget(rkey, key, complete)
    }
  }

  _getMany (keys, options, callback) {
    const db = this[kClient]
    const rkey = this.location + ':h'

    const complete = (err, values) => {
      if (err) {
        return callback(err)
      }

      const result = new Array(keys.length)
      for (let i = 0; i < keys.length; i++) {
        const value = values[i]
        result[i] = (value === null) ? undefined : value
      }
      callback(null, result)
    }

    if (isBufferEncoding(options.valueEncoding)) {
      db.hmgetBuffer(rkey, keys, complete)
    } else {
      db.hmget(rkey, keys, complete)
    }
  }

  _del (key, options, callback) {
    this[kExec](appendDelCmd([], key, this.location), callback)
  }

  _batch (operationArray, options, callback) {
    const commandList = []
    const prefix = this.location
    for (let i = 0; i < operationArray.length; i++) {
      const operation = operationArray[i]
      if (operation.type === 'put') {
        appendPutCmd(commandList, operation.key, operation.value, prefix)
      } else if (operation.type === 'del') {
        appendDelCmd(commandList, operation.key, prefix)
      } else {
        const error = new ModuleError('Unknown type of operation ' + JSON.stringify(operation), { code: 'LEVEL_NOT_SUPPORTED' })
        return this.nextTick(callback, error)
      }
    }
    this[kExec](commandList, callback)
  }

  _clear (options, callback) {
    const commandList = []
    if (options.limit === -1 && !Object.keys(options).some(isRangeOption)) {
      commandList.push(['del', this.location + ':h'])
      commandList.push(['del', this.location + ':z'])
      // Delete everything.
      return this[kExec](commandList, callback)
    }

    const iterator = this._keys({ ...options })
    let limit = iterator.limit

    const batchSize = options.batchSize || iterator[kHighWaterMark]
    if (Number.isNaN(batchSize)) {
      const error = new TypeError('batchSize must be a number')
      return this.nextTick(callback, error)
    }

    const loop = () => {
      iterator.nextv(batchSize, options, (err, values) => {
        if (err) {
          return iterator.end(err, callback)
        }
        values.forEach(key => {
          appendDelCmd(commandList, key, this.location)
        })
        limit = limit - values.length
        this[kExec](commandList, (err) => {
          if (err) {
            iterator.end(() => {
              callback()
            })
          }
          if (limit > 0 && !iterator[kDone]) {
            this.nextTick(loop)
          }
        })
      })

      // Some time to breathe
      this.nextTick(loop)
    }

    this.nextTick(loop)
  }

  _iterator (options) {
    return new RedisIterator(this, options)
  }

  _keys (options) {
    return new RedisKeyIterator(this, options)
  }

  _values (options) {
    return new RedisValueIterator(this, options)
  }

  destroy (doClose, callback) {
    if (!callback && typeof doClose === 'function') {
      callback = doClose
      doClose = true
    }

    const client = this[kClient]
    client.del(this.location + ':h', this.location + ':z', (e) => {
      if (doClose) {
        this.close()
          .then(() => callback())
          .catch(e => callback(e))
      } else {
        this.nextTick(callback)
      }
    })
  }

  // for testing
  get client () {
    return this[kClient]
  }

  static reset (callback) {
    connections.forEach(v => {
      try {
        v.client.disconnect()
      } catch (x) {
      }
    })
    connections.clear()
    if (callback) {
      return callback()
    }
  }

  static get connectionCount () {
    return connections.size
  }
}

RedisLevel.prototype[kExec] = function (commandList, callback) {
  this[kClient].multi(commandList).exec(callback)
}

exports.RedisLevel = RedisLevel

// Use setImmediate() in Node.js to allow IO in between our callbacks
if (typeof process !== 'undefined' && !process.browser && typeof global !== 'undefined' && typeof global.setImmediate === 'function') {
  const setImmediate = global.setImmediate

  // Automatically applies to iterators, sublevels and chained batches as well
  RedisLevel.prototype.nextTick = function (fn, ...args) {
    if (args.length === 0) {
      setImmediate(fn)
    } else {
      setImmediate(() => fn(...args))
    }
  }
}

function isRangeOption (k) {
  return rangeOptions.has(k)
}

function processRangeOptions (options) {
  const reverse = !!options.reverse
  let start, end
  let exclusiveStart = false
  let exclusiveEnd = false

  if (options.gt !== undefined) {
    if (!reverse) {
      exclusiveStart = true
      start = options.gt
    } else {
      exclusiveEnd = true
      end = options.gt
    }
  } else if (options.gte !== undefined) {
    if (!reverse) {
      start = options.gte
    } else {
      end = options.gte
    }
  }
  if (options.lt !== undefined) {
    if (!reverse) {
      exclusiveEnd = true
      end = options.lt
    } else {
      exclusiveStart = true
      start = options.lt
    }
  } else if (options.lte !== undefined) {
    if (!reverse) {
      end = options.lte
    } else {
      start = options.lte
    }
  }

  start = !start ? (reverse ? '+' : '-') : (concatKey((exclusiveStart ? '(' : '['), start))
  end = !end ? (reverse ? '-' : '+') : (concatKey((exclusiveEnd ? '(' : '['), end))

  return { start, end }
}

function appendPutCmd (commandList, key, value, prefix) {
  commandList.push(['hset', prefix + ':h', key, value === undefined ? '' : value])
  commandList.push(['zadd', prefix + ':z', 0, key])
  return commandList
}

function appendDelCmd (commandList, key, prefix) {
  commandList.push(['hdel', prefix + ':h', key])
  commandList.push(['zrem', prefix + ':z', key])
  return commandList
}

/**
 * Internal: generate the options for redis.
 * create an identifier for a redis client from the options passed to _open.
 * when the identifier is identical, it is safe to reuse the same client.
 */
function _getBaseOptions (location, options) {
  const redisIdOptions = [
    'host', 'port', 'db', 'family', 'keyPrefix', 'tls', 'dropBufferSupport',
    'enableOfflineQueue', 'path', 'connectTimeout', 'reconnectOnError'
  ]
  let opts = { }
  if (typeof options === 'string') {
    const parsed = parseURL(options.connection)
    if (parsed.auth) {
      parsed.auth_pass = parsed.auth.split(':')[1]
    }
    opts = { ...parsed }
  } else {
    opts = { ...options }
  }

  const redisOptions = {}
  redisIdOptions.forEach(function (opt) {
    if (opts[opt] !== undefined && opts[opt] !== null) {
      redisOptions[opt] = opts[opt]
    }
  })

  return redisOptions
}

function sanitizeLocation (location) {
  if (!location) {
    return 'rl'
  }
  if (location.indexOf('://') > 0) {
    const url = parseURL(location)
    location = url.hostname || 'rd'
  }
  if (location.charAt(0) === '/') {
    return location.substring(1)
  }
  // Keep the hash delimited by curly brackets safe
  // as it is used by redis-cluster to force the selection of a slot.
  if (location.indexOf('%7B') === 0 && location.indexOf('%7D') > 0) {
    location = location.replace('%7B', '{').replace('%7D', '}')
  }
  return location
}

function isBufferEncoding (encoding) {
  return ['buffer', 'view', 'binary'].indexOf(encoding) !== -1
}

function hash (data) {
  const source = objToString(data)
  return crypto.createHash('sha256').update(source).digest('hex')
}

function objToString (hash) {
  if (typeof hash !== 'object') {
    return '' + hash
  }
  const keys = Object.keys(hash).sort()
  const parts = keys.map((key) => {
    const val = hash[key]
    return key + ':' + objToString(val)
  })
  return parts.join('')
}

function concatKey (prefix, key, force) {
  if (typeof key === 'string' && (force || key.length)) {
    return prefix + key
  }
  if (Buffer.isBuffer(key) && (force || key.length)) {
    return Buffer.concat([Buffer.from(prefix), key])
  }
  return key
}
