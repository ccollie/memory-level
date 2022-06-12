const Redis = require('ioredis')
const { RedisLevel } = require('../')

const defaultHighWaterMark = RedisLevel.defaultHighWaterMark

let dbidx = 0

const location = function () {
  return '_RedisLevel_test_db_:' + dbidx++
}

const lastLocation = function () {
  return '_RedisLevel_test_db_:' + dbidx
}

const cleanup = function (callback) {
  RedisLevel.reset()
  const client = new Redis()
  client.del('_RedisLevel_test_db_', function (e) {
    client.disconnect()
    RedisLevel.reset(callback)
  })
}

const setUp = function (t) {
  cleanup(function (err) {
    t.notOk(err, 'cleanup returned an error')
    t.end()
  })
}
const setUpSmallBatches = function (t) {
  RedisLevel.defaultHighWaterMark = 3
  cleanup(function (err) {
    t.notOk(err, 'cleanup returned an error')
    t.end()
  })
}

const tearDown = function (t) {
  RedisLevel.defaultHighWaterMark = defaultHighWaterMark
  setUp(t) // same cleanup!
}

const collectEntries = function (iterator, callback) {
  const data = []
  const next = function () {
    iterator.next(function (err, key, value) {
      if (err) return callback(err)
      if (!arguments.length) {
        return iterator.end(function (err) {
          callback(err, data)
        })
      }
      data.push({ key, value })
      process.nextTick(next)
    })
  }
  next()
}

module.exports = {
  location,
  cleanup,
  lastLocation,
  setUp,
  tearDown,
  collectEntries,
  smallBatches: {
    location,
    cleanup,
    lastLocation,
    setUp: setUpSmallBatches,
    tearDown,
    collectEntries
  }
}
