'use strict'
const { RedisLevel } = require('../index')
const Redis = require('ioredis')
let db

module.exports.setUp = function (redisLevel, test, testCommon) {
  test('setUp common', testCommon.setUp)
}

module.exports.args = function (redisLevel, test, testCommon) {
  test('test shared redis client for 2 redis-level instances', function (t) {
    t.equal(RedisLevel.connectionCount, 0)
    db = new RedisLevel(testCommon.location())
    db.open({}, function (e) {
      t.notOk(e, 'no error')
      t.equal(RedisLevel.connectionCount, 1)
      const db2 = new RedisLevel(testCommon.location())
      db2.open({}, function (e) {
        t.notOk(e, 'no error')
        db2.close(function (e) {
          t.notOk(e, 'no error')
          t.equal(RedisLevel.connectionCount, 1)
          const redis = db.client
          t.notEqual(redis.status, 'close')
          db.close(function (e) {
            t.notOk(e, 'no error')
            t.equal(redis.status, 'close')
            t.equal(RedisLevel.connectionCount, 0)
            t.end()
          })
        })
      })
    })
  })

  test('reuse a redis client directly', function (t) {
    t.equal(RedisLevel.connectionCount, 0)
    const redis = new Redis()
    db = new RedisLevel(testCommon.location())
    db.open({ connection: redis }, function (e) {
      t.notOk(e, 'no error')
      t.equal(RedisLevel.connectionCount, 0)
      t.equal(db.client, redis)
      db.close(function (e) {
        t.notOk(e, 'no error')
        // the redis client is not managed by redisLevel, so we don't quit
        t.notEqual(redis.status, 'close')
        redis.quit()
        t.end()
      })
    })
  })
  test('test destroy redis statically by name', function (t) {
    t.equal(RedisLevel.connectionCount, 0)
    const location = testCommon.location()
    db = new RedisLevel(location)
    db.open({}, function (e) {
      t.notOk(e, 'no error')
      redisLevel.destroy(location, function (e) {
        t.notOk(e, 'no error')
        t.end()
      })
    })
  })
}

module.exports.tearDown = function (test, testCommon) {
  test('tearDown', function (t) {
    db.close(testCommon.tearDown.bind(null, t))
  })
}

module.exports.all = function (redisLevel, test, testCommon) {
  module.exports.setUp(redisLevel, test, testCommon)
  module.exports.args(redisLevel, test, testCommon)
  module.exports.tearDown(test, testCommon)
}
