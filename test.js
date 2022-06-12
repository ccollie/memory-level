'use strict'
const test = require('tape')
const suite = require('abstract-level/test')
const { RedisLevel } = require('.')
const { Buffer } = require('buffer')

let idx = 0

const location = function () {
  return '__RedisLevel_test_db_:' + idx++
}

// Test abstract-level compliance
suite({
  test,
  factory: (...args) => new RedisLevel(location(), { ...args, ownClient: true })
})

// Additional tests for this implementation of abstract-level
test('iterator does not clone buffers', function (t) {
  const db = new RedisLevel('redis-level', { keyEncoding: 'buffer', valueEncoding: 'buffer' })
  const buf = Buffer.from('a')

  db.open(function (err) {
    t.ifError(err, 'no open() error')

    db.put(buf, buf, function (err) {
      t.ifError(err, 'no put() error')

      db.iterator().all(function (err, entries) {
        t.ifError(err, 'no all() error')
        t.is(entries[0][0], buf, 'key is same buffer')
        t.is(entries[0][1], buf, 'value is same buffer')
        t.end()
      })
    })
  })
})
