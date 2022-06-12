const Leveljs = require('../')
const test = require('tape')
const suite = require('abstract-level/test')
const tempy = require('tempy')

suite({
  test,
  factory: function () {
    return new Leveljs(tempy.directory())
  },
  // Opt-out of unsupported features
  errorIfExists: false,
  snapshots: false,
  seek: false
})

const testCommon = require('./testCommon')

/** * redis client management */
require('./redis-client-test').all(Leveljs, test, testCommon)
