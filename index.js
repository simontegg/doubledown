var util = require('util')
var AWS = require('aws-sdk')
var async = require('async')
var ALD = require('abstract-leveldown').AbstractLevelDOWN

var Iterator = require('./iterator')

AWS.config.apiVersions = { dynamodb: '2012-08-10' }

function DoubleDown (location) {
  if (!(this instanceof DoubleDown)) {
    return new DoubleDown(location)
  }

  var tableHash = location.split('/')
  this.tableName = tableHash[0]
  this.hashKey = tableHash[1] || '!'

  ALD.call(this, location)
}

util.inherits(DoubleDown, ALD)

DoubleDown.prototype._open = function (options, cb) {
  if (!options.dynamo) {
    var msg = 'DB open requires options argument with "dynamo" key.'
    return cb(new Error(msg))
  }

  var dynOpts = options.dynamo
  dynOpts.tableName = this.tableName

  this.ddb = new AWS.DynamoDB(dynOpts)
  this.cache = options.cache

  if (options.createIfMissing) {
    return this.createTable(options.dynamo, (err, data) => {
      var exists = err && (err.code === 'ResourceInUseException')

      if (options.errorIfExists && exists) {
        return cb(err)
      }

      if (err && !exists) {
        return cb(err)
      }

      return cb(null, self)
    })
  }

  setImmediate(() => {
    return cb(null, this)
  })
}

DoubleDown.prototype._close = function (cb) {
  this.ddb = null
  this.cache = null

  setImmediate(() => {
    return cb(null)
  })
}

DoubleDown.prototype._put = function (key, value, options, cb) {
  var params = {
    TableName: this.tableName,
    Item: {
      hkey: { S: this.hashKey },
      rkey: { S: key },
      value: {
        S: value.toString()
      }
    }
  }

  this.ddb.putItem(params, err => {
    if (err) {
      return cb(err)
    }

    if (this.cache) {
      return this.cache.put(key, value, cb)
    }

    return cb()
  })
}

DoubleDown.prototype._get = function (key, options, cb) {
  var params = {
    TableName: this.tableName,
    Key: {
      hkey: { S: this.hashKey },
      rkey: { S: key }
    }
  }

  this.ddb.getItem(params, (err, data) => {
    if (err) return cb(err)
    if (data && data.Item && data.Item.value) {
      var value = data.Item.value.S
      var val = options.asBuffer ? new Buffer(value) : value

      // write to cache
      if (this.cache) {
        return this.cache.put(key, value, options, err => {
          if (err) return cb(err)
          return cb(null, val)
        })
      }

      return cb(null, val)
    } else {
      return cb(new Error('NotFound'))
    }
  })
}

DoubleDown.prototype._del = function (key, options, cb) {
  var params = {
    TableName: this.tableName,
    Key: {
      hkey: { S: this.hashKey },
      rkey: { S: key }
    }
  }

  this.ddb.deleteItem(params, (err, data) => {
    if (err) {
      return cb(err)
    }

    if (this.cache) {
      return this.cache.del(key, options, cb)
    }

    cb(null, data)
  })
}

DoubleDown.prototype._batch = function (array, options, cb) {
  var opKeys = {}
  var multipleOpError = false

  tableName = this.tableName
  var ops = array.map(item => {
    if (opKeys[item.key]) {
      multipleOpError = true
    }

    opKeys[item.key] = true

    return item.type === 'del' 
      ? {
          DeleteRequest: {
            Key: {
              hkey: { S: self.hashKey },
              rkey: { S: item.key }
            }
          }
        }
      : {
          PutRequest: {
            Item: {
              hkey: { S: self.hashKey },
              rkey: { S: item.key },
              value: {
                S: item.value.toString()
              }
            }
          }
        }
  })

  if (multipleOpError) {
    return cb(new Error('Cannot perform multiple operations on the same item in a batch (DynamoDB limitation)'))
  }

  var params = { RequestItems: {} }

  var loop = (err, data) => {
    if (err) {
      return cb(err)
    }

    var reqs = []

    if (data && data.UnprocessedItems && data.UnprocessedItems[tableName]) {
      reqs.push.apply(reqs, data.UnprocessedItems[tableName])
    }

    reqs.push.apply(reqs, ops.splice(0, 25 - reqs.length))

    if (reqs.length === 0) {
      if (this.cache) {
        return this.cache.batch(array, options, cb)
      }

      return cb()
    }

    params.RequestItems[tableName] = reqs
    this.ddb.batchWriteItem(params, loop)
  }

  loop()
}

DoubleDown.prototype._iterator = function (options) {
  return new Iterator(this, options)
}

DoubleDown.prototype.createTable = function (opts, cb) {
  var params = Object.assign({
    TableName: this.tableName,
    AttributeDefinitions: [
      {
        AttributeName: "hkey",
        AttributeType: "S"
      },
      {
        AttributeName: "rkey",
        AttributeType: "S"
      }
    ],
    KeySchema: [
      {
        AttributeName: "hkey",
        KeyType: "HASH"
      },
      {
        AttributeName: "rkey",
        KeyType: "RANGE"
      }
    ],
    // overwrite by setting in opts
    ProvisionedThroughput: {
      ReadCapacityUnits: 1,
      WriteCapacityUnits: 1
    }
  }, opts)

  delete params.region
  delete params.secretAccessKey
  delete params.accessKeyId

  this.ddb.createTable(params, cb)
}
