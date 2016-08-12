var log = require('./log')('mongo');

var MongoClient = require('mongodb').MongoClient,
    Db = require('mongodb').Db;

var Mongo = module.exports = function(config, cb) {
  if (config.mongo) {
    this.mongo(config.mongo, config.db ? config.db.collection : undefined, cb);
  } else if (config.db) {
    this.database(config.db.address, config.db.collection, config.db.options, cb);
  }
};

// Configuration Methods

Mongo.prototype.mongo = function( mdb, collection, cb ){
  this._mdb = mdb;
  this.db_init(collection, cb);   // NF 20/04/2015
  return this;
};

/** Connect to the spec'd MongoDB server and database.
 *  Notes:
 *    - If `url` inludes auth details then `options` must specify: { 'uri_decode_auth': true }. This does Auth on the specified
 *      database, not the Admin database. If you are using Auth on the Admin DB and not on the Agenda DB, then you need to
 *      authenticate against the Admin DB and then pass the MongoDB instance in to the Constructor or use Agenda.mongo().
 *    - If your app already has a MongoDB connection then use that. ie. specify config.mongo in the Constructor or use Agenda.mongo().
 */

Mongo.prototype.database = function(url, collection, options, cb) {
  if (!url.match(/^mongodb:\/\/.*/)) {
    url = 'mongodb://' + url;
  }

  collection = collection || 'agendaJobs';
  options = options || {};
  var self = this;

  MongoClient.connect(url, options, function(error, db) {
    if (error) {
      if (cb) {
        cb(error, null);
      } else {
        throw error;
      }

      return;
    }

    self._mdb = db;
    self.db_init( collection, cb );
  });
  return this;
};


/** Setup and initialize the collection used to manage Jobs.
 *  @param collection collection name or undefined for default 'agendaJobs'
 *  NF 20/04/2015
 */


Mongo.prototype.db_init = function( collection, cb ) {
  var self = this;

  this._collection = this._mdb.collection(collection || 'agendaJobs');
  this._collection.createIndexes([
    {
      "key": {"name" : 1, "priority" : -1, "lockedAt" : 1, "nextRunAt" : 1, "disabled" : 1},
      "name": "findAndLockNextJobIndex1"
    },
    {
      "key": {"name" : 1, "lockedAt" : 1, "priority" : -1, "nextRunAt" : 1, "disabled" : 1},
      "name": "findAndLockNextJobIndex2"
    }
  ], function( err, result ){
    handleLegacyCreateIndex(err, result, self, cb);
  });
};


function handleLegacyCreateIndex(err, result, self, cb){
  if(!err || err.message === 'no such cmd: createIndexes') {
    // Looks like a mongo.version < 2.4.x
    err = null;
    self._collection.ensureIndex(
        {"name": 1, "priority": -1, "lockedAt": 1, "nextRunAt": 1, "disabled": 1},
        {name: "findAndLockNextJobIndex1"}
    );
    self._collection.ensureIndex(
        {"name": 1, "lockedAt": 1, "priority": -1, "nextRunAt": 1, "disabled": 1},
        {name: "findAndLockNextJobIndex2"}
    );
  }
  if (cb){
    cb(err, self._collection);
  }
}


/** Find all Jobs matching `query` and pass same back in cb().
 *  refactored. NF 21/04/2015
 */

Mongo.prototype.jobs = function(query, cb) {
  log.trace('jobs', query);
  var self = this;
  this._collection.find(query).toArray(cb);
};

Mongo.prototype.deleteJobsWithIds = function(ids, cb) {
  var idsArray = Array.isArray(ids) ? ids : [ids];
  this.deleteJobs({ _id: { $in: idsArray } }, cb);
}

Mongo.prototype.deleteJobsExceptWithNames = function(names, cb) {
  if (!names) {
    this.deleteJobs(null, cb);
    return;
  }

  var namesArray = Array.isArray(names) ? names : [names];
  this.deleteJobs({ name: { $nin: namesArray } }, cb);
}

/** Cancels any jobs matching the passed mongodb query, and removes them from the database.
 *  @param query mongo db query
 *  @param cb callback( error, numRemoved )
 *
 *  @caller client code, Agenda.purge(), Job.remove()
 */

Mongo.prototype.deleteJobs = function(query, cb) {
  log.trace('deleteJobs', query);
  this._collection.deleteMany( query, function( error, result ){
    if (cb) {
      cb( error, result && result.result ? result.result.n : undefined );
    }
  });
};

Mongo.prototype.saveJob = function(job, lastModifiedBy, cb) {
  log.trace('saveJob', job.attrs);
  var props = job.toJSON();
  var id = job.attrs._id;
  var unique = job.attrs.unique;
  var uniqueOpts = job.attrs.uniqueOpts;

  delete props._id;
  delete props.unique;
  delete props.uniqueOpts;

  props.lastModifiedBy = lastModifiedBy;

  var now = new Date(),
      protect = {},
      update = { $set: props };

  if (id) {
    this._collection.findAndModify({_id: id}, {}, update, {new: true}, cb);
  } else if (props.type == 'single') {
    if (props.nextRunAt && props.nextRunAt <= now) {
      protect.nextRunAt = props.nextRunAt;
      delete props.nextRunAt;
    }
    if (Object.keys(protect).length > 0) {
      update.$setOnInsert = protect;
    }
    // Try an upsert.
    this._collection.findAndModify({name: props.name, type: 'single'}, {}, update, {upsert: true, new: true}, cb);
  } else if (unique) {
    var query = job.attrs.unique;
    query.name = props.name;
    if ( uniqueOpts && uniqueOpts.insertOnly )
      update = { $setOnInsert: props };
    this._collection.findAndModify(query, {}, update, {upsert: true, new: true}, cb);
  } else {
    this._collection.insertOne(props, cb);    // NF updated 22/04/2015
  }
};

/**
 * Find and lock jobs
 * @param {String} jobName
 * @param {Function} cb
 * @protected
 *  @caller jobQueueFilling() only
 */

Mongo.prototype._findAndLockNextJob = function(jobName, definition, nextScanAt, cb) {
  log.trace('_findAndLockNextJob', jobName);
  var self = this,
      now = new Date(),
      lockDeadline = new Date(Date.now().valueOf() - definition.lockLifetime);

  // Don't try and access Mongo Db if we've lost connection to it. Also see clibu_automation.js db.on.close code. NF 29/04/2015
  // Trying to resolve crash on Dev PC when it resumes from sleep.
  if (this._mdb.s.topology.connections().length === 0) {
    cb(new Error( 'No MongoDB Connection'));
  } else {
    this._collection.findAndModify(
      {
        $or: [
          {name: jobName, lockedAt: null, nextRunAt: {$lte: nextScanAt}, disabled: { $ne: true }},
          {name: jobName, lockedAt: {$exists: false}, nextRunAt: {$lte: nextScanAt}, disabled: { $ne: true }},
          {name: jobName, lockedAt: {$lte: lockDeadline}, disabled: { $ne: true }}
        ]
      },
      {'priority': -1},  // sort
      {$set: {lockedAt: now}},  // Doc
      {'new': true},  // options
      cb
    );
  }
};

// Refactored to Agenda method. NF 22/04/2015
// @caller Agenda.stop() only. Could be moved into stop(). NF
Mongo.prototype._unlockJobs = function(lockedJobIds, done) {
  log.trace('_unlockJobs', lockedJobIds);
  this._collection.updateMany(
    {_id: { $in: lockedJobIds } },
    { $set: { lockedAt: null } }, done);    // NF refactored .update() 22/04/2015
};

Mongo.prototype.findUnlockedJob = function(job, cb) {
  log.trace('findUnlockedJob', job.attrs);
  var now = new Date();
  var criteria = {
    _id: job.attrs._id,
    lockedAt: null,
    nextRunAt: job.attrs.nextRunAt,
    disabled: { $ne: true }
  };

  var sort = {};
  var update = { $set: { lockedAt: now } };
  var options = { new: true };

  this._collection.findAndModify(criteria, sort, update, options, cb);
};

Mongo.prototype.close = function(done) {
  this._mdb.close(done);
}

Mongo.prototype.databaseName = function() {
  return this._mdb.databaseName;
}
