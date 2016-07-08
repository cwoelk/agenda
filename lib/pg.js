var pg = require('pg');

var Postgres = module.exports = function(config, cb) {
  var self = this;
  var pgConfig = config.pg;
  var table = config.table;
  var mdb = new pg.Pool(pgConfig);

  mdb.connect().then(function(client) {
    client.release();
    self.pg(mdb, table, cb);
  }).catch(function(error) {
    handleError(error, cb);
  });
};

// Configuration Methods

Postgres.prototype.pg = function(mdb, table, cb) {
  this._mdb = mdb;
  this.db_init(table, cb);
  return this;
};

/** Connect to the spec'd MongoDB server and database.
 *  Notes:
 *    - If `url` inludes auth details then `options` must specify: { 'uri_decode_auth': true }. This does Auth on the specified
 *      database, not the Admin database. If you are using Auth on the Admin DB and not on the Agenda DB, then you need to
 *      authenticate against the Admin DB and then pass the MongoDB instance in to the Constructor or use Agenda.mongo().
 *    - If your app already has a MongoDB connection then use that. ie. specify config.mongo in the Constructor or use Agenda.mongo().
 */



/** Setup and initialize the collection used to manage Jobs.
 *  @param collection collection name or undefined for default 'agendaJobs'
 *  NF 20/04/2015
 */
Postgres.prototype.db_init = function(table, cb) {
  this._mdb.connect().then(function(client) {
    createTables(client, table, function(error) {
      if (error) throw error;

      cb();
    });
  }).catch(function(error) {
    handleError(error, cb);
  });
};

/** Find all Jobs matching `query` and pass same back in cb().
 *  refactored. NF 21/04/2015
 */
Postgres.prototype.jobs = function(query, cb) {
};

/** Cancels any jobs matching the passed mongodb query, and removes them from the database.
 *  @param query mongo db query
 *  @param cb callback( error, numRemoved )
 *
 *  @caller client code, Agenda.purge(), Job.remove()
 */
Postgres.prototype.deleteJobs = function(query, cb) {

};

Postgres.prototype.saveJob = function(job, lastModifiedBy, cb) {
};

/**
 * Find and lock jobs
 * @param {String} jobName
 * @param {Function} cb
 * @protected
 *  @caller jobQueueFilling() only
 */
Postgres.prototype._findAndLockNextJob = function(jobName, definition, nextScanAt, cb) {

};

// Refactored to Agenda method. NF 22/04/2015
// @caller Agenda.stop() only. Could be moved into stop(). NF
Postgres.prototype._unlockJobs = function(lockedJobIds, done) {

};

Postgres.prototype.findUnlockedJob = function(job, cb) {

};

Postgres.prototype.close = function(done) {

}

Postgres.prototype.databaseName = function() {
  return this._mdb.databaseName;
}

// Helper functions

function handleError(error, cb) {
  if (error) {
    if (cb) {
      cb(error, null);
    } else {
      throw error;
    }
    return;
  }
}

function createTables(client, table, cb) {
  table =  table || 'agendaJobs';
  client.query('BEGIN');
  var query = 'CREATE TABLE IF NOT EXISTS ';
  query = query.concat(table);
  query = query.concat(' ( ');
  query = query.concat('id SERIAL PRIMARY KEY,');
  query = query.concat('name VARCHAR(1024),');
  query = query.concat('data TEXT,');
  query = query.concat('type VARCHAR(100),');
  query = query.concat('disabled BOOLEAN,');
  query = query.concat('priority INTEGER,');
  query = query.concat('nextRunAt TIMESTAMP,');
  query = query.concat('lockedAt TIMESTAMP,');
  query = query.concat('lastModifiedBy TEXT');
  query = query.concat(' ); ');
  client.query(query);

  createIndexes(client, table, function(error) {
    if (error && cb) return cb(error);
    finishTransaction(client, cb);
  });
}

function createIndexes(client, table, cb) {
  var query = 'SELECT subquery.name FROM(';
  query = query.concat('SELECT c.relname as name ');
  query = query.concat('FROM pg_catalog.pg_class c ');
  query = query.concat('JOIN pg_catalog.pg_index i ON i.indexrelid = c.oid ');
  query = query.concat('JOIN pg_catalog.pg_class c2 ON i.indrelid = c2.oid ');
  query = query.concat('LEFT JOIN pg_catalog.pg_user u ON u.usesysid = c.relowner ');
  query = query.concat('LEFT JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace ');
  query = query.concat('WHERE c.relkind IN (\'i\',\'\') ');
  query = query.concat('AND n.nspname NOT IN (\'pg_catalog\', \'pg_toast\') ');
  query = query.concat('AND pg_catalog.pg_table_is_visible(c.oid) ) subquery ');
  query = query.concat('WHERE subquery.name like \'findandlocknextjobindex%\';');
  client.query(query, function(error, result) {
      if (error && cb) return cb(error);
      if (result.rowCount  < 1) {
        query = 'CREATE INDEX findAndLockNextJobIndex1 ON ';
        query = query.concat(table);
        query = query.concat('(name, priority DESC, lockedAt, nextRunAt, disabled); ');
        client.query(query, cb);
      } else cb();
  });
}

function finishTransaction(client, cb) {
  client.query('COMMIT', function(error, qry) {
    client.release();

    if (error && cb) return cb(error);

    cb();
  });
}
