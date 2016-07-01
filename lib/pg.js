
var pg = require('pg');

var Postgres = module.exports = function(config, cb) {
  var self = this;
  var pgConfig = config.pg;
  var table = config.table;
  var client = new pg.Client(pgConfig);

  client.connect(function(error) {
    if (error) {
      if (cb) {
        cb(error, null);
      } else {
        throw error;
      }
      return;
    }

    self._mdb = client;
    self.db_init(table, cb);
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
  table =  table || 'agendaJobs';
  if (cb) cb();
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
