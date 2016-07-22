var pg = require('pg'),
  squel = require("squel"),
  Client = pg.Client,
  Pool = pg.Pool;

var Postgres = module.exports = function(config, cb) {
  if (config && config.pg && Object.keys(config.pg).length) {
    var self = this;
    var pgConfig = config.pg;
    var table = config.table;
    var mdb = new Pool(pgConfig);

    this._mdb = mdb;
    mdb.connect().then(function(client) {
      self._disconnect(client);
      self.pg(mdb, table, cb);
    }).catch(function(error) {
      handleError(error, cb);
    });
  } else if (cb) cb();
  return this;
};

// Configuration Methods
Postgres.prototype.pg = function(mdb, table, cb) {
  this._mdb = mdb;
  if (typeof table === 'function') {
    cb = table;
    table = null;
  }
  this.db_init(table, cb);
  return this;
};

/** Setup and initialize the collection used to manage Jobs.
 *  @param collection collection name or undefined for default 'agendaJobs'
 *  NF 20/04/2015
 */
Postgres.prototype.db_init = function(table, cb) {
  var self = this;
  table =  table || 'agendaJobs';
  this._table = table;
  this._connect(function(error, client) {
    if (error) return handleError(error, cb);

    createTables(client, table, function(error) {
      self._disconnect(client);
      if (error) return handleError(error, cb);
      cb();
    });
  });
};

/** Find all Jobs matching `query` and pass same back in cb().
 *  refactored. NF 21/04/2015
 */
Postgres.prototype.jobs = function(query, cb) {
  var self = this;

  this._connect(function(error, client) {
    if (error) return handleError(error, cb);
    var query = squel.select().from(self._table).toString();

    client.query(query, function(error, res) {
      if (error) return handleError(error, cb);

      cb(null, sanitizeJobs(res.rows));
    });
  })
};

/** Cancels any jobs matching the passed mongodb query, and removes them from the database.
 *  @param query mongo db query
 *  @param cb callback( error, numRemoved )
 *
 *  @caller client code, Agenda.purge(), Job.remove()
 */
Postgres.prototype.deleteJobs = function(where, cb) {
  var self = this;
  this._connect(function(error, client) {
    if (error) return handleError(error);

    var query;

    if (where) {
      query = squel.delete().from(self._table).where(where).toString();
    } else {
      query = 'TRUNCATE TABLE '.concat(self._table);
    }

    client.query(query, cb);
  });
};

Postgres.prototype.saveJob = function(job, lastModifiedBy, cb) {
  var self = this;
  var now = new Date();

  this._connect(function(error, client) {
    if (error) return handleError(error, cb);

    var whereExpr = squel.expr();

    var props = job.toJSON();
    var id = job.attrs._id;
    var unique = job.attrs.unique;
    var uniqueOpts = job.attrs.uniqueOpts;

    if (id) whereExpr = whereExpr.and('id = ?', id);
    if (props.name) whereExpr = whereExpr.and('name = ?', props.name);

    if (props.type == 'single') {
      whereExpr = whereExpr.and('type = ?', props.type);
    } else if (unique) {
      // TODO: query logic for unique
      console.log('UNIQUE')
    }

    var query = squel.select().from(self._table).where(whereExpr).toString();
    client.query(query, function(error, result) {
      if (error) {
        self._disconnect(client);
        return handleError(error, cb)
      }

      callback = function(err, jobs) {
        self._disconnect(client);
        cb(err, jobs);
      };

      if (result.rowCount  > 0) {
        props.where = whereExpr.toString()
        self._updateJob(client, props, callback);
      } else {
        self._insertJob(client, props, callback);
      }
    });
  })
};

Postgres.prototype._insertJob = function(client, props, cb) {
  var self = this;
  var query = squel.insert()
    .into(this._table)
    .set("name", props.name)
    .set("type", props.type)
    .set("disabled", props.disabled || false)
    .set("data", JSON.stringify(props.data || {}))
    .set("priority", props.priority || 0)
    .set("nextrunat", getTimestamp(props.nextRunAt))
    .set("lockedat", getTimestamp(props.lockedAt))
    .set("lastmodifiedby", props.lastModifiedBy || null)
    .set("repeatinterval", props.repeatInterval || null)
    .set("repeattimezone", props.repeatTimezone || null)
    .toString()
    .concat('RETURNING *');

  client.query(query, function(error, result) {
    if (error) {
      self._disconnect(client);
      return handleError(error, cb)
    }

    if (cb) cb(null, { value: sanitizeJobs(result.rows) });
  });
}

Postgres.prototype._updateJob = function(client, props, cb) {
  var count = 0;
  var self = this;
  var query = squel.update().table(this._table);

  if (props.type != undefined)  {
    count++; query = query.set("type", props.type);
  }
  if (props.name != undefined)  {
    count++; query = query.set("name", props.name);
  }
  if (props.disabled != undefined)  {
    count++; query = query.set("disabled", props.disabled);
  }
  if (props.data != undefined)  {
    count++; query = query.set("data", JSON.stringify(props.data));
  }
  if (props.priority != undefined)  {
    count++; query = query.set("priority", props.priority);
  }
  if (props.nextRunAt != undefined)  {
    count++; query = query.set("nextrunat", getTimestamp(props.nextRunAt));
  }
  if (props.lockedAt != undefined)  {
    count++; query = query.set("lockedat", getTimestamp(props.lockedAt));
  }
  if (props.lastModifiedBy != undefined)  {
    count++; query = query.set("lastmodifiedby", props.lastModifiedBy);
  }
  if (props.repeatInterval != undefined)  {
    count++; query = query.set("repeatinterval", props.repeatInterval);
  }
  if (props.repeatTimezone != undefined)  {
    count++; query = query.set("repeattimezone", props.repeatTimezone);
  }

  if (count > 0) {
    if (props.where) query.where(props.where);

    query = query.toString().concat('RETURNING *');

    client.query(query, function(error, result) {
      if (error) {
        self._disconnect(client);
        return handleError(error, cb)
      }

      if (cb) cb(null, { value: sanitizeJobs(result.rows) });
    });
  } else if (cb) cb();
}

/**
 * Find and lock jobs
 * @param {String} jobName
 * @param {Function} cb
 * @protected
 *  @caller jobQueueFilling() only
 */
Postgres.prototype._findAndLockNextJob = function(jobName, definition, nextScanAt, cb) {
  var self = this;

  this._connect(function(error, client) {
    if (error) {
      self._disconnect(client);
      return handleError(error, cb)
    }

    var now = new Date(),
      lockDeadline = new Date(Date.now().valueOf() - definition.lockLifetime);

    var where = squel.expr()
      .or(
        squel.expr()
          .and('name = ?', jobName)
          .and('lockedAt IS NULL')
          .and('nextrunat <= ?', getTimestamp(nextScanAt))
          .and('disabled != ?', true)
      ).or(
        squel.expr()
          .and('name = ?', jobName)
          .and('lockedAt <= ?', getTimestamp(lockDeadline))
          .and('disabled != ?', true)
      );

      var query = squel.update()
        .table(self._table)
        .set('lockedat', getTimestamp(now))
        .where(where)
        .toString()
        .concat(' RETURNING *');

      client.query(query, function(error, res) {
        if (error) {
          self._disconnect(client);
          return handleError(error, cb);
        }

        var jobs = sanitizeJobs(res.rows);
        cb(null, { value: jobs[0] });
      });
  });
};

// Refactored to Agenda method. NF 22/04/2015
// @caller Agenda.stop() only. Could be moved into stop(). NF
Postgres.prototype._unlockJobs = function(lockedJobIds, done) {
  var self = this;

  this._connect(function(error, client) {
    if (error) return handleError(error, done);

    var where = squel.expr();

    lockedJobIds.forEach(function(lockedJobId) {
      where = where.or('id = ?', lockedJobId);
    });

    var query = squel.update()
      .table(self._table)
      .set('lockedat = ?', null)
      .where(where)
      .toString();

    client.query(query, function(error, results) {
       self._disconnect(client);
       done();
    });
  });
};

Postgres.prototype.findUnlockedJob = function(job, cb) {
  var self = this;
  var now = new Date();

  this._connect(function(error, client) {
    if (error) return handleError(error, cb)

    var query = squel.update()
      .table(self._table)
      .set('lockedAt', getTimestamp(now))
      .where(
        squel.expr()
          .and('id = ?', job.attrs._id)
          .and('lockedat = ?', null)
          .and('nextrunat = ?', getTimestamp(job.attrs.nextRunAt))
          .and('disabled != ?', true)
      )
      .toString()
      .concat(' RETURNING *');
    client.query(query, function(error, result) {
      if (error) {
        self._disconnect(client);
        return handleError(error, cb)
      }

      if (cb) cb(null, { value: sanitizeJobs(result.rows)[0] });
    });
  })
};

Postgres.prototype.close = function(done) {
  this._mdb.end();
  return done();
}

Postgres.prototype.databaseName = function() {
  return this._mdb.database || this._mdb.pool._factory.database;
}

Postgres.prototype._connect = function(cb) {
  if (this._mdb instanceof Pool) {
    this._mdb.connect().then(function(client) {
      if (cb) cb(null, client);
    }).catch(cb);
  } else if (this._mdb instanceof Client) {
    var self = this;
    this._mdb.connect(function(error) {
      if (cb) cb(error, self._mdb);
    });
  } else {
    cb(new Error('Invalid postgres instance'));
  }
}

Postgres.prototype._disconnect = function(client) {
  if (client.release) {
    client.release();
  }
}

// Helper functions
function getTimestamp(date) {
  if (date)
    return Math.floor(date.getTime()/ 1000);
  return null;
}

function getDate(timestamp) {
  if (timestamp)
    return new Date(timestamp * 1000);
  return null;
}

function sanitizeJobs(baseJobs) {
  if (baseJobs.length) {
    return baseJobs.map(sanitizeJob)
  }
  return [];
}

function sanitizeJob(baseJob) {
  var job = {};

  if (baseJob.lockedat) job.lockedAt = getDate(baseJob.lockedat);
  if (baseJob.nextrunat) job.nextRunAt = getDate(baseJob.nextrunat);

  job.id = baseJob.id;
  job._id = baseJob.id;
  job.name = baseJob.name;
  job.repeatInterval = baseJob.repeatinterval;
  job.repeatTimezone = baseJob.repeattimezone;
  job.data = baseJob.data;
  job.type = baseJob.type;
  job.disabled = baseJob.disabled;
  job.priority = baseJob.priority;
  job.lastModifiedby = baseJob.lastmodifiedby;

  return job;
}

function handleError(error, cb) {
  console.error(error);
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
  var query = 'CREATE TABLE IF NOT EXISTS ';
  query = query.concat(table);
  query = query.concat(' ( ');
  query = query.concat('id SERIAL PRIMARY KEY,');
  query = query.concat('name VARCHAR(1024),');
  query = query.concat('repeatInterval VARCHAR(1024),');
  query = query.concat('repeatTimezone VARCHAR(1024),');
  query = query.concat('data TEXT,');
  query = query.concat('type VARCHAR(100),');
  query = query.concat('disabled BOOLEAN,');
  query = query.concat('priority INTEGER,');
  query = query.concat('nextRunAt BIGINT,');
  query = query.concat('lockedAt BIGINT,');
  query = query.concat('lastModifiedBy TEXT');
  query = query.concat(' ); ');

  client.query('BEGIN');
  client.query(query);
  createIndexes(client, table, function(error) {
    if (error) return handleError(error);

    client.query('COMMIT', cb);
  });
}

function createIndexes(client, table, cb) {
  var query = 'SELECT subquery.name FROM (';
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

  client.query(query,  function(error, result) {
    if (error) return handleError(error, cb);

    if (result.rowCount  < 1) {
      query = 'CREATE INDEX findAndLockNextJobIndex1 ON ';
      query = query.concat(table);
      query = query.concat('(name, priority DESC, lockedAt, nextRunAt, disabled); ');
      client.query(query, cb);
    } else cb();
  });
}
