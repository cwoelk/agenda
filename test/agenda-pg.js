

var expect = require('expect.js'),
    path = require('path'),
    moment = require('moment-timezone'),
    cp = require('child_process'),
    Agenda = require( path.join('..', 'index.js') ),
    Job = require( path.join('..', 'lib', 'job.js') );

describe('agenda-pg', function() {
  var agenda;
  beforeEach(function(done) {
    var agenda = new Agenda({
      pg: {
        user: 'postgresql',
        database: 'test',
        port: 5432,
      }
    }, done);
  });

  it.only('stuff', function() {

  });
});
