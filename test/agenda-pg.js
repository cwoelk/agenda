var pg = require('pg'),
  Client = pg.Client,
  Pool = pg.Pool;

var expect = require('expect.js'),
    path = require('path'),
    moment = require('moment-timezone'),
    cp = require('child_process'),
    Agenda = require( path.join('..', 'index.js') ),
    Job = require( path.join('..', 'lib', 'job.js') );


var dbConfig =  {
  user: 'postgresql',
  database: 'agenda-test',
  port: 5432,
};

// Slow timeouts for travis
var jobTimeout = process.env.TRAVIS ? 1500 : 300;

function clearJobs(done) {
  var client = new Client(dbConfig)
  client.connect();
  client.query('DROP DATABASE agendaJobs', function(error) {
    client.end();
    done(error);
  });
}

describe('agenda-pg', function() {
  this.timeout(5000);
  var jobs;

  afterEach(function(done) {
    setTimeout(function() {
      clearJobs(function() {
        jobs._dbAdapter.close(done);
      });
    }, 50);
  });

  describe('Agenda', function() {
    describe('configuration methods', function() {
      it('sets the _db when passed as an option', function() {
        jobs = new Agenda({ pg: dbConfig });
        expect(jobs._dbAdapter.databaseName()).to.equal('agenda-test');
      });

      describe('pg', function() {
        describe('Client', function() {
          var pgClient

          beforeEach(function() {
            pgClient = new Client(dbConfig);
            pgClient.on('drain', pgClient.end.bind(pgClient));
          });

          it('sets the _db directly', function(done) {
            jobs = new Agenda();
            jobs.pg(pgClient, function() {
              expect(jobs._dbAdapter.databaseName()).to.equal('agenda-test');
              done();
            });
          });

          it('returns itself', function(done) {
            var pgAgendaInstance;
            jobs = new Agenda();
            pgAgendaInstance = jobs.pg(pgClient, function() {
              expect(pgAgendaInstance).to.be(jobs);
              done();
            });
          });
        });

        describe('Pool', function() {
          var pgPool;

          beforeEach(function(done) {
            pgPool = new Pool(dbConfig);
            pgPool.connect().then(function(client) {
              done();
            }).catch(done);
          });

          it('sets the _db directly', function(done) {
            jobs = new Agenda();
            jobs.pg(pgPool, function() {
              expect(jobs._dbAdapter.databaseName()).to.equal('agenda-test');
              done();
            });
          });

          it('returns itself', function(done) {
            var pgAgendaInstance;
            jobs = new Agenda();
            pgAgendaInstance = jobs.pg(pgPool, function() {
              expect(pgAgendaInstance).to.be(jobs);
              done();
            });
          });
        });
      });
    });

    describe('job methods', function() {
      beforeEach(function(done) {
        jobs = new Agenda({ pg: dbConfig }, done);
      });

      describe('create', function() {
        var job;
        beforeEach(function() {
          job = jobs.create('sendEmail', {to: 'some guy'});
        });

        it('returns a job', function() {
          expect(job).to.be.a(Job);
        });
        it('sets the name', function() {
          expect(job.attrs.name).to.be('sendEmail');
        });
        it('sets the type', function() {
          expect(job.attrs.type).to.be('normal');
        });
        it('sets the agenda', function() {
          expect(job.agenda).to.be(jobs);
        });
        it('sets the data', function() {
          expect(job.attrs.data).to.have.property('to', 'some guy');
        });
      });

      describe('every', function() {
        describe('with a job name specified', function() {
          it('returns a job', function() {
            expect(jobs.every('5 minutes', 'send email')).to.be.a(Job);
          });

          it('sets the repeatEvery', function() {
            expect(jobs.every('5 seconds', 'send email').attrs.repeatInterval).to.be('5 seconds');
          });

          it('sets the agenda', function() {
            expect(jobs.every('5 seconds', 'send email').agenda).to.be(jobs);
          });

          it('should update a job that was previously scheduled with `every`', function(done) {
            jobs.every(10, 'shouldBeSingleJob');
            setTimeout(function() {
              jobs.every(20, 'shouldBeSingleJob');
            }, 10);

            // Give the saves a little time to propagate
            setTimeout(function() {
              jobs.jobs({name: 'shouldBeSingleJob'}, function(err, res) {
                expect(res).to.have.length(1);
                done();
              });
            }, jobTimeout);

          });
        });

        describe('with array of names specified', function() {
          it('returns array of jobs', function() {
            expect(jobs.every('5 minutes', ['send email', 'some job'])).to.be.an('array');
          });
        });
      });
    });
  });
});
