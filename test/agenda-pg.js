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
  client.query('TRUNCATE TABLE agendajobs', function(error, result) {
    client.end();
    done(error);
  });
}

describe('agenda-pg', function() {
  this.timeout(5000);
  var jobs;

  beforeEach(function(done) {
    jobs = new Agenda({ pg: dbConfig }, done);
  });

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
        describe ('Client', function() {
          var pgClient

          beforeEach(function() {
            pgClient = new Client(dbConfig);
          });

          afterEach(function() {
            pgClient.end();
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
              jobs.jobs('name= \'shouldBeSingleJob\'', function(err, res) {
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

      describe('schedule', function() {
        describe('with a job name specified', function() {
          it('returns a job', function() {
            expect(jobs.schedule('in 5 minutes', 'send email')).to.be.a(Job);
          });
          it('sets the schedule', function() {
            var fiveish = (new Date()).valueOf() + 250000;
            expect(jobs.schedule('in 5 minutes', 'send email').attrs.nextRunAt.valueOf()).to.be.greaterThan(fiveish);
          });
        });
        describe('with array of names specified', function() {
          it('returns array of jobs', function() {
            expect(jobs.schedule('5 minutes', ['send email', 'some job'])).to.be.an('array');
          });
        });
      });

      xdescribe('unique', function() {
        describe('should demonstrate unique contraint', function(done) {
          it('should modify one job when unique matches', function(done) {
            jobs.create('unique job', {type: 'active', userId: '123', 'other': true}).unique({'data.type': 'active', 'data.userId': '123'}).schedule("now").save(function(err, job1) {
              setTimeout(function() { // Avoid timing condition where nextRunAt coincidentally is the same
                jobs.create('unique job', {type: 'active', userId: '123', 'other': false}).unique({'data.type': 'active', 'data.userId': '123'}).schedule("now").save(function(err, job2) {
                  expect(job1.attrs.nextRunAt.toISOString()).not.to.equal(job2.attrs.nextRunAt.toISOString())
                  mongo.collection('agendaJobs').find({name: 'unique job'}).toArray(function(err, j) {
                    expect(j).to.have.length(1);
                    done();
                  });
                });
              }, 1);
            });
          });

          it('should not modify job when unique matches and insertOnly is set to true', function(done) {
            jobs.create('unique job', {type: 'active', userId: '123', 'other': true}).unique({'data.type': 'active', 'data.userId': '123'}, { insertOnly: true }).schedule("now").save(function(err, job1) {
              jobs.create('unique job', {type: 'active', userId: '123', 'other': false}).unique({'data.type': 'active', 'data.userId': '123'}, {insertOnly: true}).schedule("now").save(function(err, job2) {
                expect(job1.attrs.nextRunAt.toISOString()).to.equal(job2.attrs.nextRunAt.toISOString())
                mongo.collection('agendaJobs').find({name: 'unique job'}).toArray(function(err, j) {
                  expect(j).to.have.length(1);
                  done();
                });
              });
            });
          });
        });

        describe('should demonstrate non-unique contraint', function(done) {
          it('should create two jobs when unique doesn\t match', function(done) {
            var time = new Date(Date.now() + 1000*60*3);
            var time2 = new Date(Date.now() + 1000*60*4);

            jobs.create('unique job', {type: 'active', userId: '123', 'other': true}).unique({'data.type': 'active', 'data.userId': '123', nextRunAt: time}).schedule(time).save(function(err, job) {
             jobs.create('unique job', {type: 'active', userId: '123', 'other': false}).unique({'data.type': 'active', 'data.userId': '123', nextRunAt: time2}).schedule(time).save(function(err, job) {
                mongo.collection('agendaJobs').find({name: 'unique job'}).toArray(function(err, j) {
                  expect(j).to.have.length(2);
                  done();
                });
             });
            });

          });
        });

      });

      describe('now', function() {
        it('returns a job', function() {
          expect(jobs.now('send email')).to.be.a(Job);
        });

        it('sets the schedule', function() {
          var now = new Date();
          expect(jobs.now('send email').attrs.nextRunAt.valueOf()).to.be.greaterThan(now.valueOf() - 1);
        });

        it('runs the job immediately', function(done) {
          jobs.define('immediateJob', function(job) {
            expect(job.isRunning()).to.be(true);
            jobs.stop(done);
          });
          jobs.now('immediateJob');
          jobs.start();
        });
      });

      describe('jobs', function() {
        it('returns jobs', function(done) {
          var job = jobs.create('test');
          job.save(function() {
            jobs.jobs(null, function(err, c) {
              expect(c.length).to.not.be(0);
              expect(c[0]).to.be.a(Job);
              clearJobs(done);
            });
          });
        });
      });

      describe('purge', function() {
        it('removes all jobs without definitions', function(done) {
          var job = jobs.create('no definition');
          jobs.stop(function() {
            job.save(function() {
              jobs.jobs('name= \'no definition\'', function(err, j) {
                if (err) return done(err);
                expect(j).to.have.length(1);

                jobs.purge(function(err) {
                  if (err) return done(err);

                  jobs.jobs('name= \'no definition\'', function(err, j) {
                    if (err) return done(err);

                    expect(j).to.have.length(0);
                    done();
                  });
                });
              });
            });
          });
        });
      });

      describe('saveJob', function() {
        it('persists job to the database', function(done) {
          var job = jobs.create('someJob', {});
          job.save(function(err, job) {
            expect(job.attrs._id).to.be.ok();

            clearJobs(done);
          });
        });
      });
    });

    describe('cancel', function() {
      beforeEach(function(done) {
        var remaining = 3;
        var checkDone = function(err, job) {
          if (err) return done(err);
          remaining--;
          if (!remaining) {
            done();
          }
        };
        jobs.create('jobA').save(checkDone);
        jobs.create('jobA', 'someData').save(checkDone);
        jobs.create('jobB').save(checkDone);
      });

      afterEach(function(done) {
        jobs._dbAdapter.deleteJobs('name IN (\'jobA\', \'jobB\')', function(err) {
          if (err) return done(err);
          done();
        });
      });

      it('should cancel a job', function(done) {
        var jobQuery = "name = 'jobA'";

        jobs.jobs(jobQuery, function(err, j) {
          if (err) return done(err);

          expect(j).to.have.length(2);
          jobs.cancel(jobQuery, function(err) {
            if (err) return done(err);

            jobs.jobs(jobQuery, function(err, j) {
              if (err) return done(err);
              expect(j).to.have.length(0);
              done();
            });
          });
        });
      });

      it('should cancel multiple jobs', function(done) {
        var multipleJobsQuery = "name IN ('jobA', 'jobB')";

        jobs.jobs(multipleJobsQuery, function(err, j) {
          if (err) return done(err);

          expect(j).to.have.length(3);
          jobs.cancel(multipleJobsQuery, function(err) {
            if (err) return done(err);

            jobs.jobs(multipleJobsQuery, function(err, j) {
              if (err) return done(err);

              expect(j).to.have.length(0);
              done();
            });
          });
        });
      });

      xit('should cancel jobs only if the data matches', function(done){
        var jobWithDataQuery = "name = 'jobA' AND data = 'someData'";

        jobs.jobs(jobWithDataQuery, function(err, j) {
          if (err) return done(err);

          expect(j).to.have.length(1);
          jobs.cancel(jobWithDataQuery, function(err) {
            if (err) return done(err);

            jobs.jobs(jobWithDataQuery, function(err, j) {
              if (err) return done(err);

              expect(j).to.have.length(0);
              jobs.jobs("name = 'jobA'", function(err, j) {
                if (err) return done(err);

                expect(j).to.have.length(1);
                done();
              });
            });
          });
        });
      });
    });
  });
});
