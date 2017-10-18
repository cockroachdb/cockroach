const pg = require('pg');
var fs = require('fs');

// If we are running within the acceptance tests, we should use the env vars
// they've set. Otherwise, assume that we just want to run against a local
// Cockroach instance.

let config;
if (process.env.hasOwnProperty('PGSSLCERT')) {
  config = {
    user: 'root',
    host: process.env.PGHOST,
    port: process.env.PGPORT,
    ssl: {
      cert: fs.readFileSync(process.env.PGSSLCERT),
      key:  fs.readFileSync(process.env.PGSSLKEY),
    }
  };
} else {
  config = {
    user: 'root',
    host: 'localhost',
    port: 26257,
  };
}

let client = new pg.Client(config);
client.connect();

before(() => {
  client.query('DROP DATABASE IF EXISTS node_test');
});

beforeEach(() => {
  client.query('CREATE DATABASE node_test');
  client.query('USE node_test');
});

afterEach(() => {
  // We don't care if these error.
  client.query('DROP DATABASE IF EXISTS node_test').catch(() => {});
  client.query('USE ""').catch(() => {});
});

after(() => {
  client.end();
});

module.exports = client;
