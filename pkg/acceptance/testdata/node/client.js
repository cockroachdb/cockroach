const pg = require('pg');
const fs = require('fs');

// If we are running within the acceptance tests, we should use the env vars
// they've set. Otherwise, assume that we just want to run against a local
// Cockroach instance.
const config = {
  user: 'root',
  host: process.env.PGHOST || 'localhost',
  port: process.env.PGPORT || 26257,
};
if (process.env.PGSSLCERT && process.env.PGSSLKEY) {
  config.ssl = {
    cert: fs.readFileSync(process.env.PGSSLCERT),
    key:  fs.readFileSync(process.env.PGSSLKEY),
  };
}

const client = new pg.Client(config);

before(() => {
  client.connect();
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
