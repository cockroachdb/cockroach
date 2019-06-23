// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

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
    key: fs.readFileSync(process.env.PGSSLKEY),
  };
}

const client = new pg.Client(config);

before(done => {
  client
    .connect()
    .then(() => {
      return client.query('DROP DATABASE IF EXISTS node_test');
    })
    .then(() => {
      return client.query('CREATE DATABASE node_test');
    })
    .then(() => {
      return client.query('USE node_test');
    })
    .then(() => {
      done();
    });
});

after(() => {
  client.query('DROP DATABASE IF EXISTS node_test').catch(() => {});
  client.query('USE ""').catch(() => {});
  client.end();
});

module.exports = client;
