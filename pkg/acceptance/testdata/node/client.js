// Copyright 2019 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

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
