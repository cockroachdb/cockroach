// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

'use strict';
/* eslint no-var: 0 */

const _ = require('lodash');

console.log('Using custom cockroachdb test config');

const testIntegrationDialects = (
  process.env.DB ||
  'cockroachdb'
).match(/[\w-]+/g);

const testConfigs = {
  cockroachdb: {
    adapter: 'cockroachdb',
    port: process.env.PGPORT,
    host: 'localhost',
    database: 'test',
    user: process.env.PGUSER,
    password: process.env.PGPASSWORD,
    ssl: {
      rejectUnauthorized: false,
      ca: process.env.PGSSLROOTCERT
    }
  },
};

module.exports = _.reduce(
  testIntegrationDialects,
  function (res, dialectName) {
    res[dialectName] = testConfigs[dialectName];
    return res;
  },
  {}
);
