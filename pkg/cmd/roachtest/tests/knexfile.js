'use strict';
/* eslint no-var: 0 */

const _ = require('lodash');

console.log(`Using custom cockroachdb test config`);

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
