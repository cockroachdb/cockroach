// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

const assert     = require('assert');
const errorCodes = require('pg-error-codes');

function errCode(code) {
  return errorCodes[code] || 'unknown error code';
}

// rejectsWithPGError takes a promise produced from a pg query and rejects if it
// fails with a superstring of an asserted error message and a specified
// Postgres error code.
function rejectsWithPGError(promise, { msg, code }) {
  return promise.then(
    () => {
      assert.fail(`expected failure with message "${msg}" and Postgres error code ${code}, but no failure occurred`);
    },
    err => {
      assert(err.toString().indexOf(msg) > -1, `expected "${err.toString()}" to contain "${msg}"`);
      assert.equal(code, err.code, `expected error code to be ${code} (${errCode(code)}), was ${err.code} (${errCode(err.code)})`);
    }
  );
}

module.exports = rejectsWithPGError;
