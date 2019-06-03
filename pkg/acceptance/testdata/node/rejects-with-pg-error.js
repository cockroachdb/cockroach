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
