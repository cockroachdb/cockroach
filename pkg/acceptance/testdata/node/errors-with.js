const assert = require('assert');
const errorCodes = require('pg-error-codes');

function errCode(code) {
  return errorCodes[code] || 'unknown error code';
}

// errorsWith takes a promise produced from a pg query and rejects if it
// fails with a superstring of an asserted error message and a specified
// Postgres error code.
function errorsWith(promise, { msg, code }) {
  return promise.then(
    () => {
      assert.fail(`expected failure with message "${msg}" and Postgres error code ${pgCode}, but no failure occurred`);
    },
    err => {
      assert(err.toString().indexOf(msg) > -1, `expected "${err.toString()}" to contain "${msg}"`);
      assert.equal(code, err.code, `expected error code to be ${code} (${errCode(code)}), was ${err.code} (${errCode(err.code)})`);
    }
  );
}

module.exports = errorsWith;
