// Copyright 2016 The Cockroach Authors.
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

package acceptance

import (
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/util/log"

	"golang.org/x/net/context"
)

func TestDockerNodeJS(t *testing.T) {
	s := log.Scope(t)
	defer s.Close(t)

	ctx := context.Background()
	testDockerSuccess(ctx, t, "node.js", []string{"node", "-e", strings.Replace(nodeJS, "%v", "3", 1)})
	testDockerFail(ctx, t, "node.js", []string{"node", "-e", strings.Replace(nodeJS, "%v", `'a'`, 1)})
}

const nodeJS = `
const fs     = require('fs');
const pg     = require('pg');
const assert = require('assert');

const config = {
  user: 'root',
  ssl: {
    cert: fs.readFileSync(process.env.PGSSLCERT),
    key:  fs.readFileSync(process.env.PGSSLKEY),
  }
};

const client = new pg.Client(config);

// Assert that the given error is not null and matches the given regex.
function isError(err, expected, pgCode) {
  if (!err) return new Error('expected non-null error');
  if (pgCode && err.code !== pgCode) return new Error('expected error to have code ' + pgCode + ', got ' + err.code);
  if (!expected.test(err.toString())) return new Error('expected error message to match ' + expected.toString() + ', but was ' + err.toString());
  return null;
}

function testSelect(client) {
  return new Promise(resolve => {
    client.query("SELECT 1 as first, 2+$1 as second, ARRAY['\"','',''] as third", [%v], function (err, results) {
      if (err) throw err;

      assert.deepEqual(results.rows, [{
        first: 1,
        second: 5,
        third: ['"', '', '']
      }]);

      resolve();
    });
  });
}

function testSendInvalidUTF8(client) {
  return new Promise(resolve => {
    client.query({
      text: 'SELECT $1::STRING',
      values: [new Buffer([167])]
    }, function (err, results) {
      let validationError = isError(err, /invalid UTF-8 sequence/, '22021');
      if (validationError) throw validationError;
      resolve();
    });
  });
}

function testSendNotEnoughParams(client) {
  return new Promise(resolve => {
    client.query({
      text: 'SELECT 3',
      values: ['foo']
    }, function (err, results) {
      let validationError = isError(err, /expected 0 arguments, got 1/, '08P01');
      if (validationError) throw validationError;
      resolve();
    });
  });
}

function testInsertArray(client) {
  return new Promise(resolve => {
    client.query({
      text: 'CREATE DATABASE d',
    }, (err) => {
      if (err) throw err;
      client.query({
        text: 'CREATE TABLE d.x (y FLOAT[])',
      }, (err) => {
        if (err) throw err;
        client.query({
          text: 'INSERT INTO d.x VALUES ($1)',
          values: [[1, 2, 3]],
        }, (err) => {
          if (err) throw err;
          resolve();
        });
      });
    });
  });
}

function runTests(client, ...tests) {
  if (tests.length === 0) {
    return Promise.resolve();
  } else {
    return tests[0](client).then(() => runTests(client, ...tests.slice(1)));
  }
}

client.connect(function (err) {
  if (err) throw err;

  runTests(client,
    testSendInvalidUTF8,
    testSendNotEnoughParams,
    testInsertArray,
    testSelect
  ).then(result => {
    client.end(function (err) {
      if (err) throw err;
    });
  }).catch(err => {
    client.end()
    throw err;
  });
});
`
