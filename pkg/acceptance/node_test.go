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
//
// Author: Nathan VanBenschoten (nvanbenschoten@gmail.com)

package acceptance

import (
	"strings"
	"testing"
)

func TestDockerNodeJS(t *testing.T) {
	testDockerSuccess(t, "node.js", []string{"node", "-e", strings.Replace(nodeJS, "%v", "3", 1)})
	testDockerFail(t, "node.js", []string{"node", "-e", strings.Replace(nodeJS, "%v", `'a'`, 1)})
}

const nodeJS = `
var pg     = require('pg');
var fs     = require('fs');
var assert = require('assert');

var config = {
  user: 'root',
  ssl: {
    cert: fs.readFileSync(process.env.PGSSLCERT),
    key:  fs.readFileSync(process.env.PGSSLKEY)
  }
};

pg.connect(config, function (err, client, done) {
  var errQuit = function (err) {
    console.error(err);
    done();
    process.exit(1);
  }
  if (err) errQuit(err);

  client.query("SELECT 1 as first, 2+$1 as second", [%v], function (err, results) {
    if (err) errQuit(err);

    assert.deepEqual(results.rows, [{
      first: 1,
      second: 5
    }]);
    done();
    process.exit();
  });
});
`
