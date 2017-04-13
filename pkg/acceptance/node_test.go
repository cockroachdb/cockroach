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

client.connect(function (err) {
  if (err) throw err;

  client.query("SELECT 1 as first, 2+$1 as second", [%v], function (err, results) {
    if (err) throw err;

    assert.deepEqual(results.rows, [{
      first: 1,
      second: 5
    }]);
    client.end(function (err) {
      if (err) throw err;
    });
  });
});
`
