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
// Author: Matt Jibson (mjibson@cockroachlabs.com)

package acceptance

import (
	"strings"
	"testing"
)

func TestDockerPython(t *testing.T) {
	testDockerSuccess(t, "python", []string{"python", "-c", strings.Replace(python, "%v", "3", 1)})
	testDockerFail(t, "python", []string{"python", "-c", strings.Replace(python, "%v", `"a"`, 1)})
}

const python = `
import psycopg2
conn = psycopg2.connect('')
cur = conn.cursor()
cur.execute("SELECT 1, 2+%v")
v = cur.fetchall()
assert v == [(1, 5)]
# verify #6597 is fixed
cur = conn.cursor()
cur.execute("SELECT now()")
v = cur.fetchall()
`
