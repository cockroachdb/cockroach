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

// +build acceptance

package acceptance

import (
	"fmt"
	"testing"
)

// TestPHP connects to a cluster with PHP.
func TestPHP(t *testing.T) {
	testDockerSuccess(t, "php", []string{"-r", fmt.Sprintf(php, 3)})
	testDockerFail(t, "php", []string{"-r", fmt.Sprintf(php, `"a"`)})
}

const php = `
function kill($msg) {
	echo($msg);
	exit(1);
}

$dbconn = pg_connect('')
	or kill('Could not connect: ' . pg_last_error());
$result = pg_query_params('SELECT 1, 2 > $1, $1', [%v])
	or kill('Query failed: ' . pg_last_error());
$arr = pg_fetch_row($result);
($arr === ['1', 'f', '3']) or kill('Unexpected: ' . print_r($arr, true));
`
