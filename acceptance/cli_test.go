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
// Author: Raphael 'kena' Poss (knz@cockroachlabs.com)

package acceptance

import "testing"

func TestDockerCli(t *testing.T) {
	script := `
set -eu
bin=/cockroach
testdir=/go/src/github.com/cockroachdb/cockroach/cli/interactive_tests

# Fail early if the test sources are not mounted.
test -d "$testdir"

touch out
function finish() {
  cat out
}
trap finish EXIT

# Start a server.
export COCKROACH_SKIP_UPDATE_CHECK=1
export PGHOST=localhost
export PGPORT=""
$bin start &> out &
echo $! >server_pid
sleep 1

# Run the expect test harness.
for t in "$testdir"/test_*.tcl; do
   tn=$(basename $t)
   echo -n "TEST: $tn -- "
   if expect -d -f $t $bin >log 2>&1; then
      echo SUCCESS
   else
      echo FAIL
      cat log
      exit 1
   fi
done

# Stop the server and terminate.
$bin quit &>> out && wait

# FIXME: until this there's no output
# exit 1
`
	runReferenceTestWithScript(t, script)
}
