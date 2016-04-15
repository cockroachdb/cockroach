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

import "testing"

func TestDockerReadWriteReferenceVersion(t *testing.T) {
	func() { // OneShot assumes it is at specific call depth from TestFoo.
		if testDockerSingleNode(t, "reference", []string{"/cockroach", "version"}) != nil {
			t.Skip(`TODO(dt): No /cockroach binary in one-shot container. See #6086.`)
		}
	}()
	testDockerSuccess(t, "reference", []string{"/bin/bash", "-c", `
set -xe
mkdir /old
cd /old

export PGHOST=localhost
export PGPORT=""

bin=/reference-version/cockroach
$bin start &
sleep 1
echo "Use the reference binary to write a couple rows, then render its output to a file and shut down."
$bin sql -e "CREATE DATABASE old"
$bin sql -d old -e "CREATE TABLE testing (i int primary key, b bool, s string unique, d decimal, f float, t timestamp, v interval, index sb (s, b))"
$bin sql -d old -e "INSERT INTO testing values (1, true, 'hello', decimal '3.14159', 3.14159, NOW(), interval '1h')"
$bin sql -d old -e "INSERT INTO testing values (2, false, 'world', decimal '0.14159', 0.14159, NOW(), interval '234h45m2s234ms')"
$bin sql -d old -e "SELECT * FROM testing" > old.everything
$bin quit && wait # wait will block until all background jobs finish.

bin=/cockroach
$bin start &
sleep 1
echo "Read data written by reference version using new binary"
$bin sql -d old -e "SELECT * FROM testing" > new.everything
# diff returns non-zero if different. With set -e above, that would exit here.
diff new.everything old.everything

echo "Add a row with the new binary and render the updated data before shutting down."
$bin sql -d old -e "INSERT INTO testing values (3, false, '!', decimal '2.14159', 2.14159, NOW(), interval '3h')"
$bin sql -d old -e "SELECT * FROM testing" > new.everything
$bin quit && wait

echo "Read the modified data using the reference binary again."
bin=/reference-version/cockroach
$bin start &
sleep 1
$bin sql -d old -e "SELECT * FROM testing" > old.everything
# diff returns non-zero if different. With set -e above, that would exit here.
diff new.everything old.everything
$bin quit && wait
`})
}
