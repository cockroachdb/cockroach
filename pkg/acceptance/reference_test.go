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
// Author: David Taylor (david@cockroachlabs.com)

package acceptance

import (
	"fmt"
	"testing"

	"golang.org/x/net/context"

	"github.com/cockroachdb/cockroach/pkg/acceptance/cluster"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/docker/docker/api/types/container"
)

func runReferenceTestWithScript(ctx context.Context, t *testing.T, script string) {
	containerConfig := container.Config{
		Image: postgresTestImage,
		Cmd:   []string{"stat", cluster.CockroachBinaryInContainer},
	}
	if err := testDockerOneShot(ctx, t, "reference", containerConfig); err != nil {
		t.Skipf(`TODO(dt): No binary in one-shot container, see #6086: %s`, err)
	}

	containerConfig.Cmd = []string{"/bin/bash", "-c", script}
	if err := testDockerOneShot(ctx, t, "reference", containerConfig); err != nil {
		t.Error(err)
	}
}

func runReadWriteReferenceTest(
	ctx context.Context, t *testing.T, referenceBinPath string, backwardReferenceTest string,
) {
	referenceTestScript := fmt.Sprintf(`
set -xe
mkdir /old
cd /old

touch oldout newout
function finish() {
  cat oldout newout
}
trap finish EXIT

export PGHOST=localhost
export PGPORT=""
export COCKROACH_SKIP_UPDATE_CHECK=1
export COCKROACH_CERTS_DIR=/certs/

bin=/%s/cockroach
echo "TODO(marc): specify --certs-dir=/certs/ once the binary is upgraded"
$bin start --background --logtostderr &> oldout

echo "Use the reference binary to write a couple rows, then render its output to a file and shut down."
$bin sql -e "CREATE DATABASE old"
$bin sql -d old -e "CREATE TABLE testing_old (i int primary key, b bool, s string unique, d decimal, f float, t timestamp, v interval, index sb (s, b))"
$bin sql -d old -e "INSERT INTO testing_old values (1, true, 'hello', decimal '3.14159', 3.14159, NOW(), interval '1h')"
$bin sql -d old -e "INSERT INTO testing_old values (2, false, 'world', decimal '0.14159', 0.14159, NOW(), interval '234h45m2s234ms')"
$bin sql -d old -e "SELECT i, b, s, d, f, extract(epoch from (timestamp '1970-01-01 00:00:00' + v)) as v, extract(epoch FROM t) as e FROM testing_old" > old.everything
$bin quit && wait # wait will block until all background jobs finish.

bin=/cockroach/cockroach
$bin start --certs-dir=/certs/ --background --logtostderr &> newout
echo "Read data written by reference version using new binary"
$bin sql -d old -e "SELECT i, b, s, d, f, extract(epoch from (timestamp '1970-01-01 00:00:00' + v)) as v, extract(epoch FROM t) as e FROM testing_old" > new.everything
# diff returns non-zero if different. With set -e above, that would exit here.
diff new.everything old.everything

$bin debug range ls

echo "Add a row with the new binary and render the updated data before shutting down."
$bin sql -d old -e "INSERT INTO testing_old values (3, false, '!', decimal '2.14159', 2.14159, NOW(), interval '3h')"
$bin sql -d old -e "SELECT i, b, s, d, f, extract(epoch from (timestamp '1970-01-01 00:00:00' + v)) as v, extract(epoch FROM t) as e FROM testing_old" > new.everything
$bin sql -d old -e "CREATE TABLE testing_new (i int primary key, b bool, s string unique, d decimal, f float, t timestamp, v interval, index sb (s, b))"
$bin sql -d old -e "INSERT INTO testing_new values (4, false, '!!', decimal '1.14159', 1.14159, NOW(), interval '4h')"
$bin sql -d old -e "SELECT i, b, s, d, f, extract(epoch from (timestamp '1970-01-01 00:00:00' + v)) as v, extract(epoch FROM t) as e FROM testing_new" >> new.everything
$bin quit
# Let it close its listening sockets.
sleep 1

echo "Read the modified data using the reference binary again."
bin=/%s/cockroach
%s
`, referenceBinPath, referenceBinPath, backwardReferenceTest)
	runReferenceTestWithScript(ctx, t, referenceTestScript)
}

// TestDockerReadWriteBidirectionalReferenceVersion verifies that we can
// upgrade from the bidirectional reference version (i.e. the oldest
// version that we support downgrading to, specified in the
// postgres-test dockerfile), then downgrade to it again.
func TestDockerReadWriteBidirectionalReferenceVersion(t *testing.T) {
	s := log.Scope(t)
	defer s.Close(t)

	ctx := context.Background()
	backwardReferenceTest := `
touch out
function finish() {
  cat out
}
trap finish EXIT

export COCKROACH_CERTS_DIR=/certs/
echo "TODO(marc): specify --certs-dir=/certs/ once the binary is upgraded"
$bin start --background --logtostderr &> out
$bin sql -d old -e "SELECT i, b, s, d, f, extract(epoch from (timestamp '1970-01-01 00:00:00' + v)) as v, extract(epoch FROM t) as e FROM testing_old" > old.everything
$bin sql -d old -e "SELECT i, b, s, d, f, extract(epoch from (timestamp '1970-01-01 00:00:00' + v)) as v, extract(epoch FROM t) as e FROM testing_new" >> old.everything
# diff returns non-zero if different. With set -e above, that would exit here.
diff new.everything old.everything
$bin quit && wait
`
	runReadWriteReferenceTest(ctx, t, `bidirectional-reference-version`, backwardReferenceTest)
}

// TestDockerReadWriteForwardReferenceVersion verifies that we can
// upgrade from the forward reference version (i.e. the oldest version
// that we support upgrading from, specified in the postgres-test
// dockerfile), and that downgrading to it fails in the expected ways.
//
// When the forward and bidirectional reference versions are the same,
// this test is a duplicate of the bidirectional test above and exists
// solely to preserve the scaffolding for when they differ again.
func TestDockerReadWriteForwardReferenceVersion(t *testing.T) {
	s := log.Scope(t)
	defer s.Close(t)

	ctx := context.Background()
	backwardReferenceTest := `
touch out
function finish() {
  cat out
}
trap finish EXIT

export COCKROACH_CERTS_DIR=/certs/
echo "TODO(marc): specify --certs-dir=/certs/ once the binary is upgraded"
$bin start --background --logtostderr &> out
$bin sql -d old -e "SELECT i, b, s, d, f, extract(epoch from (timestamp '1970-01-01 00:00:00' + v)) as v, extract(epoch FROM t) as e FROM testing_old" > old.everything
$bin sql -d old -e "SELECT i, b, s, d, f, extract(epoch from (timestamp '1970-01-01 00:00:00' + v)) as v, extract(epoch FROM t) as e FROM testing_new" >> old.everything
# diff returns non-zero if different. With set -e above, that would exit here.
diff new.everything old.everything
$bin quit && wait
`
	runReadWriteReferenceTest(ctx, t, `forward-reference-version`, backwardReferenceTest)
}
