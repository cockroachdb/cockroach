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
	"archive/tar"
	"compress/gzip"
	"context"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"testing"

	"github.com/cockroachdb/cockroach/util/log"
)

const (
	forwardReferenceVersion       = `beta-20160414`
	bidirectionalReferenceVersion = `beta-20160629`
)

func maybeDownloadBinary(url string, outdir string, version string) error {
	outfile := filepath.Join(outdir, fmt.Sprintf("cockroach-%s", version))
	if _, err := os.Stat(outfile); err == nil {
		// Something already exists there, assume it's a cockroach binary. We'll
		// fail pretty quickly if it's not.
		return nil
	}

	log.Infof(context.Background(), "Downloading %s", url)
	resp, err := http.Get(url)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	unzipped, err := gzip.NewReader(resp.Body)
	if err != nil {
		return err
	}
	defer unzipped.Close()

	untarred := tar.NewReader(unzipped)
	for {
		header, err := untarred.Next()
		if err != nil {
			if err == io.EOF {
				break
			}
			return err
		}
		if filepath.Base(header.Name) == "cockroach" {
			if err := os.MkdirAll(filepath.Dir(outfile), 0755); err != nil {
				return err
			}
			out, err := os.OpenFile(outfile, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, os.FileMode(header.Mode))
			if err != nil {
				return err
			}
			defer out.Close()

			if _, err := io.Copy(out, untarred); err != io.EOF {
				return err
			}
			// We found what we wanted, return early.
			return nil
		}
	}
	return fmt.Errorf("no tarred and gzipped cockroach binary found at %q", url)
}

func runReferenceTestWithScript(t *testing.T, script string) {
	if err := testDockerOneShot(t, "reference", []string{"/cockroach", "version"}); err != nil {
		t.Skipf(`TODO(dt): No /cockroach binary in one-shot container, see #6086: %s`, err)
	}

	if err := testDockerOneShot(t, "reference", []string{"/bin/bash", "-c", script}); err != nil {
		t.Error(err)
	}
}

func runReadWriteReferenceTest(
	t *testing.T,
	version string,
	backwardReferenceTest string,
) {
	binaryURL := fmt.Sprintf("https://binaries.cockroachdb.com/cockroach-%s.linux-amd64.tgz", version)
	if err := maybeDownloadBinary(binaryURL, "../bin", version); err != nil {
		t.Fatal(err)
	}

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

bin=/go/src/github.com/cockroachdb/cockroach/bin/cockroach-%s
# TODO(bdarnell): when --background is in referenceBinPath, use it here and below.
# The until loop will also be unnecessary at that point.
$bin start --alsologtostderr & &> oldout
# Wait until cockroach has started up successfully.
until $bin sql -e "SELECT 1"; do sleep 1; done

echo "Use the reference binary to write a couple rows, then render its output to a file and shut down."
$bin sql -e "CREATE DATABASE old"
$bin sql -d old -e "CREATE TABLE testing_old (i int primary key, b bool, s string unique, d decimal, f float, t timestamp, v interval, index sb (s, b))"
$bin sql -d old -e "INSERT INTO testing_old values (1, true, 'hello', decimal '3.14159', 3.14159, NOW(), interval '1h')"
$bin sql -d old -e "INSERT INTO testing_old values (2, false, 'world', decimal '0.14159', 0.14159, NOW(), interval '234h45m2s234ms')"
$bin sql -d old -e "SELECT i, b, s, d, f, v, extract(epoch FROM t) FROM testing_old" > old.everything
$bin quit && wait # wait will block until all background jobs finish.

bin=/cockroach
$bin start --background --alsologtostderr &> newout
echo "Read data written by reference version using new binary"
$bin sql -d old -e "SELECT i, b, s, d, f, v, extract(epoch FROM t) FROM testing_old" > new.everything
# diff returns non-zero if different. With set -e above, that would exit here.
diff new.everything old.everything

# Scan all data (some of which may not have been touched by the SQL commands)
# to wake up all Raft groups.
$bin debug kv scan

echo "Add a row with the new binary and render the updated data before shutting down."
$bin sql -d old -e "INSERT INTO testing_old values (3, false, '!', decimal '2.14159', 2.14159, NOW(), interval '3h')"
$bin sql -d old -e "SELECT i, b, s, d, f, v, extract(epoch FROM t) FROM testing_old" > new.everything
$bin sql -d old -e "CREATE TABLE testing_new (i int primary key, b bool, s string unique, d decimal, f float, t timestamp, v interval, index sb (s, b))"
$bin sql -d old -e "INSERT INTO testing_new values (4, false, '!!', decimal '1.14159', 1.14159, NOW(), interval '4h')"
$bin sql -d old -e "SELECT i, b, s, d, f, v, extract(epoch FROM t) FROM testing_new" >> new.everything
$bin quit
# Let it close its listening sockets.
sleep 1

echo "Read the modified data using the reference binary again."
bin=/go/src/github.com/cockroachdb/cockroach/bin/cockroach-%s
%s
`, version, version, backwardReferenceTest)
	runReferenceTestWithScript(t, referenceTestScript)
}

func TestDockerReadWriteBidirectionalReferenceVersion(t *testing.T) {
	backwardReferenceTest := `
touch out
function finish() {
  cat out
}
trap finish EXIT

$bin start --background --alsologtostderr &> out
$bin sql -d old -e "SELECT i, b, s, d, f, v, extract(epoch FROM t) FROM testing_old" > old.everything
$bin sql -d old -e "SELECT i, b, s, d, f, v, extract(epoch FROM t) FROM testing_new" >> old.everything
# diff returns non-zero if different. With set -e above, that would exit here.
diff new.everything old.everything
$bin quit && wait
`
	runReadWriteReferenceTest(t, bidirectionalReferenceVersion, backwardReferenceTest)
}

func TestDockerReadWriteForwardReferenceVersion(t *testing.T) {
	backwardReferenceTest := `
touch out
function finish() {
  cat out
}
trap finish EXIT

$bin start & &> out
until $bin sql -e "SELECT 1"; do sleep 1; done
# grep returns non-zero if it didn't match anything. With set -e above, that would exit here.
$bin sql -d old -e "SELECT i, b, s, d, f, v, extract(epoch FROM t) FROM testing_new" 2>&1 | grep "is encoded using using version 2, but this client only supports version 1"
$bin quit && wait
`
	runReadWriteReferenceTest(t, forwardReferenceVersion, backwardReferenceTest)
}

func TestDockerMigration_7429(t *testing.T) {
	script := `
set -eux
bin=/cockroach

touch out
function finish() {
  cat out
}
trap finish EXIT

$bin start --alsologtostderr=INFO --background --store=/cockroach-data-reference-7429 &> out
$bin debug kv scan
$bin quit
`
	runReferenceTestWithScript(t, script)
}
