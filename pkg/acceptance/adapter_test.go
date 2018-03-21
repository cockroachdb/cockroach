// Copyright 2017 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	 http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package acceptance

import (
	"context"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/util/log"
)

func TestDockerC(t *testing.T) {
	s := log.Scope(t)
	defer s.Close(t)

	ctx := context.Background()
	t.Run("Success", func(t *testing.T) {
		testDockerSuccess(ctx, t, "c", []string{
			"sh", "-c",
			`cd /mnt/data/c && make test && ./test 'SELECT $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12'`,
		})
	})
	t.Run("Fail", func(t *testing.T) {
		testDockerFail(ctx, t, "c", []string{
			"sh", "-c",
			`cd /mnt/data/c && make test && ./test 'SELECT 1'`,
		})
	})
}

func TestDockerCSharp(t *testing.T) {
	t.Skip("#22769")
	s := log.Scope(t)
	defer s.Close(t)

	const csharp = `
	set -euxo pipefail

	cd /mnt/data/csharp

	# In dotnet, to get a cert with a private key, we have to use a pfx file.
	# See:
	# http://www.sparxeng.com/blog/software/x-509-self-signed-certificate-for-cryptography-in-net
	# https://stackoverflow.com/questions/808669/convert-a-cert-pem-certificate-to-a-pfx-certificate
	openssl pkcs12 -inkey ${PGSSLKEY} -in ${PGSSLCERT} -export -out node.pfx -passout pass:

	dotnet %v
	`

	ctx := context.Background()
	testDockerSuccess(ctx, t, "csharp", []string{"/bin/bash", "-c", strings.Replace(csharp, "%v", "run", 1)})
	testDockerFail(ctx, t, "csharp", []string{"/bin/bash", "-c", strings.Replace(csharp, "%v", "notacommand", 1)})
}

func TestDockerJava(t *testing.T) {
	s := log.Scope(t)
	defer s.Close(t)

	const java = `
	set -e
	cd /mnt/data/java
	# See: https://basildoncoder.com/blog/postgresql-jdbc-client-certificates.html
	openssl pkcs8 -topk8 -inform PEM -outform DER -in /certs/node.key -out key.pk8 -nocrypt

	mvn %v -o
	rm -rf target
	`

	ctx := context.Background()
	testDockerSuccess(ctx, t, "java", []string{"/bin/sh", "-c", strings.Replace(java, "%v", "test", 2)})
	testDockerFail(ctx, t, "java", []string{"/bin/sh", "-c", strings.Replace(java, "%v", "foobar", 2)})
}

func TestDockerNodeJS(t *testing.T) {
	s := log.Scope(t)
	defer s.Close(t)

	const nodeJS = `
	set -e
	cd /mnt/data/node

	export SHOULD_FAIL=%v
	# Get access to globally installed node modules.
	export NODE_PATH=$NODE_PATH:/usr/lib/node
	/usr/lib/node/.bin/mocha .
	`

	ctx := context.Background()
	testDockerSuccess(ctx, t, "node.js", []string{"/bin/sh", "-c", strings.Replace(nodeJS, "%v", "", 1)})
	testDockerFail(ctx, t, "node.js", []string{"/bin/sh", "-c", strings.Replace(nodeJS, "%v", "fail", 1)})
}

func TestDockerPHP(t *testing.T) {
	s := log.Scope(t)
	defer s.Close(t)

	ctx := context.Background()
	testDockerSuccess(ctx, t, "php", []string{"sh", "-c", "cd /mnt/data/php && php test.php 3"})
	testDockerFail(ctx, t, "php", []string{"sh", "-c", "cd /mnt/data/php && php test.php 1"})
}

func TestDockerPSQL(t *testing.T) {
	s := log.Scope(t)
	defer s.Close(t)

	ctx := context.Background()
	testDockerSuccess(ctx, t, "psql", []string{"/mnt/data/psql/test-psql.sh"})
}

func TestDockerPython(t *testing.T) {
	s := log.Scope(t)
	defer s.Close(t)

	ctx := context.Background()
	testDockerSuccess(ctx, t, "python", []string{"sh", "-c", "cd /mnt/data/python && python test.py 3"})
	testDockerFail(ctx, t, "python", []string{"sh", "-c", "cd /mnt/data/python && python test.py 2"})
}

func TestDockerRuby(t *testing.T) {
	s := log.Scope(t)
	defer s.Close(t)

	ctx := context.Background()
	testDockerSuccess(ctx, t, "ruby", []string{"sh", "-c", "cd /mnt/data/ruby && ruby test.rb 3"})
	testDockerFail(ctx, t, "ruby", []string{"sh", "-c", "cd /mnt/data/ruby && ruby test.rb 1"})
}
