// Copyright 2017 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package acceptance

import (
	"context"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/testutils/skip"
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
	skip.WithIssue(t, 58218, "flaky test")
	s := log.Scope(t)
	defer s.Close(t)

	ctx := context.Background()
	testDockerSuccess(ctx, t, "csharp", []string{"sh", "-c", "cd /mnt/data/csharp && dotnet run"})
	testDockerFail(ctx, t, "csharp", []string{"sh", "-c", "cd /mnt/data/csharp && dotnet notacommand"})
}

func TestDockerJava(t *testing.T) {
	s := log.Scope(t)
	defer s.Close(t)

	ctx := context.Background()
	testDockerSuccess(ctx, t, "java", []string{"sh", "-c", "cd /mnt/data/java && mvn -o test"})
	testDockerFail(ctx, t, "java", []string{"sh", "-c", "cd /mnt/data/java && mvn -o foobar"})
}

func TestDockerElixir(t *testing.T) {
	skip.IgnoreLint(t, "Elixir requires network to run, which can flake. When attempting to update this (#52341), the new Elixir version does not work with CRDB/TLS.")

	s := log.Scope(t)
	defer s.Close(t)

	ctx := context.Background()
	testDockerSuccess(ctx, t, "elixir", []string{"sh", "-c", "cd /mnt/data/elixir/test_crdb && mix local.hex --force && mix deps.get && psql -c 'CREATE DATABASE IF NOT EXISTS testdb' && mix test"})
	testDockerFail(ctx, t, "elixir", []string{"sh", "-c", "cd /mnt/data/elixir/test_crdb && mix local.hex --force && mix deps.get && mix thisshouldfail"})
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
