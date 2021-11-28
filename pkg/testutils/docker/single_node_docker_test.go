// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package docker

import (
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/util/contextutil"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/errors"
	"github.com/docker/docker/client"
	"golang.org/x/net/context"
)

const upstreamArtifactsPath = "/home/agent/work/.go/src/github.com/cockroachdb/cockroach/upstream_artifacts"

// sqlQuery consists of a sql query and the expected result.
type sqlQuery struct {
	query          string
	expectedResult string
}

// runContainerArgs are equivalent to arguments passed to a `docker run `
// command.
type runContainerArgs struct {
	// envSetting is to set the environment variables.
	envSetting []string
	// volSetting is to set how local directories will be mounted to the container.
	volSetting []string
	// cmd is the command to run when starting the container.
	cmd []string
}

// singleNodeDockerTest consists of two main parts: start the container with
// a single-node cockroach server using runContainerArgs,
// and execute sql queries in this running container.
type singleNodeDockerTest struct {
	testName         string
	runContainerArgs runContainerArgs
	containerName    string
	// sqlOpts are arguments passed to a `cockroach sql` command.
	sqlOpts []string
	// sqlQueries are queries to run in this container, and their expected results.
	sqlQueries []sqlQuery
}

func TestSingleNodeDocker(t *testing.T) {

	ctx := context.Background()
	pwd, err := os.Getwd()
	if err != nil {
		t.Fatal(errors.NewAssertionErrorWithWrappedErrf(err, "cannot get pwd"))
	}

	var dockerTests = []singleNodeDockerTest{
		{
			testName:      "single-node-insecure-mode",
			containerName: "roach2",
			runContainerArgs: runContainerArgs{
				envSetting: []string{
					"COCKROACH_DATABASE=mydb",
				},
				volSetting: []string{
					fmt.Sprintf("%s/cockroach-data/roach2:/cockroach/cockroach-data", pwd),
					fmt.Sprintf("%s/docker-fsnotify/:/cockroach/docker-fsnotify", upstreamArtifactsPath),
				},
				cmd: []string{"start-single-node", "--insecure"},
			},
			sqlOpts: []string{
				"--insecure",
				"--database=mydb",
			},
			sqlQueries: []sqlQuery{
				{"SELECT current_user", "current_user\nroot"},
				{"SELECT current_database()", "current_database\nmydb"},
				{"CREATE DATABASE mydb", "CREATE DATABASE"},
				{"USE mydb", "SET"},
				{"CREATE TABLE hello (X INT)", "CREATE TABLE"},
				{"INSERT INTO hello VALUES (1), (2), (3)", "INSERT 3"},
				{"SELECT * FROM hello", "x\n1\n2\n3"},
			},
		},
	}

	cl, err := client.NewClientWithOpts(client.FromEnv)
	cl.NegotiateAPIVersion(ctx)

	if err != nil {
		t.Fatal(err)
	}
	dn := dockerNode{
		cl: cl,
	}

	if err := removeLocalData(); err != nil {
		t.Fatal(err)
	}

	if err := contextutil.RunWithTimeout(
		ctx,
		"remove all containers using current image",
		defaultTimeout*time.Second,
		func(ctx context.Context) error {
			return dn.removeAllContainers(ctx)
		}); err != nil {
		t.Errorf("%v", err)
	}

	for _, test := range dockerTests {
		t.Run(test.testName, func(t *testing.T) {

			if err := contextutil.RunWithTimeout(
				ctx,
				"start container",
				defaultTimeout*time.Second,
				func(ctx context.Context) error {
					return dn.startContainer(
						ctx,
						test.containerName,
						test.runContainerArgs.envSetting,
						test.runContainerArgs.volSetting,
						test.runContainerArgs.cmd,
					)
				},
			); err != nil {
				t.Fatal(err)
			}

			if err := contextutil.RunWithTimeout(
				ctx,
				"wait for the server to fully start up",
				serverStartTimeout*time.Second,
				func(ctx context.Context) error {
					return dn.waitServerStarts(ctx)
				},
			); err != nil {
				t.Fatal(err)
			}

			if err := contextutil.RunWithTimeout(
				ctx,
				"show log",
				defaultTimeout*time.Second,
				func(ctx context.Context) error {
					return dn.showContainerLog(ctx, fmt.Sprintf("%s.log", test.testName))
				},
			); err != nil {
				log.Warningf(ctx, "cannot show container log: %v", err)
			}

			for _, qe := range test.sqlQueries {
				query := qe.query
				expected := qe.expectedResult

				if err := contextutil.RunWithTimeout(
					ctx,
					fmt.Sprintf("execute command \"%s\"", query),
					defaultTimeout*time.Second,
					func(ctx context.Context) error {
						resp, err := dn.execSQLQuery(ctx, query, test.sqlOpts)
						if err != nil {
							return err
						}
						if cleanSQLOutput(resp.stdOut) != expected {
							return fmt.Errorf("executing %s, expect:\n%#v\n, got\n%#v", query, cleanSQLOutput(resp.stdOut), expected)
						}
						return nil
					},
				); err != nil {
					t.Errorf("%v", err)
				}
			}

			if err := contextutil.RunWithTimeout(
				ctx,
				"remove current container",
				defaultTimeout*time.Second,
				func(ctx context.Context) error {
					return dn.rmContainer(ctx)
				},
			); err != nil {
				t.Errorf("%v", err)
			}

		})
	}

}
