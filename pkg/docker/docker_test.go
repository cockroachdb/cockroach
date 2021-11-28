package docker

import (
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/util/contextutil"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/docker/docker/client"
	"golang.org/x/net/context"
)

type SqlQuery struct {
	query          string
	expectedResult string
}

// runContainerArgs are equivalent to arguments passed to a `docker run `
// command.
// envSetting is to set the environment variables. volSetting is to set
// how local directories will be mounted to the container. cmd is to the command
// to run when starting the container.

type runContainerArgs struct {
	envSetting []string
	volSetting []string
	cmd        []string
}

// singleNodeDockerTest is consist of two main parts: start the container with
// a single-node cockroach server using runContainerArgs,
// and execute sql queries in this running container.
type singleNodeDockerTest struct {
	testName         string
	runContainerArgs runContainerArgs
	containerName    string
	sqlOpts          []string
	sqlQueries       []SqlQuery
}

func TestDocker(t *testing.T) {

	ctx := context.Background()
	pwd, err := os.Getwd()
	if err != nil {
		t.Fatal(fmt.Errorf("cannot get pwd: %v", err))
	}

	var dockerTests = []singleNodeDockerTest{
		{
			testName:      "single-node-certs-mode",
			containerName: "roach1",
			runContainerArgs: runContainerArgs{
				envSetting: []string{
					"COCKROACH_DATABASE=mydb",
					"COCKROACH_USER=myuser",
					"COCKROACH_PASSWORD=23333",
					"DOCKER_API_VERSION=1.39",
				},
				volSetting: []string{
					fmt.Sprintf("%s/cockroach-data/roach1:/cockroach/cockroach-data", pwd),
					fmt.Sprintf("%s/docker-entrypoint-initdb.d/:/docker-entrypoint-initdb.d", pwd),
				},
				cmd: []string{"start-single-node", "--certs-dir=certs"},
			},
			sqlOpts: []string{
				"--certs-dir=certs",
				"--user=myuser",
				"--url=postgresql://myuser:23333@127.0.0.1:26257/mydb?sslcert=certs%2Fclient.myuser.crt&sslkey=certs%2Fclient.myuser.key&sslmode=verify-full&sslrootcert=certs%2Fca.crt",
			},
			sqlQueries: []SqlQuery{
				{"SELECT current_user", "current_user\nmyuser"},
				{"SELECT current_database()", "current_database\nmydb"},
				{"CREATE TABLE hello (X INT)", "CREATE TABLE"},
				{"INSERT INTO hello VALUES (1), (2), (3)", "INSERT 3"},
				{"SELECT * FROM hello", "x\n1\n2\n3"},
				{"SELECT * FROM bello", "id | name\n1 | a\n2 | b\n3 | c"},
			},
		},
		{
			testName:      "single-node-insecure-mode",
			containerName: "roach2",
			runContainerArgs: runContainerArgs{
				envSetting: []string{
					"COCKROACH_DATABASE=mydb",
					"DOCKER_API_VERSION=1.39",
				},
				volSetting: []string{
					fmt.Sprintf("%s/cockroach-data/roach1:/cockroach/cockroach-data", pwd),
					fmt.Sprintf("%s/docker-entrypoint-initdb.d/:/docker-entrypoint-initdb.d", pwd),
				},
				cmd: []string{"start-single-node", "--insecure"},
			},
			sqlOpts: []string{
				"--insecure",
				"--database=mydb",
			},
			sqlQueries: []SqlQuery{
				{"SELECT current_user", "current_user\nroot"},
				{"SELECT current_database()", "current_database\nmydb"},
				{"CREATE TABLE hello (X INT)", "CREATE TABLE"},
				{"INSERT INTO hello VALUES (1), (2), (3)", "INSERT 3"},
				{"SELECT * FROM hello", "x\n1\n2\n3"},
				{"SELECT * FROM bello", "id | name\n1 | a\n2 | b\n3 | c"},
			},
		},
	}

	cli, err := client.NewClientWithOpts(client.FromEnv)
	cli.NegotiateAPIVersion(ctx)

	if err != nil {
		t.Fatal(err)
	}
	dn := dockerNode{
		cli: cli,
	}

	if err := contextutil.RunWithTimeout(
		ctx,
		"remove containers",
		defaultTimeout*time.Second,
		func(ctx context.Context) error {
			return dn.removeAllContainers(ctx)
		},
	); err != nil {
		t.Fatal(err)
	}

	for _, test := range dockerTests {
		t.Run(test.testName, func(t *testing.T) {

			if err := cleanUp(); err != nil {
				t.Fatal(err)
			}

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
				"wait server to fully start up",
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
						resp, err := dn.execSqlQuery(ctx, query, test.sqlOpts)
						if err != nil {
							return err
						}
						if cleanSqlOutput(resp.StdOut) != expected {
							return fmt.Errorf("executing %s, expect:\n%#v\n, got\n%#v\n", query, cleanSqlOutput(resp.StdOut), expected)
						}
						return nil
					},
				); err != nil {
					t.Errorf("%v", err)
				}
			}

			if err := contextutil.RunWithTimeout(
				ctx,
				"remove container",
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
