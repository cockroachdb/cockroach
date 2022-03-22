// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

//go:build docker
// +build docker

package docker

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"io/ioutil"
	"math"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/util/contextutil"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/errors"
	"github.com/docker/docker/api/types"
	"github.com/docker/docker/api/types/container"
	"github.com/docker/docker/api/types/filters"
	"github.com/docker/docker/client"
	"github.com/docker/docker/pkg/stdcopy"
	"github.com/docker/go-connections/nat"
)

const fsnotifyBinName = "docker-fsnotify-bin"

// sqlQuery consists of a sql query and the expected result.
type sqlQuery struct {
	query          string
	expectedResult string
}

// runContainerArgs are equivalent to arguments passed to a `docker run`
// command.
type runContainerArgs struct {
	// envSetting is to set the environment variables for the docker container.
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

	fsnotifyPath := filepath.Join(filepath.Dir(filepath.Dir(filepath.Dir(filepath.Dir(filepath.Dir(filepath.Dir(pwd)))))), "docker-fsnotify")

	var dockerTests = []singleNodeDockerTest{
		{
			testName:      "single-node-secure-mode",
			containerName: "roach1",
			runContainerArgs: runContainerArgs{
				envSetting: []string{
					"COCKROACH_DATABASE=mydb",
					"COCKROACH_USER=myuser",
					"COCKROACH_PASSWORD=23333",
				},
				volSetting: []string{
					fmt.Sprintf("%s/testdata/single-node-test/docker-entrypoint-initdb.d/:/docker-entrypoint-initdb.d", pwd),
					fmt.Sprintf("%s/docker-fsnotify-bin:/cockroach/docker-fsnotify", fsnotifyPath),
				},
				cmd: []string{"start-single-node", "--certs-dir=certs"},
			},
			sqlOpts: []string{
				"--format=csv",
				"--certs-dir=certs",
				"--user=myuser",
				"--url=postgresql://myuser:23333@127.0.0.1:26257/mydb?sslcert=certs%2Fclient.myuser.crt&sslkey=certs%2Fclient.myuser.key&sslmode=verify-full&sslrootcert=certs%2Fca.crt",
			},
			sqlQueries: []sqlQuery{
				{"SELECT current_user", "current_user\nmyuser"},
				{"SELECT current_database()", "current_database\nmydb"},
				{"CREATE TABLE hello (X INT)", "CREATE TABLE"},
				{"INSERT INTO hello VALUES (1), (2), (3)", "INSERT 3"},
				{"SELECT * FROM hello", "x\n1\n2\n3"},
				{"SELECT * FROM bello", "id,name\n1,a\n2,b\n3,c"},
			},
		},
		{
			testName:      "single-node-insecure-mode",
			containerName: "roach2",
			runContainerArgs: runContainerArgs{
				envSetting: []string{
					"COCKROACH_DATABASE=mydb",
				},
				volSetting: []string{
					fmt.Sprintf("%s/testdata/single-node-test/docker-entrypoint-initdb.d/:/docker-entrypoint-initdb.d", pwd),
					fmt.Sprintf("%s/docker-fsnotify-bin:/cockroach/docker-fsnotify", fsnotifyPath),
				},
				cmd: []string{"start-single-node", "--insecure"},
			},
			sqlOpts: []string{
				"--format=csv",
				"--insecure",
				"--database=mydb",
			},
			sqlQueries: []sqlQuery{
				{"SELECT current_user", "current_user\nroot"},
				{"SELECT current_database()", "current_database\nmydb"},
				{"CREATE TABLE hello (X INT)", "CREATE TABLE"},
				{"INSERT INTO hello VALUES (1), (2), (3)", "INSERT 3"},
				{"SELECT * FROM hello", "x\n1\n2\n3"},
				{"SELECT * FROM bello", "id,name\n1,a\n2,b\n3,c"},
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
		defaultTimeout,
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
				defaultTimeout,
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
				serverStartTimeout,
				func(ctx context.Context) error {
					return dn.waitServerStarts(ctx)
				},
			); err != nil {
				t.Fatal(err)
			}

			if err := contextutil.RunWithTimeout(
				ctx,
				"show log",
				defaultTimeout,
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
					defaultTimeout,
					func(ctx context.Context) error {
						resp, err := dn.execSQLQuery(ctx, query, test.sqlOpts)
						if err != nil {
							return err
						}
						cleanedOutput, err := cleanQueryResult(resp.stdOut)
						if err != nil {
							return err
						}
						if cleanedOutput != expected {
							return fmt.Errorf("executing %s, expect:\n%#v\n, got\n%#v", query, expected, cleanedOutput)
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
				defaultTimeout,
				func(ctx context.Context) error {
					return dn.rmContainer(ctx)
				},
			); err != nil {
				t.Errorf("%v", err)
			}

		})
	}

}

const (
	imageName           = "cockroachdb/cockroach-ci:latest"
	defaultTimeout      = 10 * time.Second
	serverStartTimeout  = 80 * time.Second
	listenURLFile       = "demoFile"
	cockroachEntrypoint = "./cockroach"
	hostPort            = "8080"
	cockroachPort       = "26257"
	hostIP              = "127.0.0.1"
)

type dockerNode struct {
	cl     client.APIClient
	contID string
}

// removeLocalData removes existing database saved in cockroach-data.
func removeLocalData() error {
	err := os.RemoveAll("./cockroach-data")
	if err != nil {
		return errors.Wrap(err, "cannot remove local data")
	}
	return nil
}

// showContainerLog outputs the container's logs to the logFile and stderr.
func (dn *dockerNode) showContainerLog(ctx context.Context, logFileName string) error {

	cmdLog, err := os.Create(logFileName)
	if err != nil {
		return errors.Wrap(err, "cannot create log file")
	}
	out := io.MultiWriter(cmdLog, os.Stderr)

	rc, err := dn.cl.ContainerLogs(ctx, dn.contID, types.ContainerLogsOptions{
		ShowStdout: true,
		ShowStderr: true,
	})
	if err != nil {
		return errors.Wrap(err, "cannot create docker logs")
	}

	// The docker log output is not quite plaintext: each line has a
	// prefix consisting of one byte file descriptor (stdout vs stderr),
	// three bytes padding, four byte length. We could use this to
	// disentangle stdout and stderr if we wanted to output them into
	// separate streams, but we don't really care.
	for {
		var header uint64
		if err := binary.Read(rc, binary.BigEndian, &header); err == io.EOF {
			break
		} else if err != nil {
			return err
		}
		size := header & math.MaxUint32
		if _, err := io.CopyN(out, rc, int64(size)); err != nil {
			return err
		}
	}

	if err := rc.Close(); err != nil {
		return errors.Wrap(err, "cannot close docker log")
	}

	return nil
}

// startContainer starts a container with given setting for environment
// variables, mounted volumes, and command to run.
func (dn *dockerNode) startContainer(
	ctx context.Context, containerName string, envSetting []string, volSetting []string, cmd []string,
) error {

	containerConfig := container.Config{
		Hostname:     containerName,
		Image:        imageName,
		Env:          envSetting,
		ExposedPorts: nat.PortSet{hostPort: struct{}{}, cockroachPort: struct{}{}},
		Cmd:          append(cmd, fmt.Sprintf("--listening-url-file=%s", listenURLFile)),
	}

	hostConfig := container.HostConfig{
		Binds: volSetting,
		PortBindings: map[nat.Port][]nat.PortBinding{
			nat.Port(hostPort):      {{HostIP: hostIP, HostPort: hostPort}},
			nat.Port(cockroachPort): {{HostIP: hostIP, HostPort: cockroachPort}},
		},
	}

	resp, err := dn.cl.ContainerCreate(
		ctx,
		&containerConfig,
		&hostConfig,
		nil,
		nil,
		containerName,
	)
	if err != nil {
		return errors.Wrap(err, "cannot create container")
	}

	dn.contID = resp.ID

	if err := dn.cl.ContainerStart(ctx, dn.contID,
		types.ContainerStartOptions{}); err != nil {
		return errors.Wrap(err, "cannot start container")
	}

	return nil
}

// removeAllContainers removes all running containers based on the cockroach-ci
// docker image by force.
func (dn *dockerNode) removeAllContainers(ctx context.Context) error {
	filter := filters.NewArgs(filters.Arg("ancestor", imageName))
	conts, err := dn.cl.ContainerList(ctx,
		types.ContainerListOptions{All: true, Filters: filter})
	if err != nil {
		return errors.Wrapf(
			err,
			"cannot list all containers on docker image %s",
			imageName,
		)
	}
	for _, cont := range conts {
		err := dn.cl.ContainerRemove(ctx, cont.ID,
			types.ContainerRemoveOptions{Force: true})
		if err != nil {
			return errors.Wrapf(err, "cannot remove container %s", cont.Names)
		}
	}
	return nil
}

type execResult struct {
	stdOut   string
	stdErr   string
	exitCode int
}

// InspectExecResp inspects the result of a docker command execution, saves its
// stdout, stderr message and exit code to an execResult, and returns this
// execResult and a possible error.
func (dn *dockerNode) InspectExecResp(ctx context.Context, execID string) (execResult, error) {
	var execRes execResult
	resp, err := dn.cl.ContainerExecAttach(ctx, execID, types.ExecStartCheck{})
	if err != nil {
		return execResult{}, err
	}
	defer resp.Close()

	var outBuf, errBuf bytes.Buffer
	outputDone := make(chan error)

	go func() {
		_, err = stdcopy.StdCopy(&outBuf, &errBuf, resp.Reader)
		outputDone <- err
	}()

	select {
	case err := <-outputDone:
		if err != nil {
			return execRes, err
		}
		break

	case <-ctx.Done():
		return execRes, ctx.Err()
	}

	stdout, err := ioutil.ReadAll(&outBuf)
	if err != nil {
		return execRes, err
	}
	stderr, err := ioutil.ReadAll(&errBuf)
	if err != nil {
		return execRes, err
	}

	res, err := dn.cl.ContainerExecInspect(ctx, execID)
	if err != nil {
		return execRes, err
	}

	execRes.exitCode = res.ExitCode
	execRes.stdOut = string(stdout)
	execRes.stdErr = string(stderr)
	return execRes, nil
}

// execCommand is to execute command in the current container, and returns the
// execution result and possible error.
func (dn *dockerNode) execCommand(
	ctx context.Context, cmd []string, workingDir string,
) (*execResult, error) {
	execID, err := dn.cl.ContainerExecCreate(ctx, dn.contID, types.ExecConfig{
		User:         "root",
		AttachStderr: true,
		AttachStdout: true,
		Tty:          true,
		Cmd:          cmd,
		WorkingDir:   workingDir,
	})

	if err != nil {
		return nil, errors.Wrapf(
			err,
			"cannot create command \"%s\"",
			strings.Join(cmd, " "),
		)
	}

	res, err := dn.InspectExecResp(ctx, execID.ID)
	if err != nil {
		return nil, errors.Wrapf(
			err,
			"cannot execute command \"%s\"",
			strings.Join(cmd, " "),
		)
	}

	if res.exitCode != 0 {
		return &res, errors.Wrapf(
			errors.Newf("%s", res.stdErr),
			"command \"%s\" exit with code %d:\n %+v",
			strings.Join(cmd, " "),
			res.exitCode,
			res,
		)
	}

	return &res, nil
}

// waitServerStarts waits till the server truly starts or timeout, whichever
// earlier. It keeps listening to the listenURLFile till it is closed and
// written. This is because in #70238, the improved init process for single-node
// server is to start the server, run the init process, and then restart the
// server. We mark it as fully started until the server is successfully
// restarted, and hence write the url to listenURLFile.
func (dn *dockerNode) waitServerStarts(ctx context.Context) error {
	var res *execResult
	var err error

	// Run the binary which listens to the /cockroach folder until the
	// initialization process has finished or timeout.
	res, err = dn.execCommand(ctx, []string{
		"./docker-fsnotify",
		"/cockroach",
		listenURLFile,
		strconv.Itoa(int(serverStartTimeout.Seconds())),
	}, "/cockroach")
	if err != nil {
		return errors.Wrapf(err, "cannot run fsnotify to listen to %s:\nres:%#v", listenURLFile, res)
	}

	if strings.Contains(res.stdOut, "finished\r\n") {
		return nil
	}
	return errors.Wrap(errors.Newf("%s", res), "error in waiting the server to start")
}

// execSQLQuery executes the sql query and returns the server's output and
// possible error.
func (dn *dockerNode) execSQLQuery(
	ctx context.Context, sqlQuery string, sqlQueryOpts []string,
) (*execResult, error) {
	query := append([]string{cockroachEntrypoint, "sql", "-e", sqlQuery},
		sqlQueryOpts...,
	)

	res, err := dn.execCommand(ctx, query, "/cockroach")
	if err != nil {
		return nil, errors.Wrapf(err, "error executing query \"%s\"", sqlQuery)
	}

	return res, nil
}

//rmContainer performs a forced deletion of the current container.
func (dn *dockerNode) rmContainer(ctx context.Context) error {
	if err := dn.cl.ContainerRemove(ctx, dn.contID, types.ContainerRemoveOptions{
		Force: true,
	}); err != nil {
		return errors.Wrapf(err, "cannot remove container %s", dn.contID)
	}
	dn.contID = ""
	return nil
}

// cleanQueryResult is to parse the result from a sql query to a cleaner format.
// e.g.
// "id,name\r\n1,a\r\n2,b\r\n3,c\r\n\r\n\r\nTime: 11ms\r\n\r\n"
// => "id,name\n1,a\n2,b\n3,c"
func cleanQueryResult(queryRes string) (string, error) {
	formatted := strings.ReplaceAll(queryRes, "\r\n", "\n")
	r := regexp.MustCompile(`([\s\S]+)\n{3}Time:.+`)
	res := r.FindStringSubmatch(formatted)
	if len(res) < 2 {
		return "", errors.Wrapf(errors.Newf("%s", queryRes), "cannot parse the query result: %#v")
	}
	return res[1], nil

}
