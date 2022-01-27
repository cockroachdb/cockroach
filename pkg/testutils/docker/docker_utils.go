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
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"io/ioutil"
	"math"
	"os"
	"regexp"
	"strings"

	"github.com/cockroachdb/errors"
	"github.com/docker/docker/api/types"
	"github.com/docker/docker/api/types/container"
	"github.com/docker/docker/api/types/filters"
	"github.com/docker/docker/client"
	"github.com/docker/docker/pkg/stdcopy"
	"github.com/docker/go-connections/nat"
)

const (
	imageName           = "cockroachdb/cockroach-ci:latest"
	defaultTimeout      = 10
	serverStartTimeout  = 80
	listenURLFile       = "demoFile"
	cockroachEntrypoint = "./cockroach"
	hostPort            = "8080"
	cockroachPort       = "26275"
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
		return errors.NewAssertionErrorWithWrappedErrf(err, "cannot remove local data")
	}
	return nil
}

// showContainerLog outputs the container's logs to the logFile and stderr.
func (dn *dockerNode) showContainerLog(ctx context.Context, logFileName string) error {

	cmdLog, err := os.Create(logFileName)
	if err != nil {
		return errors.NewAssertionErrorWithWrappedErrf(err, "cannot create log file")
	}
	out := io.MultiWriter(cmdLog, os.Stderr)

	rc, err := dn.cl.ContainerLogs(ctx, dn.contID, types.ContainerLogsOptions{
		ShowStdout: true,
		ShowStderr: true,
	})
	if err != nil {
		return errors.NewAssertionErrorWithWrappedErrf(err, "cannot create docker logs")
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
		return errors.NewAssertionErrorWithWrappedErrf(err, "cannot close docker log")
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
		return errors.NewAssertionErrorWithWrappedErrf(err, "cannot create container")
	}

	dn.contID = resp.ID

	if err := dn.cl.ContainerStart(ctx, dn.contID,
		types.ContainerStartOptions{}); err != nil {
		return errors.NewAssertionErrorWithWrappedErrf(err, "cannot start container")
	}

	return nil
}

// removeAllContainers removes all running containers by force.
func (dn *dockerNode) removeAllContainers(ctx context.Context) error {
	filter := filters.NewArgs(filters.Arg("ancestor", imageName))
	conts, err := dn.cl.ContainerList(ctx,
		types.ContainerListOptions{All: true, Filters: filter})
	if err != nil {
		return errors.NewAssertionErrorWithWrappedErrf(err, "cannot list all conts")
	}
	for _, cont := range conts {
		err := dn.cl.ContainerRemove(ctx, cont.ID,
			types.ContainerRemoveOptions{Force: true})
		if err != nil {
			return errors.NewAssertionErrorWithWrappedErrf(err, "cannot remove cont %s", cont.Names)
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
		return nil, errors.NewAssertionErrorWithWrappedErrf(
			err,
			"cannot create command \"%s\"",
			strings.Join(cmd, " "),
		)
	}

	res, err := dn.InspectExecResp(ctx, execID.ID)
	if err != nil {
		return nil, errors.NewAssertionErrorWithWrappedErrf(
			err,
			"cannot execute command \"%s\"",
			strings.Join(cmd, " "),
		)
	}

	if res.exitCode != 0 {
		return &res, errors.NewAssertionErrorWithWrappedErrf(
			errors.Newf("%s", res.stdErr),
			"command \"%s\" exit with code %d:\n %+v",
			strings.Join(cmd, " "),
			res.exitCode,
			res)
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
		fmt.Sprint(serverStartTimeout),
	}, "/cockroach")

	if err != nil {
		return errors.NewAssertionErrorWithWrappedErrf(err, "cannot run fsnotify to listen to %s:\nres:%s", listenURLFile, res)
	}

	if strings.Contains(res.stdOut, "finished\r\n") {
		return nil
	}
	fmt.Println("hello: ", fmt.Sprintf("%#v", res.stdOut))
	return errors.NewAssertionErrorWithWrappedErrf(errors.Newf("%s", res), "error in waiting the server to start")
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
		return nil, errors.NewAssertionErrorWithWrappedErrf(err, "error executing query \"%s\"", sqlQuery)
	}

	return res, nil
}

//rmContainer performs a forced deletion of the current container.
func (dn *dockerNode) rmContainer(ctx context.Context) error {
	if err := dn.cl.ContainerRemove(ctx, dn.contID, types.ContainerRemoveOptions{
		Force: true,
	}); err != nil {
		return errors.NewAssertionErrorWithWrappedErrf(err, "cannot remove container %s", dn.contID)
	}
	dn.contID = ""
	return nil
}

// cleanSQLOutput format the output of sql query to be easier to compare
// with the expected result.
func cleanSQLOutput(s string) string {
	trimDash := regexp.MustCompile(`(\n|^)(\-|\+){3,}`)
	trimNewline := regexp.MustCompile(`(\r|\n){2,}`)
	trimTime := regexp.MustCompile(`\nTime:(.|\s)+`)
	trimRow := regexp.MustCompile(`\n\(\d+ rows?\)`)
	trimSpace := regexp.MustCompile(`\n\s{1,}`)

	var res string

	res = strings.TrimSpace(s)
	res = trimDash.ReplaceAllString(res, "")
	res = trimNewline.ReplaceAllString(res, "\n")
	res = trimTime.ReplaceAllString(res, "")
	res = trimRow.ReplaceAllString(res, "")
	res = trimSpace.ReplaceAllString(res, "\n")

	return res
}
