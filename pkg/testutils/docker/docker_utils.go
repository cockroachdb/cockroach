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
	"encoding/binary"
	"fmt"
	"io"
	"io/ioutil"
	"math"
	"os"
	"regexp"
	"strings"

	"github.com/docker/docker/api/types"
	"github.com/docker/docker/api/types/container"
	"github.com/docker/docker/api/types/filters"
	"github.com/docker/docker/client"
	"github.com/docker/docker/pkg/stdcopy"
	"github.com/docker/go-connections/nat"
	"golang.org/x/net/context"
)

const (
	imageName           = "cockroachdb/cockroach-ci:latest"
	defaultTimeout      = 10
	serverStartTimeout  = 80
	listenUrlFile       = "demoFile"
	cockroachEntrypoint = "./cockroach"
	hostPort            = "8080"
	cockroachPort       = "26275"
	hostIP              = "127.0.0.1"
)

type dockerNode struct {
	cl     client.APIClient
	contId string
}

// removeLocalData removes existing database saved in cockroach-data.
func removeLocalData() error {
	err := os.RemoveAll("./cockroach-data")
	if err != nil {
		return fmt.Errorf("cannot remove local data: %v", err)
	}
	return nil
}

// showContainerLog outputs the container's logs to the logFile and stderr.
func (dn *dockerNode) showContainerLog(ctx context.Context, logFileName string) error {

	cmdLog, err := os.Create(logFileName)
	out := io.MultiWriter(cmdLog, os.Stderr)

	rc, err := dn.cl.ContainerLogs(ctx, dn.contId, types.ContainerLogsOptions{
		ShowStdout: true,
		ShowStderr: true,
	})
	if err != nil {
		return err
	}
	defer rc.Close()
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
		Cmd:          append(cmd, fmt.Sprintf("--listening-url-file=%s", listenUrlFile)),
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
		return fmt.Errorf("cannot create container: %v", err)
	}
	fmt.Println("id:", resp.ID)

	dn.contId = resp.ID

	if err := dn.cl.ContainerStart(ctx, dn.contId,
		types.ContainerStartOptions{}); err != nil {
		return fmt.Errorf("cannot start container: %v", err)
	}

	return nil
}

// removeAllContainers removes all running containers by force.
func (dn *dockerNode) removeAllContainers(ctx context.Context) error {
	filter := filters.NewArgs(filters.Arg("ancestor", imageName))
	conts, err := dn.cl.ContainerList(ctx,
		types.ContainerListOptions{All: true, Filters: filter})
	if err != nil {
		return fmt.Errorf("cannot list all conts: %v", err)
	}
	for _, cont := range conts {
		err := dn.cl.ContainerRemove(ctx, cont.ID,
			types.ContainerRemoveOptions{Force: true})
		if err != nil {
			return fmt.Errorf("cannot remove cont %s: %v", cont.Names, err)
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
func (dn *dockerNode) InspectExecResp(ctx context.Context, execId string) (execResult, error) {
	var execRes execResult
	resp, err := dn.cl.ContainerExecAttach(ctx, execId, types.ExecStartCheck{})
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

	res, err := dn.cl.ContainerExecInspect(ctx, execId)
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
	execId, err := dn.cl.ContainerExecCreate(ctx, dn.contId, types.ExecConfig{
		User:         "root",
		AttachStderr: true,
		AttachStdout: true,
		Tty:          true,
		Cmd:          cmd,
		WorkingDir:   workingDir,
	})

	if err != nil {
		return nil, fmt.Errorf(
			"cannot create command \"%s\":\n %v",
			strings.Join(cmd, " "),
			err,
		)
	}

	res, err := dn.InspectExecResp(ctx, execId.ID)
	if err != nil {
		return nil, fmt.Errorf(
			"cannot execute command \"%s\":\n %v",
			strings.Join(cmd[:], " "),
			err,
		)
	}

	if res.exitCode != 0 {
		return &res, fmt.Errorf(
			"command \"%s\" exit with code %d:\n %+v",
			strings.Join(cmd[:], " "),
			res.exitCode,
			res)
	}

	return &res, nil
}

// waitServerStarts waits till the server truly starts or timeout, whichever
// earlier. It keeps listening to the listenUrlFile till it is closed and
// written. This is because in #70238, the improved init process for single-node
// server is to start the server, run the init process, and then restart the
// server. We mark it as fully started until the server is successfully
// restarted, and hence write the url to listenUrlFile.
func (dn *dockerNode) waitServerStarts(ctx context.Context) error {
	var res *execResult
	var err error

	// Install go in the docker container.
	res, err = dn.execCommand(ctx, []string{
		"microdnf",
		"install",
		"go",
	}, "/")
	if err != nil || res.exitCode != 0 {
		return fmt.Errorf("cannot install go:\nerr: %v,\nres:%v", err, res)
	}

	// Install dependencies in /docker-fsnotify.
	res, err = dn.execCommand(ctx, []string{
		"go",
		"mod",
		"tidy",
	}, "/docker-fsnotify")
	if err != nil {
		return fmt.Errorf("cannot go mod tidy docker-fsnotify: \nerr: %v,\nres:%v", err, res)
	}

	// Run the file which listen to the /cockroach folder until ei
	res, err = dn.execCommand(ctx, []string{
		"go",
		"run",
		"./ListenFileCreation.go",
		"/cockroach",
		listenUrlFile,
		fmt.Sprint(serverStartTimeout),
	}, "/docker-fsnotify")

	if err != nil {
		return fmt.Errorf("cannot run fsnotify to listen to file:\nerr: %v\nres:%v", err, res)
	}

	if res.stdOut == "finished\r\n" {
		return nil
	} else {
		return fmt.Errorf("error in waiting the server to start: %s", res.stdErr)
	}
}

// execSqlQuery executes the sql query and returns the server's output and
// possible error.
func (dn *dockerNode) execSqlQuery(
	ctx context.Context, sqlQuery string, sqlQueryOpts []string,
) (*execResult, error) {
	query := append([]string{cockroachEntrypoint, "sql", "-e", sqlQuery},
		sqlQueryOpts...,
	)

	res, err := dn.execCommand(ctx, query, "/cockroach")
	if err != nil {
		return nil, fmt.Errorf("error executing query \"%s\": \n%v", sqlQuery, err)
	}

	return res, nil
}

//rmContainer performs a forced deletion of the current container.
func (dn *dockerNode) rmContainer(ctx context.Context) error {
	if err := dn.cl.ContainerRemove(ctx, dn.contId, types.ContainerRemoveOptions{
		Force: true,
	}); err != nil {
		return fmt.Errorf("cannot remove container %s: %v", dn.contId, err)
	}
	dn.contId = ""
	return nil
}

// cleanSqlOutput format the output of sql query to be easier to compare
// with the expected result.
func cleanSqlOutput(s string) string {
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
