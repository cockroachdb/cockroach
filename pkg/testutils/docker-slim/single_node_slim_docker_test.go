// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

// Package dockerslim contains an integration test for the slim CockroachDB
// Docker image defined in build/deploy-slim/Dockerfile. The slim image has
// no cockroach.sh entrypoint wrapper — no cert auto-generation, no
// /docker-entrypoint-initdb.d processing, no shell-mode commands — so this
// test drives the container by invoking `cockroach start-single-node`
// directly and using `docker exec ./cockroach sql` to issue SQL.
package dockerslim

import (
	"bytes"
	"context"
	"encoding/binary"
	"io"
	"math"
	"strings"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/build/bazel"
	"github.com/cockroachdb/cockroach/pkg/testutils/skip"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
	"github.com/docker/docker/api/types"
	"github.com/docker/docker/api/types/container"
	"github.com/docker/docker/api/types/filters"
	"github.com/docker/docker/client"
	"github.com/docker/docker/pkg/stdcopy"
	"github.com/docker/go-connections/nat"
)

const (
	// imageName is the tag produced by build/github/docker-image-slim.sh.
	imageName = "cockroachdb/cockroach-slim:latest"
	// containerName is fixed so leftover containers from earlier runs are
	// easy to locate and remove.
	containerName = "roach-slim"
	// cockroachEntrypoint is the path to the binary inside the image.
	// `docker exec` can run it even though distroless has no shell.
	cockroachEntrypoint = "/cockroach/cockroach"

	httpPort      = "8080"
	cockroachPort = "26257"
	hostIP        = "127.0.0.1"

	// memstoreSize must exceed cockroach's 640 MiB minimum for in-memory stores.
	memstoreSize = "1GiB"

	execTimeout = 30 * time.Second
	// readyTimeout is generous because the first SQL probe happens shortly
	// after container start; cockroach needs a few seconds to accept conns.
	readyTimeout = 90 * time.Second
	// readyPollInterval controls how often we re-issue SELECT 1 until the
	// server accepts SQL traffic.
	readyPollInterval = 500 * time.Millisecond
)

// TestSingleNodeSlimDocker verifies that the slim image boots a
// single-node cluster in insecure mode and serves SQL. It is intentionally
// minimal: the slim image has no init-file machinery to exercise, and
// running extended scenarios belongs in the standard-image test.
func TestSingleNodeSlimDocker(t *testing.T) {
	if !bazel.BuiltWithBazel() {
		skip.IgnoreLint(t)
	}

	ctx := context.Background()

	cl, err := client.NewClientWithOpts(client.FromEnv)
	if err != nil {
		t.Fatal(err)
	}
	cl.NegotiateAPIVersion(ctx)

	dn := dockerNode{cl: cl}

	// Remove any leftover containers from previous runs before we start a new
	// one — leftover state from a crashed run would break container creation
	// under the fixed name.
	if err := timeutil.RunWithTimeout(ctx, "remove pre-existing containers", execTimeout,
		dn.removeAllContainers,
	); err != nil {
		t.Fatal(err)
	}

	if err := timeutil.RunWithTimeout(ctx, "start container", execTimeout,
		func(ctx context.Context) error {
			return dn.startContainer(ctx)
		},
	); err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := dn.rmContainer(context.Background()); err != nil {
			t.Logf("cleanup: %v", err)
		}
	}()

	if err := timeutil.RunWithTimeout(ctx, "wait for SQL readiness", readyTimeout,
		dn.waitForSQLReady,
	); err != nil {
		logErr := dn.showContainerLog(context.Background())
		if logErr != nil {
			t.Logf("additionally, cannot read container logs: %v", logErr)
		}
		t.Fatal(err)
	}

	// A short script of CRUD-style statements confirms the database accepts
	// DDL and DML end-to-end. Output is compared with Contains so that
	// incidental formatting (column widths, blank lines) doesn't cause churn.
	queries := []struct {
		query    string
		contains string
	}{
		{"SELECT 1", "1"},
		{"CREATE DATABASE slimtest", "CREATE DATABASE"},
		{"CREATE TABLE slimtest.t (x INT)", "CREATE TABLE"},
		{"INSERT INTO slimtest.t VALUES (1), (2), (3)", "INSERT 0 3"},
		{"SELECT sum(x) FROM slimtest.t", "6"},
	}
	for _, q := range queries {
		q := q
		t.Run(q.query, func(t *testing.T) {
			out, err := dn.execSQL(ctx, q.query)
			if err != nil {
				t.Fatalf("%v", err)
			}
			if !strings.Contains(out, q.contains) {
				t.Fatalf("%q: output %q does not contain %q", q.query, out, q.contains)
			}
		})
	}
}

// dockerNode wraps a Docker API client along with the id of the container
// the test is currently driving.
type dockerNode struct {
	cl     client.APIClient
	contID string
}

// startContainer creates and starts a slim-image container running
// `start-single-node` in insecure mode with an in-memory store.
func (dn *dockerNode) startContainer(ctx context.Context) error {
	config := container.Config{
		Hostname:     containerName,
		Image:        imageName,
		ExposedPorts: nat.PortSet{httpPort: struct{}{}, cockroachPort: struct{}{}},
		Cmd: []string{
			"start-single-node",
			"--insecure",
			"--store=type=mem,size=" + memstoreSize,
			"--listen-addr=:" + cockroachPort,
			"--http-addr=:" + httpPort,
		},
	}
	hostConfig := container.HostConfig{
		PortBindings: map[nat.Port][]nat.PortBinding{
			nat.Port(httpPort):      {{HostIP: hostIP, HostPort: httpPort}},
			nat.Port(cockroachPort): {{HostIP: hostIP, HostPort: cockroachPort}},
		},
	}
	resp, err := dn.cl.ContainerCreate(ctx, &config, &hostConfig, nil, nil, containerName)
	if err != nil {
		return errors.Wrap(err, "create container")
	}
	dn.contID = resp.ID
	if err := dn.cl.ContainerStart(ctx, dn.contID, types.ContainerStartOptions{}); err != nil {
		return errors.Wrap(err, "start container")
	}
	return nil
}

// waitForSQLReady polls `SELECT 1` until the server responds or the context
// deadline fires. This replaces the fsnotify-on-init_success readiness
// signal used by the standard-image test, which depends on cockroach.sh —
// the slim image has no such wrapper.
func (dn *dockerNode) waitForSQLReady(ctx context.Context) error {
	var lastErr error
	for {
		select {
		case <-ctx.Done():
			return errors.CombineErrors(ctx.Err(), lastErr)
		default:
		}
		out, err := dn.execSQL(ctx, "SELECT 1")
		if err == nil && strings.Contains(out, "1") {
			return nil
		}
		lastErr = err
		select {
		case <-ctx.Done():
			return errors.CombineErrors(ctx.Err(), lastErr)
		case <-time.After(readyPollInterval):
		}
	}
}

// execSQL runs `cockroach sql --insecure -e <query>` inside the running
// container via `docker exec`, returning the combined stdout output.
func (dn *dockerNode) execSQL(ctx context.Context, query string) (string, error) {
	cmd := []string{
		cockroachEntrypoint, "sql",
		"--insecure",
		"--host=localhost:" + cockroachPort,
		"-e", query,
	}
	execID, err := dn.cl.ContainerExecCreate(ctx, dn.contID, types.ExecConfig{
		AttachStderr: true,
		AttachStdout: true,
		Cmd:          cmd,
	})
	if err != nil {
		return "", errors.Wrapf(err, "exec create for query %q", query)
	}
	resp, err := dn.cl.ContainerExecAttach(ctx, execID.ID, types.ExecStartCheck{})
	if err != nil {
		return "", errors.Wrapf(err, "exec attach for query %q", query)
	}
	defer resp.Close()

	var outBuf, errBuf bytes.Buffer
	if _, err := stdcopy.StdCopy(&outBuf, &errBuf, resp.Reader); err != nil {
		return "", errors.Wrap(err, "exec stdcopy")
	}
	inspect, err := dn.cl.ContainerExecInspect(ctx, execID.ID)
	if err != nil {
		return "", errors.Wrap(err, "exec inspect")
	}
	if inspect.ExitCode != 0 {
		return outBuf.String(), errors.Newf(
			"query %q exited %d; stderr: %s",
			query, inspect.ExitCode, errBuf.String(),
		)
	}
	return outBuf.String(), nil
}

// showContainerLog streams the container's logs to the test's stderr via
// t.Logf. Used on failure to make diagnosis possible without re-running.
func (dn *dockerNode) showContainerLog(ctx context.Context) error {
	rc, err := dn.cl.ContainerLogs(ctx, dn.contID, types.ContainerLogsOptions{
		ShowStdout: true,
		ShowStderr: true,
	})
	if err != nil {
		return errors.Wrap(err, "container logs")
	}
	defer rc.Close()

	// Docker's log stream framing: [1 byte fd][3 bytes pad][4 bytes length][payload].
	for {
		var header uint64
		if err := binary.Read(rc, binary.BigEndian, &header); err == io.EOF {
			return nil
		} else if err != nil {
			return err
		}
		size := header & math.MaxUint32
		if _, err := io.CopyN(io.Discard, rc, int64(size)); err != nil {
			return err
		}
	}
}

// removeAllContainers force-removes every container derived from the slim
// image. The test uses a fixed container name, so leftover state from a
// crashed run would block a fresh start.
func (dn *dockerNode) removeAllContainers(ctx context.Context) error {
	filter := filters.NewArgs(filters.Arg("ancestor", imageName))
	conts, err := dn.cl.ContainerList(ctx, types.ContainerListOptions{All: true, Filters: filter})
	if err != nil {
		return errors.Wrapf(err, "list containers on image %s", imageName)
	}
	for _, c := range conts {
		if err := dn.cl.ContainerRemove(ctx, c.ID, types.ContainerRemoveOptions{Force: true}); err != nil {
			return errors.Wrapf(err, "remove container %v", c.Names)
		}
	}
	return nil
}

// rmContainer force-removes the container the test is currently driving.
func (dn *dockerNode) rmContainer(ctx context.Context) error {
	if dn.contID == "" {
		return nil
	}
	if err := dn.cl.ContainerRemove(ctx, dn.contID, types.ContainerRemoveOptions{Force: true}); err != nil {
		return errors.Wrapf(err, "remove container %s", dn.contID)
	}
	dn.contID = ""
	return nil
}
