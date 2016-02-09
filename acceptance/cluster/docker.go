// Copyright 2015 The Cockroach Authors.
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
// Author: Peter Mattis (peter@cockroachlabs.com)

package cluster

import (
	"encoding/binary"
	"fmt"
	"io"
	"math"
	"net"
	"net/url"
	"os"
	"runtime"
	"strconv"
	"strings"

	"github.com/docker/engine-api/types"
	"github.com/docker/engine-api/types/container"
	"github.com/docker/go-connections/nat"
	"golang.org/x/net/context"

	"github.com/cockroachdb/cockroach/util/log"
)

// Retrieve the IP address of docker itself.
func dockerIP() net.IP {
	if host := os.Getenv("DOCKER_HOST"); host != "" {
		u, err := url.Parse(host)
		if err != nil {
			panic(err)
		}
		h, _, err := net.SplitHostPort(u.Host)
		if err != nil {
			panic(err)
		}
		return net.ParseIP(h)
	}
	if runtime.GOOS == "linux" {
		return net.IPv4zero
	}
	panic("unable to determine docker ip address")
}

// Container provides the programmatic interface for a single docker
// container.
type Container struct {
	id      string
	name    string
	cluster *LocalCluster
}

// Name returns the container's name.
func (c Container) Name() string {
	return c.name
}

// createContainer creates a new container using the specified options. Per the
// docker API, the created container is not running and must be started
// explicitly.
func createContainer(l *LocalCluster, containerConfig container.Config, hostConfig container.HostConfig, containerName string) (*Container, error) {
	resp, err := l.client.ContainerCreate(&containerConfig, &hostConfig, nil, containerName)
	if err != nil {
		return nil, err
	}
	return &Container{
		id:      resp.ID,
		name:    containerName,
		cluster: l,
	}, nil
}

func maybePanic(err error) {
	if err != nil {
		panic(err)
	}
}

// Remove removes the container from docker. It is an error to remove a running
// container.
func (c *Container) Remove() error {
	if os.Getenv("CIRCLECI") == "true" {
		// HACK: Removal of docker containers fails on circleci with the error:
		// "Driver btrfs failed to remove root filesystem". So if we're running on
		// circleci, just leave the containers around.
		return nil
	}
	return c.cluster.client.ContainerRemove(types.ContainerRemoveOptions{
		ContainerID:   c.id,
		RemoveVolumes: true,
	})
}

// Kill stops a running container, without removing it.
func (c *Container) Kill() error {
	// Paused containers cannot be killed. Attempt to unpause it first
	// (which might fail) before killing.
	_ = c.Unpause()
	if err := c.cluster.client.ContainerKill(c.id, "9"); err != nil && !strings.Contains(err.Error(), "is not running") {
		return err
	}
	c.cluster.expectEvent(c, "die")
	return nil
}

// Start starts a non-running container.
//
// TODO(pmattis): Generalize the setting of parameters here.
func (c *Container) Start() error {
	return c.cluster.client.ContainerStart(c.id)
}

// Pause pauses a running container.
func (c *Container) Pause() error {
	return c.cluster.client.ContainerPause(c.id)
}

// Unpause resumes a paused container.
func (c *Container) Unpause() error {
	return c.cluster.client.ContainerUnpause(c.id)
}

// Restart restarts a running container.
// Container will be killed after 'timeout' seconds if it fails to stop.
func (c *Container) Restart(timeoutSeconds int) error {
	var exp []string
	if ci, err := c.Inspect(); err != nil {
		return err
	} else if ci.State.Running {
		exp = append(exp, "die")
	}
	if err := c.cluster.client.ContainerRestart(c.id, timeoutSeconds); err != nil {
		return err
	}
	c.cluster.expectEvent(c, append(exp, "restart")...)
	return nil
}

// Stop a running container.
func (c *Container) Stop(timeoutSeconds int) error {
	if err := c.cluster.client.ContainerStop(c.id, timeoutSeconds); err != nil {
		return err
	}
	c.cluster.expectEvent(c, "die")
	return nil
}

// Wait waits for a running container to exit.
func (c *Container) Wait() error {
	exitCode, err := c.cluster.client.ContainerWait(context.Background(), c.id)
	if err != nil {
		return err
	}
	if exitCode != 0 {
		if err := c.Logs(os.Stderr); err != nil {
			log.Warning(err)
		}
		return fmt.Errorf("non-zero exit code: %d", exitCode)
	}
	return nil
}

// Logs outputs the containers logs to the given io.Writer.
func (c *Container) Logs(w io.Writer) error {
	rc, err := c.cluster.client.ContainerLogs(context.Background(), types.ContainerLogsOptions{
		ContainerID: c.id,
		ShowStdout:  true,
		ShowStderr:  true,
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
		if _, err := io.CopyN(w, rc, int64(size)); err != nil {
			return err
		}
	}
	return nil
}

// Inspect retrieves detailed info about a container.
func (c *Container) Inspect() (types.ContainerJSON, error) {
	return c.cluster.client.ContainerInspect(c.id)
}

// Addr returns the address to connect to the specified port.
func (c *Container) Addr(name nat.Port) *net.TCPAddr {
	containerInfo, err := c.Inspect()
	if err != nil {
		return nil
	}
	if name == "" {
		name = cockroachTCP
	}
	bindings, ok := containerInfo.NetworkSettings.Ports[name]
	if !ok || len(bindings) == 0 {
		return nil
	}
	port, _ := strconv.Atoi(bindings[0].HostPort)
	return &net.TCPAddr{
		IP:   dockerIP(),
		Port: port,
	}
}

// PGAddr returns the address to connect to the Postgres port.
func (c *Container) PGAddr() *net.TCPAddr {
	return c.Addr(pgTCP)
}

// GetJSON retrieves the URL specified by https://Addr(<port>)<path>
// and unmarshals the result as JSON.
func (c *Container) GetJSON(port nat.Port, path string, v interface{}) error {
	return getJSON(true /* tls */, c.Addr(port).String(), path, v)
}
