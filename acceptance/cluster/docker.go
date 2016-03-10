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
	"encoding/json"
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
		return net.IPv4(127, 0, 0, 1)
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

func pullImage(l *LocalCluster, options types.ImagePullOptions) error {
	log.Infof("ImagePull %s:%s starting", options.ImageID, options.Tag)
	defer log.Infof("ImagePull %s:%s complete", options.ImageID, options.Tag)

	rc, err := l.client.ImagePull(context.Background(), options, nil)
	if err != nil {
		return err
	}
	defer rc.Close()
	dec := json.NewDecoder(rc)
	for {
		// Using `interface{}` to avoid dependency on github.com/docker/docker. See
		// https://github.com/docker/engine-api/issues/89.
		var message interface{}
		if err := dec.Decode(&message); err != nil {
			if err == io.EOF {
				_, _ = fmt.Fprintln(os.Stderr)
				return nil
			}
			return err
		}
		// The message is a status bar.
		if log.V(2) {
			log.Infof("ImagePull response: %s", message)
		} else {
			_, _ = fmt.Fprintf(os.Stderr, ".")
		}
	}
}

// createContainer creates a new container using the specified
// options. Per the docker API, the created container is not running
// and must be started explicitly. Note that the passed-in hostConfig
// will be augmented with the necessary settings to use the network
// defined by l.createNetwork().
func createContainer(l *LocalCluster, containerConfig container.Config, hostConfig container.HostConfig, containerName string) (*Container, error) {
	hostConfig.NetworkMode = container.NetworkMode(l.networkID)
	// Disable DNS search under the host machine's domain. This can
	// catch upstream wildcard DNS matching and result in odd behavior.
	hostConfig.DNSSearch = []string{"."}
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
	err := c.cluster.client.ContainerRemove(types.ContainerRemoveOptions{
		ContainerID:   c.id,
		RemoveVolumes: true,
		Force:         true,
	})

	if os.Getenv("CIRCLECI") == "true" {
		// HACK: Removal of docker containers on circleci reports the error:
		// "Driver btrfs failed to remove root filesystem". So if we're running on
		// circleci, just ignore the error, the containers are still removed.
		return nil
	}
	return err
}

// Kill stops a running container, without removing it.
func (c *Container) Kill() error {
	// Paused containers cannot be killed. Attempt to unpause it first
	// (which might fail) before killing.
	_ = c.Unpause()
	if err := c.cluster.client.ContainerKill(c.id, "9"); err != nil && !strings.Contains(err.Error(), "is not running") {
		return err
	}
	c.cluster.expectEvent(c, eventDie)
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
		exp = append(exp, eventDie)
	}
	if err := c.cluster.client.ContainerRestart(c.id, timeoutSeconds); err != nil {
		return err
	}
	c.cluster.expectEvent(c, append(exp, eventRestart)...)
	return nil
}

// Stop a running container.
func (c *Container) Stop(timeoutSeconds int) error {
	if err := c.cluster.client.ContainerStop(c.id, timeoutSeconds); err != nil {
		return err
	}
	c.cluster.expectEvent(c, eventDie)
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

// Addr returns the TCP address to connect to.
func (c *Container) Addr(port nat.Port) *net.TCPAddr {
	containerInfo, err := c.Inspect()
	if err != nil {
		log.Error(err)
		return nil
	}
	bindings, ok := containerInfo.NetworkSettings.Ports[port]
	if !ok || len(bindings) == 0 {
		return nil
	}
	portNum, err := strconv.Atoi(bindings[0].HostPort)
	if err != nil {
		log.Error(err)
		return nil
	}
	return &net.TCPAddr{
		IP:   dockerIP(),
		Port: portNum,
	}
}
