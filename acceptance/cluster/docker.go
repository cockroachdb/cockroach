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
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/docker/engine-api/client"
	"github.com/docker/engine-api/types"
	"github.com/docker/engine-api/types/container"
	"github.com/docker/engine-api/types/network"
	"github.com/docker/go-connections/nat"
	"github.com/pkg/errors"
	"golang.org/x/net/context"

	"github.com/cockroachdb/cockroach/util/log"
)

const matchNone = "^$"

// Retrieve the IP address of docker itself.
func dockerIP() net.IP {
	host := os.Getenv("DOCKER_HOST")
	if host == "" {
		host = client.DefaultDockerHost
	}
	u, err := url.Parse(host)
	if err != nil {
		panic(err)
	}
	if u.Scheme == "unix" {
		return net.IPv4(127, 0, 0, 1)
	}
	h, _, err := net.SplitHostPort(u.Host)
	if err != nil {
		panic(err)
	}
	return net.ParseIP(h)
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

func hasImage(l *LocalCluster, ref string) bool {
	name := strings.Split(ref, ":")[0]
	images, err := l.client.ImageList(context.Background(), types.ImageListOptions{MatchName: name})
	if err != nil {
		log.Fatal(context.TODO(), err)
	}
	for _, image := range images {
		for _, repoTag := range image.RepoTags {
			// The Image.RepoTags field contains strings of the form <repo>:<tag>.
			if ref == repoTag {
				return true
			}
		}
	}
	for _, image := range images {
		for _, tag := range image.RepoTags {
			log.Infof(context.TODO(), "ImageList %s %s", tag, image.ID)
		}
	}
	return false
}

func pullImage(l *LocalCluster, ref string, options types.ImagePullOptions) error {
	// HACK: on CircleCI, docker pulls the image on the first access from an
	// acceptance test even though that image is already present. So we first
	// check to see if our image is present in order to avoid this slowness.
	if hasImage(l, ref) {
		log.Infof(context.TODO(), "ImagePull %s already exists", ref)
		return nil
	}

	log.Infof(context.TODO(), "ImagePull %s starting", ref)
	defer log.Infof(context.TODO(), "ImagePull %s complete", ref)

	rc, err := l.client.ImagePull(context.Background(), ref, options)
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
			log.Infof(context.TODO(), "ImagePull response: %s", message)
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
func createContainer(
	l *LocalCluster,
	containerConfig container.Config,
	hostConfig container.HostConfig,
	containerName string,
) (*Container, error) {
	hostConfig.NetworkMode = container.NetworkMode(l.networkID)
	// Disable DNS search under the host machine's domain. This can
	// catch upstream wildcard DNS matching and result in odd behavior.
	hostConfig.DNSSearch = []string{"."}
	resp, err := l.client.ContainerCreate(context.Background(), &containerConfig, &hostConfig, nil, containerName)
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
	return c.cluster.client.ContainerRemove(context.Background(), c.id, types.ContainerRemoveOptions{
		RemoveVolumes: true,
		Force:         true,
	})
}

// Kill stops a running container, without removing it.
func (c *Container) Kill() error {
	// Paused containers cannot be killed. Attempt to unpause it first
	// (which might fail) before killing.
	_ = c.Unpause()
	if err := c.cluster.client.ContainerKill(context.Background(), c.id, "9"); err != nil && !strings.Contains(err.Error(), "is not running") {
		return err
	}
	c.cluster.expectEvent(c, eventDie)
	return nil
}

// Start starts a non-running container.
//
// TODO(pmattis): Generalize the setting of parameters here.
func (c *Container) Start() error {
	return c.cluster.client.ContainerStart(context.Background(), c.id, types.ContainerStartOptions{})
}

// Pause pauses a running container.
func (c *Container) Pause() error {
	return c.cluster.client.ContainerPause(context.Background(), c.id)
}

// TODO(pmattis): Container.Pause is neither used or tested. Silence unused
// warning.
var _ = (*Container).Pause

// Unpause resumes a paused container.
func (c *Container) Unpause() error {
	return c.cluster.client.ContainerUnpause(context.Background(), c.id)
}

// Restart restarts a running container.
// Container will be killed after 'timeout' seconds if it fails to stop.
func (c *Container) Restart(timeout *time.Duration) error {
	var exp []string
	if ci, err := c.Inspect(); err != nil {
		return err
	} else if ci.State.Running {
		exp = append(exp, eventDie)
	}
	if err := c.cluster.client.ContainerRestart(context.Background(), c.id, timeout); err != nil {
		return err
	}
	c.cluster.expectEvent(c, append(exp, eventRestart)...)
	return nil
}

// Stop a running container.
func (c *Container) Stop(timeout *time.Duration) error {
	if err := c.cluster.client.ContainerStop(context.Background(), c.id, timeout); err != nil {
		return err
	}
	c.cluster.expectEvent(c, eventDie)
	return nil
}

// TODO(pmattis): Container.Stop is neither used or tested. Silence unused
// warning.
var _ = (*Container).Stop

// Wait waits for a running container to exit.
func (c *Container) Wait() error {
	exitCode, err := c.cluster.client.ContainerWait(context.Background(), c.id)
	if err == nil && exitCode != 0 {
		err = errors.Errorf("non-zero exit code: %d", exitCode)
	}
	if err != nil {
		if err := c.Logs(os.Stderr); err != nil {
			log.Warning(context.TODO(), err)
		}
	}
	return err
}

// Logs outputs the containers logs to the given io.Writer.
func (c *Container) Logs(w io.Writer) error {
	rc, err := c.cluster.client.ContainerLogs(context.Background(), c.id, types.ContainerLogsOptions{
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
		if _, err := io.CopyN(w, rc, int64(size)); err != nil {
			return err
		}
	}
	return nil
}

// Inspect retrieves detailed info about a container.
func (c *Container) Inspect() (types.ContainerJSON, error) {
	return c.cluster.client.ContainerInspect(context.Background(), c.id)
}

// Addr returns the TCP address to connect to.
func (c *Container) Addr(port nat.Port) *net.TCPAddr {
	containerInfo, err := c.Inspect()
	if err != nil {
		log.Error(context.TODO(), err)
		return nil
	}
	bindings, ok := containerInfo.NetworkSettings.Ports[port]
	if !ok || len(bindings) == 0 {
		return nil
	}
	portNum, err := strconv.Atoi(bindings[0].HostPort)
	if err != nil {
		log.Error(context.TODO(), err)
		return nil
	}
	return &net.TCPAddr{
		IP:   dockerIP(),
		Port: portNum,
	}
}

// resilientDockerClient handles certain recoverable Docker usage errors.
//
// For example, `ContainerCreate` will fail if a container with the requested
// name already exists. resilientDockerClient will catch this, delete the
// existing container and try again.
type resilientDockerClient struct {
	client.APIClient
}

func (cli resilientDockerClient) ContainerCreate(
	ctx context.Context, config *container.Config, hostConfig *container.HostConfig,
	networkingConfig *network.NetworkingConfig, containerName string,
) (types.ContainerCreateResponse, error) {
	response, err := cli.APIClient.ContainerCreate(ctx, config, hostConfig, networkingConfig, containerName)
	if err != nil && strings.Contains(err.Error(), "already in use") {
		log.Infof(ctx, "unable to create container %s: %v", containerName, err)
		containers, cerr := cli.ContainerList(ctx, types.ContainerListOptions{
			All:   true,
			Limit: -1, // no limit, see docker/engine-api/client/container_list.go
		})
		if cerr != nil {
			log.Infof(ctx, "unable to list containers: %v", cerr)
			return types.ContainerCreateResponse{}, err
		}
		for _, c := range containers {
			for _, n := range c.Names {
				// The container names begin with a "/".
				n = strings.TrimPrefix(n, "/")
				if n != containerName {
					continue
				}
				log.Infof(ctx, "trying to remove %s", c.ID)
				options := types.ContainerRemoveOptions{
					RemoveVolumes: true,
					Force:         true,
				}
				if rerr := cli.ContainerRemove(ctx, c.ID, options); rerr != nil {
					log.Infof(ctx, "unable to remove container: %v", rerr)
					return types.ContainerCreateResponse{}, err
				}
				return cli.ContainerCreate(ctx, config, hostConfig, networkingConfig, containerName)
			}
		}
		log.Warningf(ctx, "error indicated existing container %s, "+
			"but none found:\nerror: %s\ncontainers: %+v",
			containerName, err, containers)
		// We likely raced with a previous (late) removal of the container.
		// Return a timeout so a higher level can retry and hopefully
		// succeed (or get stuck in an infinite loop, at which point at
		// least we'll have gathered an additional bit of information).
		return response, context.DeadlineExceeded
	}
	return response, err
}

func (cli resilientDockerClient) ContainerRemove(ctx context.Context, container string, options types.ContainerRemoveOptions) error {
	err := cli.APIClient.ContainerRemove(ctx, container, options)

	if os.Getenv("CIRCLECI") == "true" {
		// HACK: Removal of docker containers on circleci reports the error:
		// "Driver btrfs failed to remove root filesystem". So if we're running on
		// circleci, just ignore the error, the containers are still removed.
		return nil
	}
	return err
}

// retryingDockerClient proxies the Docker client api and retries problematic
// calls.
//
// Sometimes http requests to the Docker server, on circleci in particular, will
// hang indefinitely and non-deterministically. This leads to flaky tests. To
// avoid this, we wrap some of them in a timeout and retry loop.
type retryingDockerClient struct {
	// Implements client.APIClient, but we use that it's resilient.
	resilientDockerClient
	attempts int
	timeout  time.Duration
}

// retry invokes the supplied function with time-limited contexts as long as
// returned error is a context timeout. When needing more than one attempt to
// get a (non-timeout) result, any errors matching retryErrorsRE are swallowed.
//
// For example, retrying a container removal could fail on the second attempt
// if the first request timed out (but still executed).
func retry(
	ctx context.Context,
	attempts int,
	timeout time.Duration,
	name string,
	retryErrorsRE string,
	f func(ctx context.Context) error,
) error {
	for i := 0; i < attempts; i++ {
		timeoutCtx, _ := context.WithTimeout(ctx, timeout)
		err := f(timeoutCtx)
		if err != nil {
			// TODO(tamird):
			// s/client.ErrConnectionFailed/context.DeadlineExceeded/ when
			// https://github.com/docker/engine-api/issues/347 is fixed.
			if err == client.ErrConnectionFailed {
				continue
			} else if i > 0 && retryErrorsRE != matchNone {
				if regexp.MustCompile(retryErrorsRE).MatchString(err.Error()) {
					log.Infof(ctx, "%s: swallowing expected error after retry: %v",
						name, err)
					return nil
				}
			}
		}
		return err
	}
	return fmt.Errorf("%s: exceeded %d tries with a %s timeout", name, attempts, timeout)
}

func (cli retryingDockerClient) ContainerCreate(
	ctx context.Context,
	config *container.Config,
	hostConfig *container.HostConfig,
	networkingConfig *network.NetworkingConfig,
	containerName string,
) (types.ContainerCreateResponse, error) {
	var ret types.ContainerCreateResponse
	err := retry(ctx, cli.attempts, cli.timeout,
		"ContainerCreate", matchNone,
		func(timeoutCtx context.Context) error {
			var err error
			ret, err = cli.resilientDockerClient.ContainerCreate(timeoutCtx, config, hostConfig, networkingConfig, containerName)
			_ = ret // silence incorrect unused warning
			return err
		})
	return ret, err
}

func (cli retryingDockerClient) ContainerStart(ctx context.Context, container string, options types.ContainerStartOptions) error {
	return retry(ctx, cli.attempts, cli.timeout, "ContainerStart", matchNone,
		func(timeoutCtx context.Context) error {
			return cli.resilientDockerClient.ContainerStart(timeoutCtx, container, options)
		})
}

func (cli retryingDockerClient) ContainerRemove(ctx context.Context, container string, options types.ContainerRemoveOptions) error {
	return retry(ctx, cli.attempts, cli.timeout, "ContainerRemove", "No such container",
		func(timeoutCtx context.Context) error {
			return cli.resilientDockerClient.ContainerRemove(timeoutCtx, container, options)
		})
}

func (cli retryingDockerClient) ContainerKill(ctx context.Context, container, signal string) error {
	return retry(ctx, cli.attempts, cli.timeout, "ContainerKill",
		"Container .* is not running",
		func(timeoutCtx context.Context) error {
			return cli.resilientDockerClient.ContainerKill(timeoutCtx, container, signal)
		})
}

func (cli retryingDockerClient) ContainerWait(ctx context.Context, container string) (int, error) {
	var ret int
	return ret, retry(ctx, cli.attempts, cli.timeout, "ContainerWait", matchNone,
		func(timeoutCtx context.Context) error {
			var err error
			ret, err = cli.resilientDockerClient.ContainerWait(timeoutCtx, container)
			_ = ret // silence incorrect unused warning
			return err
		})
}

func (cli retryingDockerClient) ImageList(
	ctx context.Context, options types.ImageListOptions,
) ([]types.Image, error) {
	var ret []types.Image
	return ret, retry(ctx, cli.attempts, cli.timeout, "ImageList", matchNone,
		func(timeoutCtx context.Context) error {
			var err error
			ret, err = cli.resilientDockerClient.ImageList(timeoutCtx, options)
			_ = ret // silence incorrect unused warning
			return err
		})
}

func (cli retryingDockerClient) ImagePull(
	ctx context.Context, ref string, options types.ImagePullOptions,
) (io.ReadCloser, error) {
	// Image pulling is potentially slow. Make sure the timeout is sufficient.
	timeout := cli.timeout
	if minTimeout := 2 * time.Minute; timeout < minTimeout {
		timeout = minTimeout
	}
	var ret io.ReadCloser
	return ret, retry(ctx, cli.attempts, timeout, "ImagePull", matchNone,
		func(timeoutCtx context.Context) error {
			var err error
			ret, err = cli.resilientDockerClient.ImagePull(timeoutCtx, ref, options)
			_ = ret // silence incorrect unused warning
			return err
		})
}
