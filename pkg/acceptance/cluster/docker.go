// Copyright 2015 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package cluster

import (
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"math"
	"net"
	"net/url"
	"os"
	"os/user"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/cockroachdb/cockroach/pkg/util/contextutil"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/log/severity"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/errors/oserror"
	"github.com/docker/distribution/reference"
	"github.com/docker/docker/api/types"
	"github.com/docker/docker/api/types/container"
	"github.com/docker/docker/api/types/filters"
	"github.com/docker/docker/api/types/network"
	"github.com/docker/docker/client"
	"github.com/docker/docker/pkg/jsonmessage"
	"github.com/docker/go-connections/nat"
	isatty "github.com/mattn/go-isatty"
	specs "github.com/opencontainers/image-spec/specs-go/v1"
)

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
	cluster *DockerCluster
}

// Name returns the container's name.
func (c Container) Name() string {
	return c.name
}

func hasImage(ctx context.Context, l *DockerCluster, ref string) error {
	distributionRef, err := reference.ParseNamed(ref)
	if err != nil {
		return err
	}
	path := reference.Path(distributionRef)
	// Correct for random docker stupidity:
	//
	// https://github.com/moby/moby/blob/7248742/registry/service.go#L207:L215
	path = strings.TrimPrefix(path, "library/")

	images, err := l.client.ImageList(ctx, types.ImageListOptions{
		All: true,
		Filters: filters.NewArgs(
			filters.Arg("reference", path),
		),
	})
	if err != nil {
		return err
	}

	tagged, ok := distributionRef.(reference.Tagged)
	if !ok {
		return errors.Errorf("untagged reference %s not permitted", ref)
	}

	wanted := fmt.Sprintf("%s:%s", path, tagged.Tag())
	for _, image := range images {
		for _, repoTag := range image.RepoTags {
			// The Image.RepoTags field contains strings of the form <path>:<tag>.
			if repoTag == wanted {
				return nil
			}
		}
	}
	var imageList []string
	for _, image := range images {
		for _, tag := range image.RepoTags {
			imageList = append(imageList, "%s %s", tag, image.ID)
		}
	}
	return errors.Errorf("%s not found in:\n%s", wanted, strings.Join(imageList, "\n"))
}

func pullImage(
	ctx context.Context, l *DockerCluster, ref string, options types.ImagePullOptions,
) error {
	// HACK: on CircleCI, docker pulls the image on the first access from an
	// acceptance test even though that image is already present. So we first
	// check to see if our image is present in order to avoid this slowness.
	if hasImage(ctx, l, ref) == nil {
		log.Infof(ctx, "ImagePull %s already exists", ref)
		return nil
	}

	log.Infof(ctx, "ImagePull %s starting", ref)
	defer log.Infof(ctx, "ImagePull %s complete", ref)

	rc, err := l.client.ImagePull(ctx, ref, options)
	if err != nil {
		return err
	}
	defer rc.Close()
	out := os.Stderr
	outFd := out.Fd()
	isTerminal := isatty.IsTerminal(outFd)

	if err := jsonmessage.DisplayJSONMessagesStream(rc, out, outFd, isTerminal, nil); err != nil {
		return err
	}
	if err := hasImage(ctx, l, ref); err != nil {
		return errors.Wrapf(err, "pulled image %s but still don't have it", ref)
	}
	return nil
}

// splitBindSpec splits a Docker bind specification into its host and container
// paths.
func splitBindSpec(bind string) (hostPath string, containerPath string) {
	s := strings.SplitN(bind, ":", 2)
	return s[0], s[1]
}

// getNonRootContainerUser determines a non-root UID and GID to use in the
// container to minimize file ownership problems in bind mounts. It returns a
// UID:GID string suitable for use as the User field container.Config.
func getNonRootContainerUser() (string, error) {
	// This number is Debian-specific, but for now all of our acceptance test
	// containers are based on Debian.
	// See: https://www.debian.org/doc/debian-policy/#uid-and-gid-classes
	const minUnreservedID = 101
	user, err := user.Current()
	if err != nil {
		return "", err
	}
	uid, err := strconv.Atoi(user.Uid)
	if err != nil {
		return "", errors.Wrap(err, "looking up host UID")
	}
	if uid < minUnreservedID {
		return "", fmt.Errorf("host UID %d in container's reserved UID space", uid)
	}
	gid, err := strconv.Atoi(user.Gid)
	if err != nil {
		return "", errors.Wrap(err, "looking up host GID")
	}
	if gid < minUnreservedID {
		// If the GID is in the reserved space, silently upconvert to the known-good
		// UID. We don't want to return an error because users on a macOS host
		// typically have a GID in the reserved space, and this upconversion has
		// been empirically verified to not cause ownership issues.
		gid = uid
	}
	return fmt.Sprintf("%d:%d", uid, gid), nil
}

// createContainer creates a new container using the specified
// options. Per the docker API, the created container is not running
// and must be started explicitly. Note that the passed-in hostConfig
// will be augmented with the necessary settings to use the network
// defined by l.createNetwork().
func createContainer(
	ctx context.Context,
	l *DockerCluster,
	containerConfig container.Config,
	hostConfig container.HostConfig,
	platformSpec specs.Platform,
	containerName string,
) (*Container, error) {
	hostConfig.NetworkMode = container.NetworkMode(l.networkID)
	// Disable DNS search under the host machine's domain. This can
	// catch upstream wildcard DNS matching and result in odd behavior.
	hostConfig.DNSSearch = []string{"."}

	// Run the container as the current user to avoid creating root-owned files
	// and directories from within the container.
	user, err := getNonRootContainerUser()
	if err != nil {
		return nil, err
	}
	containerConfig.User = user

	// Additionally ensure that the host side of every bind exists. Otherwise, the
	// Docker daemon will create the host directory as root before running the
	// container.
	for _, bind := range hostConfig.Binds {
		hostPath, _ := splitBindSpec(bind)
		if _, err := os.Stat(hostPath); oserror.IsNotExist(err) {
			maybePanic(os.MkdirAll(hostPath, 0755))
		} else {
			maybePanic(err)
		}
	}

	resp, err := l.client.ContainerCreate(ctx, &containerConfig, &hostConfig, nil, &platformSpec, containerName)
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
func (c *Container) Remove(ctx context.Context) error {
	return c.cluster.client.ContainerRemove(ctx, c.id, types.ContainerRemoveOptions{
		RemoveVolumes: true,
		Force:         true,
	})
}

// Kill stops a running container, without removing it.
func (c *Container) Kill(ctx context.Context) error {
	if err := c.cluster.client.ContainerKill(ctx, c.id, "9"); err != nil && !strings.Contains(err.Error(), "is not running") {
		return err
	}
	c.cluster.expectEvent(c, eventDie)
	return nil
}

// Start starts a non-running container.
//
// TODO(pmattis): Generalize the setting of parameters here.
func (c *Container) Start(ctx context.Context) error {
	return c.cluster.client.ContainerStart(ctx, c.id, types.ContainerStartOptions{})
}

// Restart restarts a running container.
// Container will be killed after 'timeout' seconds if it fails to stop.
func (c *Container) Restart(ctx context.Context, timeout *time.Duration) error {
	var exp []string
	if ci, err := c.Inspect(ctx); err != nil {
		return err
	} else if ci.State.Running {
		exp = append(exp, eventDie)
	}
	if err := c.cluster.client.ContainerRestart(ctx, c.id, timeout); err != nil {
		return err
	}
	c.cluster.expectEvent(c, append(exp, eventRestart)...)
	return nil
}

// Wait waits for a running container to exit.
func (c *Container) Wait(ctx context.Context, condition container.WaitCondition) error {
	waitOKBodyCh, errCh := c.cluster.client.ContainerWait(ctx, c.id, condition)
	select {
	case err := <-errCh:
		return err
	case waitOKBody := <-waitOKBodyCh:
		outputLog := filepath.Join(c.cluster.volumesDir, "logs", "console-output.log")
		cmdLog, err := os.Create(outputLog)
		if err != nil {
			return err
		}
		defer cmdLog.Close()

		out := io.MultiWriter(cmdLog, os.Stderr)
		if err := c.Logs(ctx, out); err != nil {
			log.Warningf(ctx, "%v", err)
		}

		if exitCode := waitOKBody.StatusCode; exitCode != 0 {
			err = errors.Errorf("non-zero exit code: %d", exitCode)
			fmt.Fprintln(out, err.Error())
			log.Shoutf(ctx, severity.INFO, "command left-over files in %s", c.cluster.volumesDir)
		}

		return err
	}
}

// Logs outputs the containers logs to the given io.Writer.
func (c *Container) Logs(ctx context.Context, w io.Writer) error {
	rc, err := c.cluster.client.ContainerLogs(ctx, c.id, types.ContainerLogsOptions{
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
func (c *Container) Inspect(ctx context.Context) (types.ContainerJSON, error) {
	return c.cluster.client.ContainerInspect(ctx, c.id)
}

// Addr returns the TCP address to connect to.
func (c *Container) Addr(ctx context.Context, port nat.Port) *net.TCPAddr {
	containerInfo, err := c.Inspect(ctx)
	if err != nil {
		log.Errorf(ctx, "%v", err)
		return nil
	}
	bindings, ok := containerInfo.NetworkSettings.Ports[port]
	if !ok || len(bindings) == 0 {
		return nil
	}
	portNum, err := strconv.Atoi(bindings[0].HostPort)
	if err != nil {
		log.Errorf(ctx, "%v", err)
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

func (cli resilientDockerClient) ContainerStart(
	clientCtx context.Context, id string, opts types.ContainerStartOptions,
) error {
	for {
		err := contextutil.RunWithTimeout(clientCtx, "start container", 20*time.Second, func(ctx context.Context) error {
			return cli.APIClient.ContainerStart(ctx, id, opts)
		})

		// Keep going if ContainerStart timed out, but client's context is not
		// expired.
		if errors.Is(err, context.DeadlineExceeded) && clientCtx.Err() == nil {
			log.Warningf(clientCtx, "ContainerStart timed out, retrying")
			continue
		}
		return err
	}
}

func (cli resilientDockerClient) ContainerCreate(
	ctx context.Context,
	config *container.Config,
	hostConfig *container.HostConfig,
	networkingConfig *network.NetworkingConfig,
	platformSpec *specs.Platform,
	containerName string,
) (container.ContainerCreateCreatedBody, error) {
	response, err := cli.APIClient.ContainerCreate(
		ctx, config, hostConfig, networkingConfig, platformSpec, containerName,
	)
	if err != nil && strings.Contains(err.Error(), "already in use") {
		log.Infof(ctx, "unable to create container %s: %v", containerName, err)
		containers, cerr := cli.ContainerList(ctx, types.ContainerListOptions{
			All:   true,
			Limit: -1, // no limit, see docker/docker/client/container_list.go
		})
		if cerr != nil {
			log.Infof(ctx, "unable to list containers: %v", cerr)
			return container.ContainerCreateCreatedBody{}, err
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
					return container.ContainerCreateCreatedBody{}, err
				}
				return cli.ContainerCreate(ctx, config, hostConfig, networkingConfig, platformSpec, containerName)
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
