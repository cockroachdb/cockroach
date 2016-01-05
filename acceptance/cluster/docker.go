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
	"crypto/tls"
	"crypto/x509"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"math"
	"net"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"runtime"
	"strconv"

	"github.com/cockroachdb/cockroach/util"
	"github.com/cockroachdb/cockroach/util/log"
	"github.com/samalba/dockerclient"
)

func getTLSConfig() *tls.Config {
	certPath := os.Getenv("DOCKER_CERT_PATH")

	clientCert, err := tls.LoadX509KeyPair(
		filepath.Join(certPath, "cert.pem"),
		filepath.Join(certPath, "key.pem"),
	)
	if err != nil {
		panic(err)
	}

	rootCAs := x509.NewCertPool()
	caCert, err := ioutil.ReadFile(filepath.Join(certPath, "ca.pem"))
	if err != nil {
		panic(err)
	}
	rootCAs.AppendCertsFromPEM(caCert)

	return &tls.Config{
		Certificates: []tls.Certificate{clientCert},
		RootCAs:      rootCAs,
	}
}

// newDockerClient constructs a new docker client using the best available
// method. If DOCKER_HOST is set, initialize the client using DOCKER_TLS_VERIFY
// and DOCKER_CERT_PATH. If DOCKER_HOST is not set, look for the unix domain
// socket in /run/docker.sock and /var/run/docker.sock.
func NewDockerClient() dockerclient.Client {
	if host := os.Getenv("DOCKER_HOST"); host != "" {
		if os.Getenv("DOCKER_TLS_VERIFY") == "" {
			c, err := dockerclient.NewDockerClient(host, nil)
			if err != nil {
				log.Fatal(err)
			}
			return c
		}
		c, err := dockerclient.NewDockerClient(host, getTLSConfig())
		if err != nil {
			log.Fatal(err)
		}
		return c
	}

	for _, l := range []string{"/run/docker.sock", "/var/run/docker.sock"} {
		if _, err := os.Stat(l); err != nil {
			continue
		}
		c, err := dockerclient.NewDockerClient("unix://"+l, nil)
		if err != nil {
			return nil
		}
		return c
	}
	log.Fatal("docker not configured")
	return nil
}

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
	ID      string
	Name    string
	cluster *LocalCluster
}

// createContainer creates a new container using the specified options. Per the
// docker API, the created container is not running and must be started
// explicitly.
func createContainer(l *LocalCluster, config dockerclient.ContainerConfig) (*Container, error) {
	id, err := l.client.CreateContainer(&config, "", nil)
	if err != nil {
		return nil, err
	}
	return &Container{
		ID:      id,
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
	return c.cluster.client.RemoveContainer(c.ID, false, true)
}

// Kill stops a running container, without removing it.
func (c *Container) Kill() error {
	// Paused containers cannot be killed. Attempt to unpause it first
	// (which might fail) before killing.
	_ = c.Unpause()
	if err := c.cluster.client.KillContainer(c.ID, "9"); err != nil {
		return err
	}
	c.cluster.expectEvent(c, "die")
	return nil
}

// Start starts a non-running container.
//
// TODO(pmattis): Generalize the setting of parameters here.
func (c *Container) Start(binds []string, dns, vols *Container) error {
	config := &dockerclient.HostConfig{
		Binds:           binds,
		PublishAllPorts: true,
	}
	if dns != nil {
		ci, err := dns.Inspect()
		if err != nil {
			return err
		}
		config.Dns = append(config.Dns, ci.NetworkSettings.IPAddress)
	}
	if vols != nil {
		config.VolumesFrom = append(config.VolumesFrom, vols.ID)
	}
	return c.cluster.client.StartContainer(c.ID, config)
}

// Pause pauses a running container.
func (c *Container) Pause() error {
	return c.cluster.client.PauseContainer(c.ID)
}

// Unpause resumes a paused container.
func (c *Container) Unpause() error {
	return c.cluster.client.UnpauseContainer(c.ID)
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
	if err := c.cluster.client.RestartContainer(c.ID, timeoutSeconds); err != nil {
		return err
	}
	c.cluster.expectEvent(c, append(exp, "restart")...)
	return nil
}

// Stop a running container.
func (c *Container) Stop(timeoutSeconds int) error {
	if err := c.cluster.client.StopContainer(c.ID, timeoutSeconds); err != nil {
		return err
	}
	c.cluster.expectEvent(c, "die")
	return nil
}

// Wait waits for a running container to exit.
func (c *Container) Wait() error {
	// TODO(pmattis): dockerclient does not support the "wait" method
	// (yet), so perform the http call ourselves. Remove once "wait"
	// support is added to dockerclient.
	dc := c.cluster.client.(*dockerclient.DockerClient)
	resp, err := dc.HTTPClient.Post(
		fmt.Sprintf("%s/containers/%s/wait", dc.URL, c.ID), util.JSONContentType, nil)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusNotFound {
		return fmt.Errorf("no such container: %s", c.ID)
	}
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return err
	}
	var r struct{ StatusCode int }
	err = json.Unmarshal(body, &r)
	if err != nil {
		return err
	}
	if r.StatusCode != 0 {
		_ = c.Logs(os.Stderr)
		return fmt.Errorf("non-zero exit code: %d", r.StatusCode)
	}
	return nil
}

// Logs outputs the containers logs to the given io.Writer.
func (c *Container) Logs(w io.Writer) error {
	r, err := c.cluster.client.ContainerLogs(c.ID, &dockerclient.LogOptions{
		Stdout: true,
		Stderr: true,
	})
	if err != nil {
		return err
	}
	defer r.Close()
	// The docker log output is not quite plaintext: each line has a
	// prefix consisting of one byte file descriptor (stdout vs stderr),
	// three bytes padding, four byte length. We could use this to
	// disentangle stdout and stderr if we wanted to output them into
	// separate streams, but we don't really care.
	for {
		var header uint64
		if err := binary.Read(r, binary.BigEndian, &header); err == io.EOF {
			break
		} else if err != nil {
			return err
		}
		size := header & math.MaxUint32
		if _, err := io.CopyN(w, r, int64(size)); err != nil {
			return err
		}
	}
	return nil
}

// Inspect retrieves detailed info about a container.
func (c *Container) Inspect() (*dockerclient.ContainerInfo, error) {
	return c.cluster.client.InspectContainer(c.ID)
}

// Addr returns the address to connect to the specified port.
func (c *Container) Addr(name string) *net.TCPAddr {
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
func (c *Container) GetJSON(port, path string, v interface{}) error {
	return getJSON(true /* tls */, c.Addr(port).String(), path, v)
}
