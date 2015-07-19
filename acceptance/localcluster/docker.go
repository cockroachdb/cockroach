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
// permissions and limitations under the License. See the AUTHORS file
// for names of contributors.
//
// Author: Peter Mattis (peter@cockroachlabs.com)

package localcluster

import (
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"net"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"time"

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
func newDockerClient() dockerclient.Client {
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
	panic(fmt.Errorf("unable to determine docker ip address"))
}

// Container provides the programmatic interface for a single docker
// container.
type Container struct {
	ID            string
	containerInfo dockerclient.ContainerInfo
	client        dockerclient.Client
}

// createContainer creates a new container using the specified options. Per the
// docker API, the created container is not running and must be started
// explicitly.
func createContainer(client dockerclient.Client, config dockerclient.ContainerConfig) (*Container, error) {
	id, err := client.CreateContainer(&config, "")
	if err != nil {
		return nil, err
	}
	return &Container{
		ID:            id,
		containerInfo: dockerclient.ContainerInfo{Id: id},
		client:        client}, nil
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
	return c.client.RemoveContainer(c.ID, false, true)
}

func (c *Container) mustRemove() {
	maybePanic(c.Remove())
}

// Kill stops a running container and removes it.
func (c *Container) Kill() error {
	// Paused containers cannot be killed. Attempt to unpause it first
	// (which might fail) before killing.
	_ = c.Unpause()
	err := c.client.KillContainer(c.ID, "9")
	if err != nil {
		return err
	}
	return c.Remove()
}

func (c *Container) mustKill() {
	maybePanic(c.Kill())
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
		config.Dns = append(config.Dns, dns.containerInfo.NetworkSettings.IPAddress)
	}
	if vols != nil {
		config.VolumesFrom = append(config.VolumesFrom, vols.ID)
	}
	return c.client.StartContainer(c.ID, config)
}

// Pause pauses a running container.
func (c *Container) Pause() error {
	return c.client.PauseContainer(c.ID)
}

// Unpause resumes a paused container.
func (c *Container) Unpause() error {
	return c.client.UnpauseContainer(c.ID)
}

// Restart restarts a running container.
// Container will be killed after 'timeout' seconds if it fails to stop.
func (c *Container) Restart(timeoutSeconds int) error {
	if err := c.client.RestartContainer(c.ID, timeoutSeconds); err != nil {
		return err
	}
	// We need to refresh the container metadata. Ports change on restart.
	return c.Inspect()
}

// Stop a running container.
func (c *Container) Stop(timeoutSeconds int) error {
	return c.client.StopContainer(c.ID, timeoutSeconds)
}

// Wait waits for a running container to exit.
func (c *Container) Wait() error {
	// TODO(pmattis): dockerclient does not support the "wait" method
	// (yet), so perform the http call ourselves. Remove once "wait"
	// support is added to dockerclient.
	dc := c.client.(*dockerclient.DockerClient)
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
	if err := json.Unmarshal(body, &r); err != nil {
		return err
	}
	if r.StatusCode != 0 {
		_ = c.Logs()
		return fmt.Errorf("non-zero exit code: %d", r.StatusCode)
	}
	return nil
}

// Logs outputs the containers logs to stdout/stderr.
func (c *Container) Logs() error {
	r, err := c.client.ContainerLogs(c.ID, &dockerclient.LogOptions{
		Stdout: true,
		Stderr: true,
	})
	if err != nil {
		return err
	}
	defer r.Close()
	_, err = io.Copy(os.Stdout, r)
	return err
}

// Inspect retrieves detailed info about a container.
func (c *Container) Inspect() error {
	out, err := c.client.InspectContainer(c.ID)
	if err != nil {
		return err
	}
	c.containerInfo = *out
	return nil
}

// Addr returns the address to connect to the specified port.
func (c *Container) Addr(name string) *net.TCPAddr {
	if name == "" {
		// No port specified, pick a random one (random because iteration
		// over maps is randomized).
		for port := range c.containerInfo.NetworkSettings.Ports {
			name = port
			break
		}
	}
	bindings, ok := c.containerInfo.NetworkSettings.Ports[name]
	if !ok || len(bindings) == 0 {
		return nil
	}
	port, _ := strconv.Atoi(bindings[0].HostPort)
	return &net.TCPAddr{
		IP:   dockerIP(),
		Port: port,
	}
}

// GetJSON retrieves the URL specified by https://Addr(<port>)<path>
// and unmarshals the result as JSON.
func (c *Container) GetJSON(port, path string, v interface{}) error {
	client := &http.Client{
		Timeout: 200 * time.Millisecond,
		Transport: &http.Transport{
			TLSClientConfig: &tls.Config{
				InsecureSkipVerify: true,
			},
		}}
	resp, err := client.Get(fmt.Sprintf("https://%s%s", c.Addr(port), path))
	if err != nil {
		if log.V(1) {
			log.Info(err)
		}
		return err
	}
	defer resp.Body.Close()
	b, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		if log.V(1) {
			log.Info(err)
		}
		return err
	}
	if err := json.Unmarshal(b, v); err != nil {
		if log.V(1) {
			log.Info(err)
		}
	}
	return nil
}
