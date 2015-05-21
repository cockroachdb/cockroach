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
// Author: Peter Mattis (peter.mattis@gmail.com)

package localcluster

import (
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"path/filepath"

	"github.com/samalba/dockerclient"
)

const (
	builderImage   = "cockroachdb/builder"
	dockerspyImage = "cockroachdb/docker-spy"
	domain         = "local"
)

var cockroachImage = flag.String("i", builderImage, "the docker image to run")
var cockroachBinary = flag.String("b", defaultBinary(), "the binary to run (if image == "+builderImage+")")
var cockroachEntry = flag.String("e", "", "the entry point for the image")

func prettyJSON(v interface{}) string {
	pretty, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		panic(err)
	}
	return string(pretty)
}

func defaultBinary() string {
	gopath := filepath.SplitList(os.Getenv("GOPATH"))
	if len(gopath) == 0 {
		return ""
	}
	return gopath[0] + "/bin/linux_amd64/cockroach"
}

func exists(path string) bool {
	if _, err := os.Stat(path); os.IsNotExist(err) {
		return false
	}
	return true
}

// Cluster ... TODO(pmattis): document
type Cluster struct {
	client  dockerclient.Client
	dns     *Container
	vols    *Container
	Nodes   []*Container
	stopper chan struct{}
}

// Create ... TODO(pmattis): document
func Create(numNodes int, stopper chan struct{}) (l *Cluster) {
	if *cockroachImage == builderImage && !exists(*cockroachBinary) {
		log.Fatalf("\"%s\": does not exist", *cockroachBinary)
	}

	l = &Cluster{
		client:  newDockerClient(),
		Nodes:   make([]*Container, numNodes),
		stopper: stopper,
	}
	defer l.stopOnPanic()
	l.init()
	return l
}

func (l *Cluster) stopOnPanic() {
	if r := recover(); r != nil {
		l.Stop()
		if r != l {
			panic(r)
		}
	}
}

func (l *Cluster) panicOnSig() {
	if l.stopper == nil {
		panic(l)
	}

	select {
	case <-l.stopper:
		l.stopper = nil
		panic(l)
	default:
	}
}

func (l *Cluster) runDockerSpy() {
	l.panicOnSig()

	create := func() (*Container, error) {
		return createContainer(l.client, dockerclient.ContainerConfig{
			Image: dockerspyImage,
			Cmd:   []string{"--dns-domain=" + domain},
		})
	}
	c, err := create()
	if err == dockerclient.ErrNotFound {
		log.Printf("pulling %s", dockerspyImage)
		err = l.client.PullImage(dockerspyImage, nil)
		if err == nil {
			c, err = create()
		}
	}
	if err != nil {
		panic(err)
	}
	maybePanic(c.Start([]string{"/var/run/docker.sock:/var/run/docker.sock"}, nil, nil))
	maybePanic(c.Inspect())
	c.Name = "docker-spy"
	l.dns = c
	log.Printf("started %s: %s\n", c.Name, c.NetworkSettings.IPAddress)
}

// create the volumes container that keeps all of the volumes used by the
// cluster.
func (l *Cluster) createVolumes() {
	l.panicOnSig()

	vols := map[string]struct{}{}
	for i := range l.Nodes {
		vols[l.data(i)] = struct{}{}
	}
	create := func() (*Container, error) {
		return createContainer(l.client, dockerclient.ContainerConfig{
			Image:   *cockroachImage,
			Volumes: vols,
		})
	}
	c, err := create()
	if err == dockerclient.ErrNotFound && *cockroachImage == builderImage {
		log.Printf("pulling %s", *cockroachImage)
		err = l.client.PullImage(*cockroachImage, nil)
		if err == nil {
			c, err = create()
		}
	}
	if err != nil {
		panic(err)
	}
	binds := []string{os.ExpandEnv("${PWD}/certs:/certs")}
	if *cockroachImage == builderImage {
		path, err := filepath.Abs(*cockroachBinary)
		if err != nil {
			panic(err)
		}
		binds = append(binds, path+":/"+filepath.Base(*cockroachBinary))
	}
	maybePanic(c.Start(binds, nil, nil))
	c.Name = "volumes"
	log.Printf("created volumes")
	l.vols = c
}

func (l *Cluster) createRoach(i int, cmd ...string) *Container {
	l.panicOnSig()

	var hostname string
	if i >= 0 {
		hostname = fmt.Sprintf("roach%d", i)
	}
	var entrypoint []string
	if *cockroachImage == builderImage {
		entrypoint = append(entrypoint, "/"+filepath.Base(*cockroachBinary))
	} else if *cockroachEntry != "" {
		entrypoint = append(entrypoint, *cockroachEntry)
	}
	c, err := createContainer(l.client, dockerclient.ContainerConfig{
		Hostname:     hostname,
		Domainname:   domain,
		Image:        *cockroachImage,
		ExposedPorts: map[string]struct{}{"8080/tcp": {}},
		Entrypoint:   entrypoint,
		Cmd:          cmd,
	})
	if err != nil {
		panic(err)
	}
	return c
}

func (l *Cluster) createCACert() {
	log.Printf("creating ca")
	c := l.createRoach(-1, "cert", "--certs=/certs", "create-ca")
	defer c.mustRemove()
	maybePanic(c.Start(nil, nil, l.vols))
	maybePanic(c.Wait())
}

func (l *Cluster) createNodeCerts() {
	log.Printf("creating node certs: ./certs")
	var nodes []string
	for i := range l.Nodes {
		nodes = append(nodes, l.node(i))
	}
	args := []string{"cert", "--certs=/certs", "create-node"}
	args = append(args, nodes...)
	c := l.createRoach(-1, args...)
	defer c.mustRemove()
	maybePanic(c.Start(nil, nil, l.vols))
	maybePanic(c.Wait())
}

func (l *Cluster) initCluster() {
	log.Printf("initializing cluster")
	c := l.createRoach(-1, "init", "--stores=ssd="+l.data(0))
	defer c.mustRemove()
	maybePanic(c.Start(nil, nil, l.vols))
	maybePanic(c.Wait())
}

func (l *Cluster) init() {
	l.runDockerSpy()
	l.createVolumes()
	l.createCACert()
	l.createNodeCerts()
	l.initCluster()
}

func (l *Cluster) startNode(i int) *Container {
	cmd := []string{
		"start",
		"--stores=ssd=" + l.data(i),
		"--certs=/certs",
		"--addr=" + l.node(i) + ":8080",
		"--gossip=" + l.node(0) + ":8080",
	}
	c := l.createRoach(i, cmd...)
	maybePanic(c.Start(nil, l.dns, l.vols))
	maybePanic(c.Inspect())
	c.Name = l.node(i)
	log.Printf("started %s: %s", c.Name, c.Addr(""))
	return c
}

// Start ... TODO(pmattis): document
func (l *Cluster) Start() bool {
	defer l.stopOnPanic()
	for i := range l.Nodes {
		l.Nodes[i] = l.startNode(i)
	}
	return true
}

// Stop ... TODO(pmattis): document
func (l *Cluster) Stop() {
	if l.dns != nil {
		maybePanic(l.dns.Kill())
		l.dns = nil
	}
	if l.vols != nil {
		maybePanic(l.vols.Kill())
		l.vols = nil
		os.RemoveAll("certs")
	}
	for _, n := range l.Nodes {
		if n != nil {
			maybePanic(n.Kill())
		}
	}
	l.Nodes = nil
}

func (l *Cluster) node(i int) string {
	return fmt.Sprintf("roach%d.%s", i, domain)
}

func (l *Cluster) data(i int) string {
	return fmt.Sprintf("/data%d", i)
}
