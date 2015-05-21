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
	"io/ioutil"
	"os"
	"path/filepath"

	"github.com/cockroachdb/cockroach/util/log"
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

// Cluster manages a local cockroach cluster running on docker. The cluster is
// composed of a "dns" container which automatically registers dns entries for
// the cockroach nodes, a "volumes" container which manages the persistent
// volumes used for certs and node data and N cockroach nodes.
type Cluster struct {
	client   dockerclient.Client
	dns      *Container
	vols     *Container
	Nodes    []*Container
	certsDir string
	stopper  chan struct{}
}

// Create creates a new local cockroach cluster. The stopper is used to
// gracefully shutdown the channel (e.g. when a signal arrives). The cluster
// must be started before being used.
func Create(numNodes int, stopper chan struct{}) *Cluster {
	if *cockroachImage == builderImage && !exists(*cockroachBinary) {
		log.Fatalf("\"%s\": does not exist", *cockroachBinary)
	}

	return &Cluster{
		client:  newDockerClient(),
		Nodes:   make([]*Container, numNodes),
		stopper: stopper,
	}
}

func (l *Cluster) stopOnPanic() {
	if r := recover(); r != nil {
		l.Stop()
		if r != l {
			panic(r)
		}
	}
}

func (l *Cluster) panicOnStop() {
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
	l.panicOnStop()

	create := func() (*Container, error) {
		return createContainer(l.client, dockerclient.ContainerConfig{
			Image: dockerspyImage,
			Cmd:   []string{"--dns-domain=" + domain},
		})
	}
	c, err := create()
	if err == dockerclient.ErrNotFound {
		log.Infof("pulling %s", dockerspyImage)
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
	log.Infof("started %s: %s\n", c.Name, c.NetworkSettings.IPAddress)
}

// create the volumes container that keeps all of the volumes used by the
// cluster.
func (l *Cluster) createVolumes() {
	l.panicOnStop()

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
		log.Infof("pulling %s", *cockroachImage)
		err = l.client.PullImage(*cockroachImage, nil)
		if err == nil {
			c, err = create()
		}
	}
	if err != nil {
		panic(err)
	}

	l.certsDir, err = ioutil.TempDir(os.TempDir(), "localcluster.")
	if err != nil {
		panic(err)
	}

	binds := []string{l.certsDir + ":/certs"}
	if *cockroachImage == builderImage {
		path, err := filepath.Abs(*cockroachBinary)
		if err != nil {
			panic(err)
		}
		binds = append(binds, path+":/"+filepath.Base(*cockroachBinary))
	}
	maybePanic(c.Start(binds, nil, nil))
	c.Name = "volumes"
	log.Infof("created volumes")
	l.vols = c
}

func (l *Cluster) createRoach(i int, cmd ...string) *Container {
	l.panicOnStop()

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
	log.Infof("creating ca")
	c := l.createRoach(-1, "cert", "--certs=/certs", "create-ca")
	defer c.mustRemove()
	maybePanic(c.Start(nil, nil, l.vols))
	maybePanic(c.Wait())
}

func (l *Cluster) createNodeCerts() {
	log.Infof("creating node certs: ./certs")
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
	log.Infof("initializing cluster")
	c := l.createRoach(-1, "init", "--stores=ssd="+l.data(0))
	defer c.mustRemove()
	maybePanic(c.Start(nil, nil, l.vols))
	maybePanic(c.Wait())
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
	log.Infof("started %s: %s", c.Name, c.Addr(""))
	return c
}

// Start starts the cluster. Returns false if the cluster did not start
// properly.
func (l *Cluster) Start() bool {
	defer l.stopOnPanic()

	l.runDockerSpy()
	l.createVolumes()
	l.createCACert()
	l.createNodeCerts()
	l.initCluster()

	for i := range l.Nodes {
		l.Nodes[i] = l.startNode(i)
	}

	// TODO(pmattis):
	// ch, err := l.client.MonitorEvents(nil, nil)
	// if err != nil {
	// 	panic(err)
	// }
	// go func() {
	// 	for {
	// 		e := <-ch
	// 		if e.Error != nil {
	// 			log.Println(e.Error)
	// 			break
	// 		}
	// 		log.Println(e.Event)
	// 	}
	// }()

	// Mildy tricky: we return false on failure by recovering from a panic.
	return true
}

// Stop stops the clusters. It is safe to stop the cluster multiple times.
func (l *Cluster) Stop() {
	if l.dns != nil {
		maybePanic(l.dns.Kill())
		l.dns = nil
	}
	if l.vols != nil {
		maybePanic(l.vols.Kill())
		l.vols = nil
	}
	if l.certsDir != "" {
		_ = os.RemoveAll(l.certsDir)
		l.certsDir = ""
	}
	for i, n := range l.Nodes {
		if n != nil {
			maybePanic(n.Kill())
			l.Nodes[i] = nil
		}
	}
}

func (l *Cluster) node(i int) string {
	return fmt.Sprintf("roach%d.%s", i, domain)
}

func (l *Cluster) data(i int) string {
	return fmt.Sprintf("/data%d", i)
}
