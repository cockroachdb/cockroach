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
	"sync"

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

func node(i int) string {
	return fmt.Sprintf("roach%d.%s", i, domain)
}

func data(i int) string {
	return fmt.Sprintf("/data%d", i)
}

// The various event types.
const (
	EventCreate     = "create"
	EventDestroy    = "destroy"
	EventDie        = "die"
	EventExecCreate = "exec_create"
	EventExecStart  = "exec_start"
	EventExport     = "export"
	EventKill       = "kill"
	EventOom        = "oom"
	EventPause      = "pause"
	EventRestart    = "restart"
	EventStart      = "start"
	EventStop       = "stop"
	EventUnpause    = "unpause"
)

// Event for a node containing a node index and the type of event.
type Event struct {
	NodeIndex int
	Status    string
}

// Cluster manages a local cockroach cluster running on docker. The cluster is
// composed of a "dns" container which automatically registers dns entries for
// the cockroach nodes, a "volumes" container which manages the persistent
// volumes used for certs and node data and N cockroach nodes.
type Cluster struct {
	client         dockerclient.Client
	stopper        chan struct{}
	mu             sync.Mutex // Protects the fields below
	dns            *Container
	vols           *Container
	Nodes          []*Container
	Events         chan Event
	certsDir       string
	monitorStopper chan struct{}
}

// Create creates a new local cockroach cluster. The stopper is used to
// gracefully shutdown the channel (e.g. when a signal arrives). The cluster
// must be started before being used.
func Create(numNodes int, stopper chan struct{}) *Cluster {
	select {
	case <-stopper:
		// The stopper was already closed, exit early.
		os.Exit(1)
	default:
	}

	if *cockroachImage == builderImage && !exists(*cockroachBinary) {
		log.Fatalf("\"%s\": does not exist", *cockroachBinary)
	}

	return &Cluster{
		client:  newDockerClient(),
		stopper: stopper,
		Nodes:   make([]*Container, numNodes),
	}
}

// stopOnPanic is invoked as a deferred function in Start in order to attempt
// to tear down the cluster if a panic occurs while starting it. If the panic
// was initiated by the stopper being closed (which panicOnStop notices) then
// the process is exited with a failure code.
func (l *Cluster) stopOnPanic() {
	if r := recover(); r != nil {
		l.Stop()
		if r != l {
			panic(r)
		}
		os.Exit(1)
	}
}

// panicOnStop tests whether the stopper channel has been closed and panics if
// it has. This allows polling for whether to stop and avoids nasty locking
// complications with trying to call Stop at arbitrary points such as in the
// middle of creating a container.
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
		vols[data(i)] = struct{}{}
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

	l.CertsDir, err = ioutil.TempDir(os.TempDir(), "localcluster.")
	if err != nil {
		panic(err)
	}

	binds := []string{l.CertsDir + ":/certs"}
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
		nodes = append(nodes, node(i))
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
	c := l.createRoach(-1, "init", "--stores=ssd="+data(0))
	defer c.mustRemove()
	maybePanic(c.Start(nil, nil, l.vols))
	maybePanic(c.Wait())
}

func (l *Cluster) startNode(i int) *Container {
	cmd := []string{
		"start",
		"--stores=ssd=" + data(i),
		"--certs=/certs",
		"--addr=" + node(i) + ":8080",
		"--gossip=" + node(0) + ":8080",
	}
	c := l.createRoach(i, cmd...)
	maybePanic(c.Start(nil, l.dns, l.vols))
	maybePanic(c.Inspect())
	c.Name = node(i)
	log.Infof("started %s: %s", c.Name, c.Addr(""))
	return c
}

func (l *Cluster) processEvent(e dockerclient.EventOrError) bool {
	l.mu.Lock()
	defer l.mu.Unlock()

	if e.Error != nil {
		l.Events <- Event{NodeIndex: -1, Status: EventDie}
		return false
	}

	for i, n := range l.Nodes {
		if n != nil && n.Id == e.Id {
			l.Events <- Event{NodeIndex: i, Status: e.Status}
			return true
		}
	}

	// TODO(pmattis): When we add the ability to start/stop/restart nodes we'll
	// need to keep around a map of old node container ids in order to ignore
	// events on those containers.

	// An event on any other container is unexpected. Die.
	select {
	case <-l.stopper:
	default:
		// There is a very tiny race here: the signal handler might be closing the
		// stopper simultaneously.
		close(l.stopper)
	}
	return false
}

func (l *Cluster) monitor(ch <-chan dockerclient.EventOrError) {
	for {
		if !l.processEvent(<-ch) {
			break
		}
	}
}

// Start starts the cluster.
func (l *Cluster) Start() {
	defer l.stopOnPanic()

	l.mu.Lock()
	defer l.mu.Unlock()

	l.runDockerSpy()
	l.createVolumes()
	l.createCACert()
	l.createNodeCerts()
	l.initCluster()

	if l.Events != nil {
		l.monitorStopper = make(chan struct{})
		ch, err := l.client.MonitorEvents(nil, l.monitorStopper)
		if err != nil {
			panic(err)
		}
		go l.monitor(ch)
	}

	for i := range l.Nodes {
		l.Nodes[i] = l.startNode(i)
	}
}

// Stop stops the clusters. It is safe to stop the cluster multiple times.
func (l *Cluster) Stop() {
	log.Infof("stopping")

	l.mu.Lock()
	defer l.mu.Unlock()

	if l.monitorStopper != nil {
		close(l.monitorStopper)
		l.monitorStopper = nil
	}

	if l.dns != nil {
		maybePanic(l.dns.Kill())
		l.dns = nil
	}
	if l.vols != nil {
		maybePanic(l.vols.Kill())
		l.vols = nil
	}
	if l.CertsDir != "" {
		_ = os.RemoveAll(l.CertsDir)
		l.CertsDir = ""
	}
	for i, n := range l.Nodes {
		if n != nil {
			maybePanic(n.Kill())
			l.Nodes[i] = nil
		}
	}
}
