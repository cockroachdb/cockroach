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
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"github.com/cockroachdb/cockroach/security"
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
var waitOnStop = flag.Bool("w", false, "wait for the user to interrupt before tearing down the cluster")
var logDirectory = flag.String("l", "", "the directory to store log files, relative to where the test source")
var pwd = filepath.Clean(os.ExpandEnv("${PWD}"))

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
	CertsDir       string
	monitorStopper chan struct{}
	LogDir         string
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
	c.containerInfo.Name = "docker-spy"
	l.dns = c
	log.Infof("started %s: %s", c.containerInfo.Name, c.containerInfo.NetworkSettings.IPAddress)
}

// create the volumes container that keeps all of the volumes used by
// the cluster.
func (l *Cluster) initCluster() {
	log.Infof("initializing cluster")
	l.panicOnStop()

	vols := map[string]struct{}{}
	for i := range l.Nodes {
		vols[data(i)] = struct{}{}
	}
	create := func() (*Container, error) {
		var entrypoint []string
		if *cockroachImage == builderImage {
			entrypoint = append(entrypoint, "/"+filepath.Base(*cockroachBinary))
		} else if *cockroachEntry != "" {
			entrypoint = append(entrypoint, *cockroachEntry)
		}
		return createContainer(l.client, dockerclient.ContainerConfig{
			Image:      *cockroachImage,
			Volumes:    vols,
			Entrypoint: entrypoint,
			Cmd:        []string{"init", "--stores=ssd=" + data(0)},
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

	// Create the temporary certs directory in the current working
	// directory. Boot2docker's handling of binding local directories
	// into the container is very confusing. If the directory being
	// bound has a parent directory that exists in the boot2docker VM
	// then that directory is bound into the container. In particular,
	// that means that binds of /tmp and /var will be problematic.
	l.CertsDir, err = ioutil.TempDir(pwd, ".localcluster.certs.")
	if err != nil {
		panic(err)
	}

	binds := []string{l.CertsDir + ":/certs"}
	if logDirectory != nil && len(*logDirectory) > 0 {
		l.LogDir = filepath.Join(pwd, *logDirectory)
		binds = append(binds, l.LogDir+":/logs")
	}
	if *cockroachImage == builderImage {
		path, err := filepath.Abs(*cockroachBinary)
		if err != nil {
			panic(err)
		}
		binds = append(binds, path+":/"+filepath.Base(*cockroachBinary))
	}

	maybePanic(c.Start(binds, nil, nil))
	maybePanic(c.Wait())
	c.containerInfo.Name = "volumes"
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
	maybePanic(security.RunCreateCACert(l.CertsDir, 512))
}

func (l *Cluster) createNodeCerts() {
	log.Infof("creating node certs")
	nodes := []string{dockerIP().String()}
	for i := range l.Nodes {
		nodes = append(nodes, node(i))
	}
	maybePanic(security.RunCreateNodeCert(l.CertsDir, 512, nodes))
}

func (l *Cluster) startNode(i int) *Container {
	gossipNodes := []string{}
	for i := range l.Nodes {
		gossipNodes = append(gossipNodes, node(i)+":8080")
	}

	cmd := []string{
		"start",
		"--stores=ssd=" + data(i),
		"--certs=/certs",
		"--addr=" + node(i) + ":8080",
		"--gossip=" + strings.Join(gossipNodes, ","),
		"--scan-max-idle-time=200ms", // set low to speed up tests
	}
	if logDirectory != nil && len(*logDirectory) > 0 {
		dockerDir := "/logs/" + node(i)
		localDir := l.LogDir + "/" + node(i)
		if !exists(localDir) {
			if err := os.Mkdir(localDir, 0777); err != nil {
				log.Fatal(err)
			}
		}
		log.Infof("Logs for node %s are located at %s.", node(i), localDir)
		cmd = append(
			cmd,
			"--log-dir="+dockerDir,
			"--logtostderr=false",
			"--alsologtostderr=true")
	}
	c := l.createRoach(i, cmd...)
	maybePanic(c.Start(nil, l.dns, l.vols))
	maybePanic(c.Inspect())
	c.containerInfo.Name = node(i)
	log.Infof("started %s: https://%s", c.containerInfo.Name, c.Addr(""))
	return c
}

func (l *Cluster) processEvent(e dockerclient.EventOrError, monitorStopper chan struct{}) bool {
	l.mu.Lock()
	defer l.mu.Unlock()

	if e.Error != nil {
		log.Errorf("monitoring error: %s", e.Error)
		if l.Events != nil {
			l.Events <- Event{NodeIndex: -1, Status: EventDie}
		}
		return false
	}

	for i, n := range l.Nodes {
		if n != nil && n.ID == e.Id {
			if log.V(1) {
				log.Errorf("node=%d status=%s", i, e.Status)
			}
			if l.Events != nil {
				l.Events <- Event{NodeIndex: i, Status: e.Status}
			}
			return true
		}
	}

	// TODO(pmattis): When we add the ability to start/stop/restart nodes we'll
	// need to keep around a map of old node container ids in order to ignore
	// events on those containers.

	// An event on any other container is unexpected. Die.
	select {
	case <-l.stopper:
	case <-monitorStopper:
	default:
		// There is a very tiny race here: the signal handler might be closing the
		// stopper simultaneously.
		log.Errorf("stopping due to unexpected event: %+v", e)
		r, err := l.client.ContainerLogs(e.Id, &dockerclient.LogOptions{
			Stdout: true,
			Stderr: true,
		})
		if err == nil {
			defer r.Close()
			_, err = io.Copy(os.Stdout, r)
		}
		close(l.stopper)
	}
	return false
}

func (l *Cluster) monitor(ch <-chan dockerclient.EventOrError, monitorStopper chan struct{}) {
	for e := range ch {
		if !l.processEvent(e, monitorStopper) {
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
	l.initCluster()
	l.createCACert()
	l.createNodeCerts()

	l.monitorStopper = make(chan struct{})
	ch, err := l.client.MonitorEvents(nil, l.monitorStopper)
	if err != nil {
		panic(err)
	}
	go l.monitor(ch, l.monitorStopper)

	for i := range l.Nodes {
		l.Nodes[i] = l.startNode(i)
	}
}

// Stop stops the clusters. It is safe to stop the cluster multiple times.
func (l *Cluster) Stop() {
	if *waitOnStop {
		log.Infof("waiting for interrupt")
		select {
		case <-l.stopper:
		}
	}

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
