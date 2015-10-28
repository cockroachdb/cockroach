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
	"time"

	"github.com/cockroachdb/cockroach/security"
	"github.com/cockroachdb/cockroach/util"
	"github.com/cockroachdb/cockroach/util/log"
	"github.com/samalba/dockerclient"
)

const (
	builderImage   = "cockroachdb/builder"
	dockerspyImage = "cockroachdb/docker-spy"
	domain         = "local"
	cockroachPort  = 26257
)

var cockroachImage = flag.String("i", builderImage, "the docker image to run")
var cockroachBinary = flag.String("b", defaultBinary(), "the binary to run (if image == "+builderImage+")")
var cockroachEntry = flag.String("e", "", "the entry point for the image")
var waitOnStop = flag.Bool("w", false, "wait for the user to interrupt before tearing down the cluster")
var logDirectory = flag.String("l", "", "the directory to store log files, relative to where the test source")
var pwd = filepath.Clean(os.ExpandEnv("${PWD}"))

// keyLen is the length (in bits) of the generated CA and node certs.
const keyLen = 1024

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
	eventDie = "die"
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
	numNodes       int
	Nodes          []*Container
	events         chan Event
	expectedEvents chan Event
	CertsDir       string
	monitorStopper chan struct{}
	LogDir         string
	ForceLogging   bool // Forces logging to disk on a per test basis
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
		client:   newDockerClient(),
		stopper:  stopper,
		numNodes: numNodes,
		// TODO(tschottdorf): deadlocks will occur if these channels fill up.
		events:         make(chan Event, 1000),
		expectedEvents: make(chan Event, 1000),
	}
}

func (l *Cluster) expectEvent(c *Container, msgs ...string) {
	for index, ctr := range l.Nodes {
		if c.ID != ctr.ID {
			continue
		}
		for _, status := range msgs {
			l.expectedEvents <- Event{NodeIndex: index, Status: status}
		}
		break
	}
}

// stopOnPanic is invoked as a deferred function in Start in order to attempt
// to tear down the cluster if a panic occurs while starting it. If the panic
// was initiated by the stopper being closed (which panicOnStop notices) then
// the process is exited with a failure code.
func (l *Cluster) stopOnPanic() {
	if r := recover(); r != nil {
		l.stop()
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
		return createContainer(l, dockerclient.ContainerConfig{
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
	c.Name = "docker-spy"
	l.dns = c
	if ci, err := c.Inspect(); err != nil {
		log.Error(err)
	} else {
		log.Infof("started %s: %s", c.Name, ci.NetworkSettings.IPAddress)
	}
}

// create the volumes container that keeps all of the volumes used by
// the cluster.
func (l *Cluster) initCluster() {
	log.Infof("initializing cluster")
	l.panicOnStop()

	vols := map[string]struct{}{}
	for i := 0; i < l.numNodes; i++ {
		vols[data(i)] = struct{}{}
	}
	create := func() (*Container, error) {
		var entrypoint []string
		if *cockroachImage == builderImage {
			entrypoint = append(entrypoint, "/"+filepath.Base(*cockroachBinary))
		} else if *cockroachEntry != "" {
			entrypoint = append(entrypoint, *cockroachEntry)
		}
		return createContainer(l, dockerclient.ContainerConfig{
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

	binds := []string{
		l.CertsDir + ":/certs",
		filepath.Join(pwd, "..") + ":/go/src/github.com/cockroachdb/cockroach",
	}

	if logDirectory != nil && len(*logDirectory) > 0 {
		if filepath.IsAbs(*logDirectory) {
			l.LogDir = *logDirectory
		} else {
			l.LogDir = filepath.Join(pwd, *logDirectory)
		}
		binds = append(binds, l.LogDir+":/logs")
	} else if l.ForceLogging {
		l.LogDir, err = ioutil.TempDir(pwd, ".localcluster.logs.")
		if err != nil {
			panic(err)
		}
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
	l.vols = c
	l.vols.Name = "volumes"
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
	c, err := createContainer(l, dockerclient.ContainerConfig{
		Hostname:     hostname,
		Domainname:   domain,
		Image:        *cockroachImage,
		ExposedPorts: map[string]struct{}{fmt.Sprintf("%d/tcp", cockroachPort): {}},
		Entrypoint:   entrypoint,
		Cmd:          cmd,
		Labels: map[string]string{
			// Allow for `docker ps --filter label=Hostname=roach0` or `--filter label=Roach`.
			"Hostname": hostname,
			"Roach":    "",
		},
	})
	if err != nil {
		panic(err)
	}
	return c
}

func (l *Cluster) createCACert() {
	log.Infof("creating ca (%dbit) in: %s", keyLen, l.CertsDir)
	maybePanic(security.RunCreateCACert(l.CertsDir, keyLen))
}

func (l *Cluster) createNodeCerts() {
	log.Infof("creating node (%dbit) certs in: %s", keyLen, l.CertsDir)
	nodes := []string{dockerIP().String()}
	for i := 0; i < l.numNodes; i++ {
		nodes = append(nodes, node(i))
	}
	maybePanic(security.RunCreateNodeCert(l.CertsDir, keyLen, nodes))
}

func (l *Cluster) startNode(i int) *Container {
	gossipNodes := []string{}
	for i := 0; i < l.numNodes; i++ {
		gossipNodes = append(gossipNodes, fmt.Sprintf("%s:%d", node(i), cockroachPort))
	}

	cmd := []string{
		"start",
		"--stores=ssd=" + data(i),
		"--certs=/certs",
		"--addr=" + fmt.Sprintf("%s:%d", node(i), cockroachPort),
		"--gossip=" + strings.Join(gossipNodes, ","),
		"--scan-max-idle-time=200ms", // set low to speed up tests
	}
	var localLogDir string
	if len(l.LogDir) > 0 {
		dockerLogDir := "/logs/" + node(i)
		localLogDir = filepath.Join(l.LogDir, node(i))
		if !exists(localLogDir) {
			if err := os.Mkdir(localLogDir, 0777); err != nil {
				log.Fatal(err)
			}
		}
		cmd = append(
			cmd,
			"--log-dir="+dockerLogDir,
			"--logtostderr=false",
			"--alsologtostderr=true")
	}
	c := l.createRoach(i, cmd...)
	maybePanic(c.Start(nil, l.dns, l.vols))
	c.Name = node(i)
	uri := fmt.Sprintf("https://%s", c.Addr(""))
	// Infof doesn't take positional parameters, hence the Sprintf.
	log.Infof(fmt.Sprintf(`*** started %[1]s ***
  ui:    %[2]s
  trace: %[2]s/debug/requests
  logs:  %[3]s/cockroach.INFO
  certs: %[4]s
  pprof: docker exec -it %[5]s /bin/bash -c 'go tool pprof /cockroach <(wget --no-check-certificate -qO- https://$(hostname):26257/debug/pprof/heap)'`,
		c.Name, uri, localLogDir, l.CertsDir, c.ID[:5]))
	return c
}

func (l *Cluster) processEvent(e dockerclient.EventOrError, monitorStopper chan struct{}) bool {
	l.mu.Lock()
	defer l.mu.Unlock()

	if e.Error != nil {
		log.Errorf("monitoring error: %s", e.Error)
		l.events <- Event{NodeIndex: -1, Status: eventDie}
		return false
	}

	for i, n := range l.Nodes {
		if n != nil && n.ID == e.Id {
			if log.V(1) {
				log.Errorf("node=%d status=%s", i, e.Status)
			}
			l.events <- Event{NodeIndex: i, Status: e.Status}
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
		if r, err := l.client.ContainerLogs(e.Id, &dockerclient.LogOptions{
			Stdout: true,
			Stderr: true,
		}); err == nil {
			if _, err := io.Copy(os.Stderr, r); err != nil {
				log.Infof("error listing logs: %s", err)
			}
			r.Close()
		}
		close(l.stopper)
	}
	return false
}

func (l *Cluster) monitor(monitorStopper chan struct{}) {
	// MonitorEvents will (as of Docker 1.7) block until the first event is
	// received.
	ch, err := l.client.MonitorEvents(nil, l.monitorStopper)
	if err != nil {
		panic(err)
	}

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
	go l.monitor(l.monitorStopper)
	l.Nodes = make([]*Container, l.numNodes)
	for i := range l.Nodes {
		l.Nodes[i] = l.startNode(i)
	}
}

// Assert drains the Events channel and compares the actual events with those
// expected to have been generated by the operations performed on the nodes in
// the cluster (restart, kill, ...). In the event of a mismatch, the passed
// Tester receives a fatal error.
// Currently, the only events generated (and asserted against) are "die" and
// "restart", to maximize compatibility across different versions of Docker.
func (l *Cluster) Assert(t util.Tester) {
	const almostZero = 50 * time.Millisecond
	filter := func(ch chan Event, wait time.Duration) *Event {
		for {
			select {
			case act := <-ch:
				if act.Status != "die" && act.Status != "restart" {
					continue
				}
				return &act
			case <-time.After(wait):
			}
			break
		}
		return nil
	}

	var events []Event
	for {
		exp := filter(l.expectedEvents, almostZero)
		if exp == nil {
			break
		}
		act := filter(l.events, time.Second)
		if act == nil || *exp != *act {
			t.Fatalf("expected event %v, got %v (after %v)", exp, act, events)
		}
		events = append(events, *exp)
	}
	if cur := filter(l.events, almostZero); cur != nil {
		t.Fatalf("unexpected extra event %v (after %v)", cur, events)
	}
	if log.V(2) {
		log.Infof("asserted %v", events)
	}
}

// AssertAndStop calls Assert and then stops the cluster. It is safe to stop
// the cluster multiple times.
func (l *Cluster) AssertAndStop(t util.Tester) {
	defer l.stop()
	l.Assert(t)
}

// stop stops the cluster.
func (l *Cluster) stop() {
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
		maybePanic(l.dns.Remove())
		l.dns = nil
	}
	if l.vols != nil {
		maybePanic(l.vols.Kill())
		maybePanic(l.vols.Remove())
		l.vols = nil
	}
	if l.CertsDir != "" {
		_ = os.RemoveAll(l.CertsDir)
		l.CertsDir = ""
	}
	for i, n := range l.Nodes {
		ci, err := n.Inspect()
		crashed := err != nil || (!ci.State.Running && ci.State.ExitCode != 0)
		maybePanic(n.Kill())
		if len(l.LogDir) > 0 {
			// TODO(bdarnell): make these filenames more consistent with
			// structured logs?
			file := filepath.Join(l.LogDir, node(i),
				fmt.Sprintf("stderr.%s.log", strings.Replace(
					time.Now().Format(time.RFC3339), ":", "_", -1)))
			w, err := os.Create(file)
			maybePanic(err)
			defer w.Close()
			maybePanic(n.Logs(w))
			if crashed {
				log.Infof("node %d: stderr at %s", i, file)
			}
		} else if crashed {
			log.Warningf("node %d died: %s", i, ci.State)
		}
		maybePanic(n.Remove())
	}
	l.Nodes = nil
	// Only delete the logging directory if ForceLogging was set and there was
	// no passed in logDirectory flag. This must be after the killing of the
	// nodes or there might be a panic when it tries to log that it is killing
	// the node.
	if l.ForceLogging && !(logDirectory != nil && len(*logDirectory) > 0) {
		_ = os.RemoveAll(l.LogDir)
		l.LogDir = ""
	}
}
