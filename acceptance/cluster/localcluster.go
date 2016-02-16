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
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"net"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	dockerclient "github.com/docker/engine-api/client"
	"github.com/docker/engine-api/types"
	"github.com/docker/engine-api/types/container"
	"github.com/docker/engine-api/types/events"
	"github.com/docker/engine-api/types/strslice"
	"github.com/docker/go-connections/nat"
	"golang.org/x/net/context"

	"github.com/cockroachdb/cockroach/base"
	"github.com/cockroachdb/cockroach/security"
	"github.com/cockroachdb/cockroach/util/log"
)

const (
	builderImage   = "cockroachdb/builder"
	dockerspyImage = "cockroachdb/docker-spy"
	dockerspyTag   = "20160209-143235"
	domain         = "local"
)

var (
	cockroachTCP nat.Port = base.CockroachPort + "/tcp"
	pgTCP        nat.Port = base.PGPort + "/tcp"
)

var cockroachImage = flag.String("i", builderImage, "the docker image to run")
var cockroachBinary = flag.String("b", defaultBinary(), "the binary to run (if image == "+builderImage+")")
var cockroachEntry = flag.String("e", "", "the entry point for the image")
var waitOnStop = flag.Bool("w", false, "wait for the user to interrupt before tearing down the cluster")
var pwd = filepath.Clean(os.ExpandEnv("${PWD}"))

// keyLen is the length (in bits) of the generated CA and node certs.
const keyLen = 1024

func prettyJSON(v interface{}) string {
	pretty, err := json.MarshalIndent(v, "", "  ")
	maybePanic(err)
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

func nodeStr(i int) string {
	return fmt.Sprintf("roach%d.%s", i, domain)
}

func dataStr(node, store int) string {
	return fmt.Sprintf("/data%d.%d", node, store)
}

// The various event types.
const (
	eventDie     = "die"
	eventRestart = "restart"
)

// Event for a node containing a node index and the type of event.
type Event struct {
	NodeIndex int
	Status    string
}

// LocalCluster manages a local cockroach cluster running on docker. The
// cluster is composed of a "dns" container which automatically registers dns
// entries for the cockroach nodes, a "volumes" container which manages the
// persistent volumes used for certs and node data and N cockroach nodes.
type LocalCluster struct {
	client               *dockerclient.Client
	stopper              chan struct{}
	mu                   sync.Mutex // Protects the fields below
	dns                  *Container
	vols                 *Container
	numLocal             int
	numStores            int
	Nodes                []*Container
	events               chan Event
	expectedEvents       chan Event
	oneshot              *Container
	CertsDir             string
	monitorCtx           context.Context
	monitorCtxCancelFunc func()
	logDir               string
	keepLogs             bool
}

// CreateLocal creates a new local cockroach cluster. The stopper is used to
// gracefully shutdown the channel (e.g. when a signal arrives). The cluster
// must be started before being used.
func CreateLocal(numLocal, numStores int, logDir string, stopper chan struct{}) *LocalCluster {
	select {
	case <-stopper:
		// The stopper was already closed, exit early.
		os.Exit(1)
	default:
	}

	if *cockroachImage == builderImage && !exists(*cockroachBinary) {
		log.Fatalf("\"%s\": does not exist", *cockroachBinary)
	}

	cli, err := dockerclient.NewEnvClient()
	maybePanic(err)

	return &LocalCluster{
		client:    cli,
		stopper:   stopper,
		numLocal:  numLocal,
		numStores: numStores,
		// TODO(tschottdorf): deadlocks will occur if these channels fill up.
		events:         make(chan Event, 1000),
		expectedEvents: make(chan Event, 1000),
		logDir:         logDir,
	}
}

func (l *LocalCluster) expectEvent(c *Container, msgs ...string) {
	for index, ctr := range l.Nodes {
		if c.id != ctr.id {
			continue
		}
		for _, status := range msgs {
			l.expectedEvents <- Event{NodeIndex: index, Status: status}
		}
		break
	}
}

// OneShot runs a container, expecting it to successfully run to completion
// and die, after which it is removed. Not goroutine safe: only one OneShot
// can be running at once.
func (l *LocalCluster) OneShot(ipo types.ImagePullOptions, containerConfig container.Config, hostConfig container.HostConfig) error {
	if err := pullImage(l, ipo); err != nil {
		return err
	}
	container, err := createContainer(l, containerConfig, hostConfig, "")
	if err != nil {
		return err
	}
	l.oneshot = container
	defer func() {
		if err := l.oneshot.Remove(); err != nil {
			log.Errorf("ContainerRemove: %s", err)
		}
		l.oneshot = nil
	}()

	if err := l.oneshot.Start(); err != nil {
		return err
	}
	return l.oneshot.Wait()
}

// stopOnPanic is invoked as a deferred function in Start in order to attempt
// to tear down the cluster if a panic occurs while starting it. If the panic
// was initiated by the stopper being closed (which panicOnStop notices) then
// the process is exited with a failure code.
func (l *LocalCluster) stopOnPanic() {
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
func (l *LocalCluster) panicOnStop() {
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

func (l *LocalCluster) runDockerSpy() {
	l.panicOnStop()

	create := func() (*Container, error) {
		return createContainer(l,
			container.Config{
				Image: dockerspyImage + ":" + dockerspyTag,
				Cmd:   strslice.New("--dns-domain=" + domain),
			}, container.HostConfig{
				Binds:           []string{"/var/run/docker.sock:/var/run/docker.sock"},
				PublishAllPorts: true,
			},
			"docker-spy",
		)
	}
	c, err := create()
	if dockerclient.IsErrImageNotFound(err) {
		if err := pullImage(l, types.ImagePullOptions{ImageID: dockerspyImage, Tag: dockerspyTag}); err != nil {
			log.Fatal(err)
		}

		c, err = create()
	}
	maybePanic(err)
	maybePanic(c.Start())
	l.dns = c
	if ci, err := c.Inspect(); err != nil {
		log.Error(err)
	} else {
		log.Infof("started %s: %s", c.Name(), ci.NetworkSettings.IPAddress)
	}
}

// create the volumes container that keeps all of the volumes used by
// the cluster.
func (l *LocalCluster) initCluster() {
	log.Infof("initializing cluster")
	l.panicOnStop()

	// Create the temporary certs directory in the current working
	// directory. Boot2docker's handling of binding local directories
	// into the container is very confusing. If the directory being
	// bound has a parent directory that exists in the boot2docker VM
	// then that directory is bound into the container. In particular,
	// that means that binds of /tmp and /var will be problematic.
	var err error
	l.CertsDir, err = ioutil.TempDir(pwd, ".localcluster.certs.")
	maybePanic(err)

	binds := []string{
		l.CertsDir + ":/certs",
		filepath.Join(pwd, "..") + ":/go/src/github.com/cockroachdb/cockroach",
	}

	if l.logDir != "" {
		l.keepLogs = true
		if !filepath.IsAbs(l.logDir) {
			l.logDir = filepath.Join(pwd, l.logDir)
		}
		binds = append(binds, l.logDir+":/logs")
		// If we don't make sure the directory exists, Docker will and then we
		// may run into ownership issues (think Docker running as root, but us
		// running as a regular Joe as it happens on CircleCI).
		maybePanic(os.MkdirAll(l.logDir, 0777))
	}
	if *cockroachImage == builderImage {
		path, err := filepath.Abs(*cockroachBinary)
		maybePanic(err)
		binds = append(binds, path+":/"+filepath.Base(*cockroachBinary))
	}

	vols := map[string]struct{}{}
	for i := 0; i < l.numLocal; i++ {
		for j := 0; j < l.numStores; j++ {
			vols[dataStr(i, j)] = struct{}{}
		}
	}
	create := func() (*Container, error) {
		return createContainer(
			l,
			container.Config{
				Image:      *cockroachImage,
				Volumes:    vols,
				Entrypoint: strslice.New("/bin/true"),
			}, container.HostConfig{
				Binds:           binds,
				PublishAllPorts: true,
			},
			"volumes",
		)
	}
	c, err := create()
	if dockerclient.IsErrImageNotFound(err) && *cockroachImage == builderImage {
		if err := pullImage(l, types.ImagePullOptions{ImageID: *cockroachImage}); err != nil {
			log.Fatal(err)
		}
		c, err = create()
	}
	maybePanic(err)

	maybePanic(c.Start())
	maybePanic(c.Wait())
	l.vols = c
}

func (l *LocalCluster) createRoach(i int, dns, vols *Container, cmd ...string) *Container {
	l.panicOnStop()

	hostConfig := container.HostConfig{
		PublishAllPorts: true,
	}

	if dns != nil {
		ci, err := dns.Inspect()
		maybePanic(err)
		hostConfig.DNS = append(hostConfig.DNS, ci.NetworkSettings.IPAddress)
	}
	if vols != nil {
		hostConfig.VolumesFrom = append(hostConfig.VolumesFrom, vols.id)
	}

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
	c, err := createContainer(
		l,
		container.Config{
			Hostname:   hostname,
			Domainname: domain,
			Image:      *cockroachImage,
			ExposedPorts: map[nat.Port]struct{}{
				cockroachTCP: {},
				pgTCP:        {},
			},
			Entrypoint: strslice.New(entrypoint...),
			Cmd:        strslice.New(cmd...),
			Labels: map[string]string{
				// Allow for `docker ps --filter label=Hostname=roach0` or `--filter label=Roach`.
				"Hostname": hostname,
				"Roach":    "",
			},
		},
		hostConfig,
		nodeStr(i),
	)
	maybePanic(err)
	return c
}

func (l *LocalCluster) createCACert() {
	maybePanic(security.RunCreateCACert(l.CertsDir, keyLen))
}

func (l *LocalCluster) createNodeCerts() {
	nodes := []string{dockerIP().String()}
	for i := 0; i < l.numLocal; i++ {
		nodes = append(nodes, nodeStr(i))
	}
	maybePanic(security.RunCreateNodeCert(l.CertsDir, keyLen, nodes))
}

func (l *LocalCluster) startNode(i int) *Container {
	var stores = "ssd=" + dataStr(i, 0)
	for j := 1; j < l.numStores; j++ {
		stores += ",ssd=" + dataStr(i, j)
	}

	cmd := []string{
		"start",
		"--stores=" + stores,
		"--certs=/certs",
		"--host=" + nodeStr(i),
		"--port=" + base.CockroachPort,
		"--scan-max-idle-time=200ms", // set low to speed up tests
	}
	// Append --join flag for all nodes except first.
	if i > 0 {
		cmd = append(cmd, "--join="+net.JoinHostPort(nodeStr(0), base.CockroachPort))
	}

	var locallogDir string
	if len(l.logDir) > 0 {
		dockerlogDir := "/logs/" + nodeStr(i)
		locallogDir = filepath.Join(l.logDir, nodeStr(i))
		maybePanic(os.MkdirAll(locallogDir, 0777))
		cmd = append(
			cmd,
			"--log-dir="+dockerlogDir,
			"--logtostderr=false",
			"--alsologtostderr=true")
	}
	c := l.createRoach(i, l.dns, l.vols, cmd...)
	maybePanic(c.Start())
	uri := fmt.Sprintf("https://%s", c.Addr(""))
	// Infof doesn't take positional parameters, hence the Sprintf.
	log.Infof(fmt.Sprintf(`*** started %[1]s ***
  ui:    %[2]s
  trace: %[2]s/debug/requests
  logs:  %[3]s/cockroach.INFO
  pprof: docker exec -it %[4]s /bin/bash -c 'go tool pprof /cockroach <(wget --no-check-certificate -qO- https://$(hostname):%[5]s/debug/pprof/heap)'`,
		c.Name(), uri, locallogDir, c.id[:5], base.CockroachPort))
	return c
}

func (l *LocalCluster) processEvent(event events.Message) bool {
	l.mu.Lock()
	defer l.mu.Unlock()

	// If there's currently a oneshot container, ignore any die messages from
	// it because those are expected.
	if l.oneshot != nil && event.ID == l.oneshot.id && event.Status == eventDie {
		return true
	}

	for i, n := range l.Nodes {
		if n != nil && n.id == event.ID {
			if log.V(1) {
				log.Errorf("node=%d status=%s", i, event.Status)
			}
			l.events <- Event{NodeIndex: i, Status: event.Status}
			return true
		}
	}

	// An event on any other container is unexpected. Die.
	select {
	case <-l.stopper:
	case <-l.monitorCtx.Done():
	default:
		// There is a very tiny race here: the signal handler might be closing the
		// stopper simultaneously.
		log.Errorf("stopping due to unexpected event: %+v", event)
		if rc, err := l.client.ContainerLogs(context.Background(), types.ContainerLogsOptions{
			ContainerID: event.Actor.ID,
			ShowStdout:  true,
			ShowStderr:  true,
		}); err == nil {
			defer rc.Close()
			if _, err := io.Copy(os.Stderr, rc); err != nil {
				log.Infof("error listing logs: %s", err)
			}
		}
		close(l.stopper)
	}
	return false
}

func (l *LocalCluster) monitor() {
	rc, err := l.client.Events(l.monitorCtx, types.EventsOptions{})
	maybePanic(err)
	defer rc.Close()
	dec := json.NewDecoder(rc)
	for {
		var event events.Message
		if err := dec.Decode(&event); err != nil {
			break
		}

		// Currently, the only events generated (and asserted against) are "die"
		// and "restart", to maximize compatibility across different versions of
		// Docker.
		switch event.Status {
		case eventDie, eventRestart:
		default:
			continue
		}

		if !l.processEvent(event) {
			break
		}
	}
}

// Start starts the cluster.
func (l *LocalCluster) Start() {
	defer l.stopOnPanic()

	l.mu.Lock()
	defer l.mu.Unlock()

	l.runDockerSpy()
	l.initCluster()
	log.Infof("creating certs (%dbit) in: %s", keyLen, l.CertsDir)
	l.createCACert()
	l.createNodeCerts()
	maybePanic(security.RunCreateClientCert(l.CertsDir, 512, security.RootUser))

	l.monitorCtx, l.monitorCtxCancelFunc = context.WithCancel(context.Background())
	go l.monitor()
	l.Nodes = make([]*Container, l.numLocal)
	for i := range l.Nodes {
		l.Nodes[i] = l.startNode(i)
	}
}

// Assert drains the Events channel and compares the actual events with those
// expected to have been generated by the operations performed on the nodes in
// the cluster (restart, kill, ...). In the event of a mismatch, the passed
// Tester receives a fatal error.
func (l *LocalCluster) Assert(t *testing.T) {
	const almostZero = 50 * time.Millisecond
	filter := func(ch chan Event, wait time.Duration) *Event {
		select {
		case act := <-ch:
			return &act
		case <-time.After(wait):
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
func (l *LocalCluster) AssertAndStop(t *testing.T) {
	defer l.stop()
	l.Assert(t)
}

// stop stops the cluster.
func (l *LocalCluster) stop() {
	if *waitOnStop {
		log.Infof("waiting for interrupt")
		select {
		case <-l.stopper:
		}
	}

	log.Infof("stopping")

	l.mu.Lock()
	defer l.mu.Unlock()

	if l.monitorCtxCancelFunc != nil {
		l.monitorCtxCancelFunc()
		l.monitorCtxCancelFunc = nil
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
		if len(l.logDir) > 0 {
			// TODO(bdarnell): make these filenames more consistent with
			// structured logs?
			file := filepath.Join(l.logDir, nodeStr(i),
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
	// Removing the log files must happen after shutting down the nodes or
	// there might be a panic when it tries to log that it is killing the node.
	if !l.keepLogs {
		_ = os.RemoveAll(l.logDir)
		l.logDir = ""
	}
}

// ConnString creates a connections string.
func (l *LocalCluster) ConnString(i int) string {
	return "rpcs://" + security.NodeUser + "@" +
		l.Nodes[i].Addr("").String() +
		"?certs=" + l.CertsDir
}

// PGAddr returns the Postgres address for the given node.
func (l *LocalCluster) PGAddr(i int) *net.TCPAddr {
	return l.Nodes[i].PGAddr()
}

// PGUrl returns a URL string for the given node postgres server.
func (l *LocalCluster) PGUrl(i int) string {
	certUser := security.RootUser
	options := url.Values{}
	options.Add("sslmode", "verify-full")
	options.Add("sslcert", security.ClientCertPath(l.CertsDir, certUser))
	options.Add("sslkey", security.ClientKeyPath(l.CertsDir, certUser))
	options.Add("sslrootcert", security.CACertPath(l.CertsDir))
	pgURL := url.URL{
		Scheme:   "postgres",
		User:     url.User(certUser),
		Host:     l.Nodes[i].PGAddr().String(),
		RawQuery: options.Encode(),
	}
	return pgURL.String()
}

// NumNodes returns the number of nodes in the cluster.
func (l *LocalCluster) NumNodes() int {
	return len(l.Nodes)
}

// Kill kills the i-th node.
func (l *LocalCluster) Kill(i int) error {
	return l.Nodes[i].Kill()
}

// Restart restarts the given node. If the node isn't running, this starts it.
func (l *LocalCluster) Restart(i int) error {
	return l.Nodes[i].Restart(5)
}

// URL returns the base url.
func (l *LocalCluster) URL(i int) string {
	return "https://" + l.Nodes[i].Addr(cockroachTCP).String()
}
