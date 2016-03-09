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

	"github.com/docker/engine-api/client"
	"github.com/docker/engine-api/types"
	"github.com/docker/engine-api/types/container"
	"github.com/docker/engine-api/types/events"
	"github.com/docker/engine-api/types/network"
	"github.com/docker/go-connections/nat"
	"golang.org/x/net/context"

	"github.com/cockroachdb/cockroach/base"
	roachClient "github.com/cockroachdb/cockroach/client"
	"github.com/cockroachdb/cockroach/rpc"
	"github.com/cockroachdb/cockroach/security"
	"github.com/cockroachdb/cockroach/util"
	"github.com/cockroachdb/cockroach/util/log"
	"github.com/cockroachdb/cockroach/util/stop"
)

const (
	builderImage     = "cockroachdb/builder"
	builderTag       = "20160305-182433"
	builderImageFull = builderImage + ":" + builderTag
)

const defaultTCP nat.Port = base.DefaultPort + "/tcp"

var cockroachImage = flag.String("i", builderImageFull, "the docker image to run")
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
	return fmt.Sprintf("roach%d", i)
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

type testStore struct {
	index   int
	dataStr string
	config  StoreConfig
}

type testNode struct {
	*Container
	index   int
	nodeStr string
	config  NodeConfig
	stores  []testStore
}

// LocalCluster manages a local cockroach cluster running on docker. The
// cluster is composed of a "volumes" container which manages the
// persistent volumes used for certs and node data and N cockroach nodes.
type LocalCluster struct {
	client               *client.Client
	stopper              chan struct{}
	mu                   sync.Mutex // Protects the fields below
	vols                 *Container
	config               TestConfig
	Nodes                []*testNode
	events               chan Event
	expectedEvents       chan Event
	oneshot              *Container
	CertsDir             string
	monitorCtx           context.Context
	monitorCtxCancelFunc func()
	logDir               string
	networkID            string
}

// CreateLocal creates a new local cockroach cluster. The stopper is used to
// gracefully shutdown the channel (e.g. when a signal arrives). The cluster
// must be started before being used.
func CreateLocal(cfg TestConfig, logDir string, stopper chan struct{}) *LocalCluster {
	select {
	case <-stopper:
		// The stopper was already closed, exit early.
		os.Exit(1)
	default:
	}

	if *cockroachImage == builderImageFull && !exists(*cockroachBinary) {
		log.Fatalf("\"%s\": does not exist", *cockroachBinary)
	}

	cli, err := client.NewEnvClient()
	maybePanic(err)

	return &LocalCluster{
		client:  cli,
		stopper: stopper,
		config:  cfg,
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
func (l *LocalCluster) OneShot(ipo types.ImagePullOptions, containerConfig container.Config, hostConfig container.HostConfig, name string) error {
	if err := pullImage(l, ipo); err != nil {
		return err
	}
	container, err := createContainer(l, containerConfig, hostConfig, nil, name)
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

func (l *LocalCluster) createNetwork() {
	l.panicOnStop()

	resp, err := l.client.NetworkCreate(types.NetworkCreate{
		Name:   "cockroachdb_acceptance",
		Driver: "bridge",
		// Docker gets very confused if two networks have the same name.
		CheckDuplicate: true,
	})
	maybePanic(err)
	if resp.Warning != "" {
		log.Warningf("creating network: %s", resp.Warning)
	}
	l.networkID = resp.ID
}

// create the volumes container that keeps all of the volumes used by
// the cluster.
func (l *LocalCluster) initCluster() {
	log.Infof("Initializing Cluster:\n%s", l.config.PrettyString())
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
		if !filepath.IsAbs(l.logDir) {
			l.logDir = filepath.Join(pwd, l.logDir)
		}
		binds = append(binds, l.logDir+":/logs")
		// If we don't make sure the directory exists, Docker will and then we
		// may run into ownership issues (think Docker running as root, but us
		// running as a regular Joe as it happens on CircleCI).
		maybePanic(os.MkdirAll(l.logDir, 0777))
	}
	if *cockroachImage == builderImageFull {
		path, err := filepath.Abs(*cockroachBinary)
		maybePanic(err)
		binds = append(binds, path+":/"+filepath.Base(*cockroachBinary))
	}

	l.Nodes = []*testNode{}
	vols := map[string]struct{}{}
	// Expand the cluster configuration into nodes and stores per node.
	var nodeCount int
	for _, nc := range l.config.Nodes {
		for i := 0; i < int(nc.Count); i++ {
			newTestNode := &testNode{
				config:  nc,
				index:   nodeCount,
				nodeStr: nodeStr(nodeCount),
			}
			nodeCount++
			var storeCount int
			for _, sc := range nc.Stores {
				for j := 0; j < int(sc.Count); j++ {
					vols[dataStr(nodeCount, storeCount)] = struct{}{}
					newTestNode.stores = append(newTestNode.stores,
						testStore{
							config:  sc,
							index:   j,
							dataStr: dataStr(nodeCount, storeCount),
						})
					storeCount++
				}
			}
			l.Nodes = append(l.Nodes, newTestNode)
		}
	}

	if *cockroachImage == builderImageFull {
		maybePanic(pullImage(l, types.ImagePullOptions{ImageID: builderImage, Tag: builderTag}))
	}
	c, err := createContainer(
		l,
		container.Config{
			Image:      *cockroachImage,
			Volumes:    vols,
			Entrypoint: []string{"/bin/true"},
		}, container.HostConfig{
			Binds:           binds,
			PublishAllPorts: true,
		},
		nil, "volumes",
	)
	maybePanic(err)
	maybePanic(c.Start())
	maybePanic(c.Wait())
	l.vols = c
}

func (l *LocalCluster) createRoach(node *testNode, vols *Container, env []string, cmd ...string) {
	l.panicOnStop()

	hostConfig := container.HostConfig{
		PublishAllPorts: true,
		NetworkMode:     container.NetworkMode(l.networkID),
	}

	if vols != nil {
		hostConfig.VolumesFrom = append(hostConfig.VolumesFrom, vols.id)
	}

	var hostname string
	if node.index >= 0 {
		hostname = fmt.Sprintf("roach%d", node.index)
	}
	var entrypoint []string
	if *cockroachImage == builderImageFull {
		entrypoint = append(entrypoint, "/"+filepath.Base(*cockroachBinary))
	} else if *cockroachEntry != "" {
		entrypoint = append(entrypoint, *cockroachEntry)
	}
	var err error
	node.Container, err = createContainer(
		l,
		container.Config{
			Hostname: hostname,
			Image:    *cockroachImage,
			ExposedPorts: map[nat.Port]struct{}{
				defaultTCP: {},
			},
			Entrypoint: entrypoint,
			Env:        env,
			Cmd:        cmd,
			Labels: map[string]string{
				// Allow for `docker ps --filter label=Hostname=roach0` or `--filter label=Roach`.
				"Hostname": hostname,
				"Roach":    "",
			},
		},
		hostConfig,
		&network.NetworkingConfig{
			EndpointsConfig: map[string]*network.EndpointSettings{
				l.networkID: {
					Aliases: []string{hostname},
				},
			},
		},
		node.nodeStr,
	)
	maybePanic(err)
}

func (l *LocalCluster) createCACert() {
	maybePanic(security.RunCreateCACert(l.CertsDir, keyLen))
}

func (l *LocalCluster) createNodeCerts() {
	nodes := []string{dockerIP().String()}
	for _, node := range l.Nodes {
		nodes = append(nodes, node.nodeStr)
	}
	maybePanic(security.RunCreateNodeCert(l.CertsDir, keyLen, nodes))
}

func (l *LocalCluster) startNode(node *testNode) {
	cmd := []string{
		"start",
		"--certs=/certs",
		"--host=" + node.nodeStr,
		"--alsologtostderr=INFO",
	}
	for _, store := range node.stores {
		cmd = append(cmd, fmt.Sprintf("--store=%s", store.dataStr))
	}
	// Append --join flag for all nodes except first.
	if node.index > 0 {
		cmd = append(cmd, "--join="+net.JoinHostPort(l.Nodes[0].nodeStr, base.DefaultPort))
	}

	var locallogDir string
	if len(l.logDir) > 0 {
		dockerlogDir := "/logs/" + node.nodeStr
		locallogDir = filepath.Join(l.logDir, node.nodeStr)
		maybePanic(os.MkdirAll(locallogDir, 0777))
		cmd = append(
			cmd,
			"--log-dir="+dockerlogDir,
			"--logtostderr=false",
			"--alsologtostderr=INFO")
	}
	// TODO(pmattis): Figure out why the Go DNS resolver is misbehaving.
	env := []string{"GODEBUG=netdns=cgo", "COCKROACH_SCAN_MAX_IDLE_TIME=200ms"}
	l.createRoach(node, l.vols, env, cmd...)
	maybePanic(node.Start())
	log.Infof(`*** started %[1]s ***
  ui:        %[2]s
  trace:     %[2]s/debug/requests
  logs:      %[3]s/cockroach.INFO
  pprof:     docker exec -it %[4]s /bin/bash -c 'go tool pprof /cockroach <(wget --no-check-certificate -qO- https://$(hostname):%[5]s/debug/pprof/heap)'
  cockroach: %[6]s`,
		node.Name(), "https://"+node.Addr().String(), locallogDir, node.Container.id[:5], base.DefaultPort, cmd)
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

	l.createNetwork()
	l.initCluster()
	log.Infof("creating certs (%dbit) in: %s", keyLen, l.CertsDir)
	l.createCACert()
	l.createNodeCerts()
	maybePanic(security.RunCreateClientCert(l.CertsDir, 512, security.RootUser))

	l.monitorCtx, l.monitorCtxCancelFunc = context.WithCancel(context.Background())
	go l.monitor()

	for _, node := range l.Nodes {
		l.startNode(node)
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
		act := filter(l.events, 15*time.Second)
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

	if l.vols != nil {
		maybePanic(l.vols.Kill())
		maybePanic(l.vols.Remove())
		l.vols = nil
	}
	if l.CertsDir != "" {
		_ = os.RemoveAll(l.CertsDir)
		l.CertsDir = ""
	}
	outputLogDir := l.logDir
	for i, n := range l.Nodes {
		ci, err := n.Inspect()
		crashed := err != nil || (!ci.State.Running && ci.State.ExitCode != 0)
		maybePanic(n.Kill())
		if crashed && outputLogDir == "" {
			outputLogDir = util.CreateTempDir(util.PanicTester, "crashed_nodes")
		}
		if crashed || l.logDir != "" {
			// TODO(bdarnell): make these filenames more consistent with
			// structured logs?
			file := filepath.Join(outputLogDir, nodeStr(i),
				fmt.Sprintf("stderr.%s.log", strings.Replace(
					time.Now().Format(time.RFC3339), ":", "_", -1)))

			maybePanic(os.MkdirAll(filepath.Dir(file), 0777))
			w, err := os.Create(file)
			maybePanic(err)
			defer w.Close()
			maybePanic(n.Logs(w))
			if crashed {
				log.Infof("node %d: stderr at %s", i, file)
			}
		}
		maybePanic(n.Remove())
	}
	l.Nodes = nil

	if l.networkID != "" {
		maybePanic(l.client.NetworkRemove(l.networkID))
		l.networkID = ""
	}
}

// NewClient implements the Cluster interface.
func (l *LocalCluster) NewClient(t *testing.T, i int) (*roachClient.DB, *stop.Stopper) {
	stopper := stop.NewStopper()
	rpcContext := rpc.NewContext(&base.Context{
		User:  security.NodeUser,
		Certs: l.CertsDir,
	}, nil, stopper)
	sender, err := roachClient.NewSender(rpcContext, l.Nodes[i].Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	return roachClient.NewDB(sender), stopper
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
		Host:     l.Nodes[i].Addr().String(),
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
	return "https://" + l.Nodes[i].Addr().String()
}
