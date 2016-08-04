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
	"bytes"
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
	"testing"
	"time"

	"github.com/docker/engine-api/client"
	"github.com/docker/engine-api/types"
	"github.com/docker/engine-api/types/container"
	"github.com/docker/engine-api/types/events"
	"github.com/docker/go-connections/nat"
	"github.com/pkg/errors"
	"golang.org/x/net/context"

	"github.com/cockroachdb/cockroach/base"
	"github.com/cockroachdb/cockroach/config"
	roachClient "github.com/cockroachdb/cockroach/internal/client"
	"github.com/cockroachdb/cockroach/rpc"
	"github.com/cockroachdb/cockroach/security"
	"github.com/cockroachdb/cockroach/server"
	"github.com/cockroachdb/cockroach/util"
	"github.com/cockroachdb/cockroach/util/log"
	"github.com/cockroachdb/cockroach/util/stop"
	"github.com/cockroachdb/cockroach/util/syncutil"
	"github.com/cockroachdb/cockroach/util/timeutil"
)

const (
	builderImage     = "cockroachdb/builder"
	builderTag       = "20160804-184345"
	builderImageFull = builderImage + ":" + builderTag
	networkName      = "cockroachdb_acceptance"
)

// DefaultTCP is the default SQL/RPC port specification.
const DefaultTCP nat.Port = base.DefaultPort + "/tcp"
const defaultHTTP nat.Port = base.DefaultHTTPPort + "/tcp"

var cockroachImage = flag.String("i", builderImageFull, "the docker image to run")
var cockroachBinary = flag.String("b", defaultBinary(), "the binary to run (if image == "+builderImage+")")
var cockroachEntry = flag.String("e", "", "the entry point for the image")
var waitOnStop = flag.Bool("w", false, "wait for the user to interrupt before tearing down the cluster")
var pwd = filepath.Clean(os.ExpandEnv("${PWD}"))
var maxRangeBytes = config.DefaultZoneConfig().RangeMaxBytes

// keyLen is the length (in bits) of the generated CA and node certs.
const keyLen = 1024

func defaultBinary() string {
	gopath := filepath.SplitList(os.Getenv("GOPATH"))
	if len(gopath) == 0 {
		return ""
	}
	return gopath[0] + "/bin/docker_amd64/cockroach"
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
	client               client.APIClient
	stopper              chan struct{}
	mu                   syncutil.Mutex // Protects the fields below
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
	privileged           bool // whether to run containers in privileged mode
}

// CreateLocal creates a new local cockroach cluster. The stopper is used to
// gracefully shutdown the channel (e.g. when a signal arrives). The cluster
// must be started before being used.
func CreateLocal(cfg TestConfig, logDir string, privileged bool, stopper chan struct{}) *LocalCluster {
	select {
	case <-stopper:
		// The stopper was already closed, exit early.
		os.Exit(1)
	default:
	}

	if *cockroachImage == builderImageFull && !exists(*cockroachBinary) {
		log.Fatalf(context.TODO(), "\"%s\": does not exist", *cockroachBinary)
	}

	cli, err := client.NewEnvClient()
	maybePanic(err)

	retryingClient := retryingDockerClient{
		resilientDockerClient: resilientDockerClient{APIClient: cli},
		attempts:              10,
		timeout:               10 * time.Second,
	}

	return &LocalCluster{
		client:  retryingClient,
		stopper: stopper,
		config:  cfg,
		// TODO(tschottdorf): deadlocks will occur if these channels fill up.
		events:         make(chan Event, 1000),
		expectedEvents: make(chan Event, 1000),
		logDir:         logDir,
		privileged:     privileged,
	}
}

func (l *LocalCluster) expectEvent(c *Container, msgs ...string) {
	for index, ctr := range l.Nodes {
		if c.id != ctr.id {
			continue
		}
		for _, status := range msgs {
			select {
			case l.expectedEvents <- Event{NodeIndex: index, Status: status}:
			default:
				panic("expectedEvents channel filled up")
			}
		}
		break
	}
}

// OneShot runs a container, expecting it to successfully run to completion
// and die, after which it is removed. Not goroutine safe: only one OneShot
// can be running at once.
// Adds the same binds as the cluster containers (certs, binary, etc).
func (l *LocalCluster) OneShot(
	ref string,
	ipo types.ImagePullOptions,
	containerConfig container.Config,
	hostConfig container.HostConfig,
	name string,
) error {
	if err := pullImage(l, ref, ipo); err != nil {
		return err
	}
	hostConfig.VolumesFrom = []string{l.vols.id}
	container, err := createContainer(l, containerConfig, hostConfig, name)
	if err != nil {
		return err
	}
	l.oneshot = container
	defer func() {
		if err := l.oneshot.Remove(); err != nil {
			log.Errorf(context.TODO(), "ContainerRemove: %s", err)
		}
		l.oneshot = nil
	}()

	if err := l.oneshot.Start(); err != nil {
		return err
	}
	if err := l.oneshot.Wait(); err != nil {
		return err
	}
	return nil
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

	net, err := l.client.NetworkInspect(context.Background(), networkName)
	if err == nil {
		// We need to destroy the network and any running containers inside of it.
		for containerID := range net.Containers {
			// This call could fail if the container terminated on its own after we call
			// NetworkInspect, but the likelihood of this seems low. If this line creates
			// a lot of panics we should do more careful error checking.
			maybePanic(l.client.ContainerKill(context.Background(), containerID, "9"))
		}
		maybePanic(l.client.NetworkRemove(context.Background(), networkName))
	} else if !client.IsErrNotFound(err) {
		panic(err)
	}

	resp, err := l.client.NetworkCreate(context.Background(), networkName, types.NetworkCreate{
		Driver: "bridge",
		// Docker gets very confused if two networks have the same name.
		CheckDuplicate: true,
	})
	maybePanic(err)
	if resp.Warning != "" {
		log.Warningf(context.TODO(), "creating network: %s", resp.Warning)
	}
	l.networkID = resp.ID
}

// create the volumes container that keeps all of the volumes used by
// the cluster.
func (l *LocalCluster) initCluster() {
	configJSON, err := json.Marshal(l.config)
	maybePanic(err)
	log.Infof(context.TODO(), "Initializing Cluster %s:\n%s", l.config.Name, configJSON)
	l.panicOnStop()

	// Create the temporary certs directory in the current working
	// directory. Boot2docker's handling of binding local directories
	// into the container is very confusing. If the directory being
	// bound has a parent directory that exists in the boot2docker VM
	// then that directory is bound into the container. In particular,
	// that means that binds of /tmp and /var will be problematic.
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
		maybePanic(pullImage(l, builderImage+":"+builderTag, types.ImagePullOptions{}))
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
		"volumes",
	)
	maybePanic(err)
	// Make sure this assignment to l.vols is before the calls to Start and Wait.
	// Otherwise, if they trigger maybePanic, this container won't get cleaned up
	// and it'll get in the way of future runs.
	l.vols = c
	maybePanic(c.Start())
	maybePanic(c.Wait())
}

func (l *LocalCluster) createRoach(node *testNode, vols *Container, env []string, cmd ...string) {
	l.panicOnStop()

	hostConfig := container.HostConfig{
		PublishAllPorts: true,
		NetworkMode:     container.NetworkMode(l.networkID),
		Privileged:      l.privileged,
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
				DefaultTCP:  {},
				defaultHTTP: {},
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
		node.nodeStr,
	)
	maybePanic(err)
}

func (l *LocalCluster) createCACert() {
	maybePanic(security.RunCreateCACert(
		filepath.Join(l.CertsDir, security.EmbeddedCACert),
		filepath.Join(l.CertsDir, security.EmbeddedCAKey),
		keyLen))
}

func (l *LocalCluster) createNodeCerts() {
	nodes := []string{dockerIP().String()}
	for _, node := range l.Nodes {
		nodes = append(nodes, node.nodeStr)
	}
	maybePanic(security.RunCreateNodeCert(
		filepath.Join(l.CertsDir, security.EmbeddedCACert),
		filepath.Join(l.CertsDir, security.EmbeddedCAKey),
		filepath.Join(l.CertsDir, security.EmbeddedNodeCert),
		filepath.Join(l.CertsDir, security.EmbeddedNodeKey),
		keyLen, nodes))
}

func (l *LocalCluster) startNode(node *testNode) {
	cmd := []string{
		"start",
		"--ca-cert=/certs/ca.crt",
		"--cert=/certs/node.crt",
		"--key=/certs/node.key",
		"--host=" + node.nodeStr,
		"--alsologtostderr=INFO",
		"--verbosity=1",
	}

	for _, store := range node.stores {
		storeSpec := server.StoreSpec{
			Path:        store.dataStr,
			SizeInBytes: int64(store.config.MaxRanges) * maxRangeBytes,
		}
		cmd = append(cmd, fmt.Sprintf("--store=%s", storeSpec))
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
			"--logtostderr=false")

	}
	env := []string{
		"COCKROACH_SCAN_MAX_IDLE_TIME=200ms",
		"COCKROACH_CONSISTENCY_CHECK_PANIC_ON_FAILURE=true",
		"COCKROACH_SKIP_UPDATE_CHECK=1",
	}
	l.createRoach(node, l.vols, env, cmd...)
	maybePanic(node.Start())
	httpAddr := node.Addr(defaultHTTP)

	log.Infof(context.TODO(), `*** started %[1]s ***
  ui:        %[2]s
  trace:     %[2]s/debug/requests
  logs:      %[3]s/cockroach.INFO
  pprof:     docker exec -it %[4]s /bin/bash -c 'go tool pprof /cockroach <(wget --no-check-certificate -qO- https://$(hostname):%[5]s/debug/pprof/heap)'
  cockroach: %[6]s

  cli-env:   COCKROACH_INSECURE=false COCKROACH_CA_CERT=%[7]s/ca.crt COCKROACH_CERT=%[7]s/node.crt COCKROACH_KEY=%[7]s/node.key COCKROACH_HOST=%s COCKROACH_PORT=%d`,
		node.Name(), "https://"+httpAddr.String(), locallogDir, node.Container.id[:5],
		base.DefaultHTTPPort, cmd, l.CertsDir, httpAddr.IP, httpAddr.Port)
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
				log.Errorf(context.TODO(), "node=%d status=%s", i, event.Status)
			}
			select {
			case l.events <- Event{NodeIndex: i, Status: event.Status}:
			default:
				panic("events channel filled up")
			}
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
		log.Errorf(context.TODO(), "stopping due to unexpected event: %+v", event)
		if rc, err := l.client.ContainerLogs(context.Background(), event.Actor.ID, types.ContainerLogsOptions{
			ShowStdout: true,
			ShowStderr: true,
		}); err == nil {
			defer rc.Close()
			if _, err := io.Copy(os.Stderr, rc); err != nil {
				log.Infof(context.TODO(), "error listing logs: %s", err)
			}
		}
		close(l.stopper)
	}
	return false
}

func (l *LocalCluster) monitor() {
	if log.V(1) {
		log.Infof(context.TODO(), "events monitor starts")
		defer log.Infof(context.TODO(), "events monitor exits")
	}
	longPoll := func() bool {
		// If our context was cancelled, it's time to go home.
		if l.monitorCtx.Err() != nil {
			return false
		}
		rc, err := l.client.Events(l.monitorCtx, types.EventsOptions{})
		maybePanic(err)
		defer rc.Close()
		dec := json.NewDecoder(rc)
		for {
			var event events.Message
			if err := dec.Decode(&event); err != nil {
				log.Infof(context.TODO(), "event stream done, resetting...: %s", err)
				// Sometimes we get a random string-wrapped EOF error back.
				// Hard to assert on, so we just let this goroutine spin.
				return true
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
				return false
			}
		}
	}

	for longPoll() {
	}
}

// Start starts the cluster.
func (l *LocalCluster) Start() {
	defer l.stopOnPanic()

	l.mu.Lock()
	defer l.mu.Unlock()

	l.createNetwork()
	l.initCluster()
	log.Infof(context.TODO(), "creating certs (%dbit) in: %s", keyLen, l.CertsDir)
	l.createCACert()
	l.createNodeCerts()
	maybePanic(security.RunCreateClientCert(
		filepath.Join(l.CertsDir, security.EmbeddedCACert),
		filepath.Join(l.CertsDir, security.EmbeddedCAKey),
		filepath.Join(l.CertsDir, security.EmbeddedRootCert),
		filepath.Join(l.CertsDir, security.EmbeddedRootKey),
		512, security.RootUser))

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
		log.Infof(context.TODO(), "asserted %v", events)
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
		log.Infof(context.TODO(), "waiting for interrupt")
		<-l.stopper
	}

	log.Infof(context.TODO(), "stopping")

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
		if n.Container == nil {
			continue
		}
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
					timeutil.Now().Format(time.RFC3339), ":", "_", -1)))

			maybePanic(os.MkdirAll(filepath.Dir(file), 0777))
			w, err := os.Create(file)
			maybePanic(err)
			defer w.Close()
			maybePanic(n.Logs(w))
			if crashed {
				log.Infof(context.TODO(), "node %d: stderr at %s", i, file)
			}
		}
		maybePanic(n.Remove())
	}
	l.Nodes = nil

	if l.networkID != "" {
		maybePanic(
			l.client.NetworkRemove(context.Background(), l.networkID))
		l.networkID = ""
	}
}

// NewClient implements the Cluster interface.
func (l *LocalCluster) NewClient(t *testing.T, i int) (*roachClient.DB, *stop.Stopper) {
	stopper := stop.NewStopper()
	rpcContext := rpc.NewContext(&base.Context{
		User:       security.NodeUser,
		SSLCA:      filepath.Join(l.CertsDir, security.EmbeddedCACert),
		SSLCert:    filepath.Join(l.CertsDir, security.EmbeddedNodeCert),
		SSLCertKey: filepath.Join(l.CertsDir, security.EmbeddedNodeKey),
	}, nil, stopper)
	sender, err := roachClient.NewSender(rpcContext, l.Nodes[i].Addr(DefaultTCP).String())
	if err != nil {
		t.Fatal(err)
	}
	return roachClient.NewDB(sender), stopper
}

// InternalIP returns the IP address used for inter-node communication.
func (l *LocalCluster) InternalIP(i int) net.IP {
	c := l.Nodes[i]
	containerInfo, err := c.Inspect()
	if err != nil {
		return nil
	}
	return net.ParseIP(containerInfo.NetworkSettings.Networks[networkName].IPAddress)
}

// PGUrl returns a URL string for the given node postgres server.
func (l *LocalCluster) PGUrl(i int) string {
	certUser := security.RootUser
	options := url.Values{}
	options.Add("sslmode", "verify-full")
	options.Add("sslcert", filepath.Join(l.CertsDir, security.EmbeddedRootCert))
	options.Add("sslkey", filepath.Join(l.CertsDir, security.EmbeddedRootKey))
	options.Add("sslrootcert", filepath.Join(l.CertsDir, security.EmbeddedCACert))
	pgURL := url.URL{
		Scheme:   "postgres",
		User:     url.User(certUser),
		Host:     l.Nodes[i].Addr(DefaultTCP).String(),
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
	// The default timeout is 10 seconds.
	return l.Nodes[i].Restart(nil)
}

// URL returns the base url.
func (l *LocalCluster) URL(i int) string {
	return "https://" + l.Nodes[i].Addr(defaultHTTP).String()
}

// Addr returns the host and port from the node in the format HOST:PORT.
func (l *LocalCluster) Addr(i int, port string) string {
	return l.Nodes[i].Addr(nat.Port(port + "/tcp")).String()
}

// ExecRoot runs a command as root.
func (l *LocalCluster) ExecRoot(i int, cmd []string) error {
	execRoot := func(ctx context.Context) error {
		cfg := types.ExecConfig{
			User:         "root",
			Privileged:   true,
			Cmd:          cmd,
			AttachStderr: true,
			AttachStdout: true,
		}
		createResp, err := l.client.ContainerExecCreate(ctx, l.Nodes[i].Container.id, cfg)
		if err != nil {
			return err
		}
		var outputStream, errorStream bytes.Buffer
		{
			resp, err := l.client.ContainerExecAttach(ctx, createResp.ID, cfg)
			if err != nil {
				return err
			}
			defer resp.Close()
			ch := make(chan error)
			go func() {
				_, err := StdCopy(&outputStream, &errorStream, resp.Reader)
				ch <- err
			}()
			if err := <-ch; err != nil {
				return err
			}
		}
		{
			resp, err := l.client.ContainerExecInspect(ctx, createResp.ID)
			if err != nil {
				return err
			}
			if resp.Running {
				return errors.Errorf("command still running")
			}
			if resp.ExitCode != 0 {
				return fmt.Errorf("error executing %s:\n%s\n%s",
					cmd, outputStream.String(),
					errorStream.String())
			}
		}
		return nil
	}

	return retry(context.Background(), 3, 10*time.Second, "ExecRoot",
		matchNone, execRoot)
}
