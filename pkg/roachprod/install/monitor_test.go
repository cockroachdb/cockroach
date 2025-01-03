// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package install

import (
	"context"
	"io"
	"sort"
	"strings"
	"sync"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/roachprod/cloud"
	"github.com/cockroachdb/cockroach/pkg/testutils/datapathutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/datadriven"
	"github.com/stretchr/testify/require"
)

func TestMonitorDataDriven(t *testing.T) {
	defer leaktest.AfterTest(t)()
	datadriven.Walk(t, datapathutils.TestDataPath(t, "monitor"), func(t *testing.T, path string) {
		var monitorChan chan NodeMonitorInfo
		wg := &sync.WaitGroup{}
		sessions := make([]*MockSession, 0)
		sessionChan := make([]chan []byte, 0)
		ctx := context.Background()
		c := &SyncedCluster{
			Cluster: cloud.Cluster{
				Name: "test-cluster",
			},
			sessionProvider: func(node Node, cmd string) session {
				return sessions[node-1]
			},
		}
		datadriven.RunTest(t, path, func(t *testing.T, td *datadriven.TestData) string {
			switch td.Cmd {
			case "monitor":
				nodeCount := 1
				td.MaybeScanArgs(t, "nodes", &nodeCount)
				c.Nodes = make([]Node, nodeCount)

				wg.Add(nodeCount)
				for i := 0; i < nodeCount; i++ {
					c.Nodes[i] = Node(i + 1)
					sessions = append(sessions, NewMockSession(DefaultMockSessionOptions()))
					sessionChan = append(sessionChan, make(chan []byte))
					go func(ch chan []byte, s *MockSession) {
						defer wg.Done()
						for sc := range ch {
							_, err := s.Stdout.Write(sc)
							require.NoError(t, err)
						}
					}(sessionChan[i], sessions[i])
				}
				monitorChan = c.Monitor(nilLogger(), ctx, MonitorOpts{})
				return ""
			case "write":
				node := 1
				td.ScanArgs(t, "node", &node)
				input := strings.ReplaceAll(td.Input, `<\n>`, "\n")
				sessionChan[node-1] <- []byte(input)
				return ""
			case "events":
				count := 0
				td.ScanArgs(t, "count", &count)
				var buf strings.Builder
				events := make([]NodeMonitorInfo, 0, count)
				for i := 0; i < count; i++ {
					e := <-monitorChan
					events = append(events, e)
				}
				// We sort the events to make the test deterministic. To test timing of
				// events, multiple write and event command combinations can be used.
				sort.Slice(events, func(i, j int) bool {
					return strings.Compare(events[i].String(), events[j].String()) < 0
				})
				for _, e := range events {
					buf.WriteString(e.String())
					buf.WriteString("\n")
				}
				return buf.String()
			default:
				t.Fatalf("unknown command: %s", td.Cmd)
			}
			return ""
		})
		for _, s := range sessions {
			s.Close()
		}
		// We expect EOF errors for all the nodes. This is to ensure that for the
		// data driven tests all events have been consumed. Any other error or event
		// would be considered a failure.
		for e := range monitorChan {
			require.IsType(t, MonitorError{}, e.Event)
			if e.Event.(MonitorError).Err != io.EOF {
				t.Fatalf("unexpected event error: %v", e)
			}
		}
		for _, sc := range sessionChan {
			close(sc)
		}
		wg.Wait()
	})
}

func TestMonitorCancel(t *testing.T) {
	defer leaktest.AfterTest(t)()
	c := &SyncedCluster{
		Cluster: cloud.Cluster{
			Name: "test-cluster",
		},
		Nodes: []Node{1},
		sessionProvider: func(node Node, cmd string) session {
			return NewMockSession(DefaultMockSessionOptions())
		},
	}

	ctx, cancel := context.WithCancel(context.Background())
	monitorChan := c.Monitor(nilLogger(), ctx, MonitorOpts{})
	cancel()
	e := <-monitorChan
	if e.Event != nil {
		t.Fatalf("unexpected event, channel should be closed: %v", e)
	}
}

func TestMonitorEOF(t *testing.T) {
	defer leaktest.AfterTest(t)()
	s := NewMockSession(DefaultMockSessionOptions())
	c := &SyncedCluster{
		Cluster: cloud.Cluster{
			Name: "test-cluster",
		},
		Nodes: []Node{1},
		sessionProvider: func(node Node, cmd string) session {
			return s
		},
	}

	monitorChan := c.Monitor(nilLogger(), context.Background(), MonitorOpts{})
	s.Close()
	e := <-monitorChan
	require.IsType(t, MonitorError{}, e.Event)
	if e.Event.(MonitorError).Err != io.EOF {
		t.Fatalf("unexpected event error: %v", e)
	}
	e = <-monitorChan
	if e.Event != nil {
		t.Fatalf("unexpected event, channel should be closed: %v", e)
	}
}

func TestMonitorOneShot(t *testing.T) {
	defer leaktest.AfterTest(t)()
	s := NewMockSession(DefaultMockSessionOptions())
	c := &SyncedCluster{
		Cluster: cloud.Cluster{
			Name: "test-cluster",
		},
		Nodes: []Node{1},
		sessionProvider: func(node Node, cmd string) session {
			return s
		},
	}

	monitorChan := c.Monitor(nilLogger(), context.Background(), MonitorOpts{OneShot: true})
	go func() {
		_, _ = s.Stdout.Write([]byte("system=100\nstatus=unknown\n\n"))
		s.Close()
	}()
	e := <-monitorChan
	require.IsType(t, MonitorProcessRunning{}, e.Event)
	// We expect EOF errors to be suppressed for one shot mode, and the channel to
	// be closed after the first event.
	e = <-monitorChan
	if e.Event != nil {
		t.Fatalf("unexpected event, channel should be closed: %v", e)
	}
}
