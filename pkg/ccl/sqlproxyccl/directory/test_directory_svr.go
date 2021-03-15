// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package directory

import (
	"container/list"
	context "context"
	"fmt"
	"net"
	"os"
	"os/exec"
	"strconv"
	"sync"
	"time"

	"github.com/cockroachdb/cockroach/pkg/util/log"

	"google.golang.org/grpc"
)

// TestDirectoryOptions holds parameters for server part of
// the directory test.
type TestDirectoryOptions struct {
	Port int
}

type TenantProcess struct {
	cmd *exec.Cmd
	sql net.Addr
}

// Test directory server implementation.
type TestDirectoryServer struct {
	sync.RWMutex
	processByAddrByTenantID map[uint64]map[net.Addr]TenantProcess
	eventListeners          list.List
}

func (s *TestDirectoryServer) List(ctx context.Context, req *TenantReq) (*ListResp, error) {
	s.Lock()
	defer s.Unlock()
	return s.listUnlocked(ctx, req)
}

func (s *TestDirectoryServer) WaitEvent(req *EventReq, server Directory_WaitEventServer) error {
	c := make(chan *EventResp)
	s.Lock()
	elem := s.eventListeners.PushBack(c)
	s.Unlock()
	for {
		select {
		case e := <-c:
			if err := server.Send(e); err != nil {
				s.Lock()
				s.eventListeners.Remove(elem)
				s.Unlock()
				break
			}
		}
	}
}

func (s *TestDirectoryServer) notifyEventListenersUnlocked(req *EventResp) {
	for e := s.eventListeners.Front(); e != nil; e = e.Next() {
		e.Value.(chan *EventResp) <- req
	}
}

func (s *TestDirectoryServer) SetPodsCount(ctx context.Context, req *SetPodsCountReq) (*SetPodsCountResp, error) {
	s.Lock()
	defer s.Unlock()

	for {
		list, err := s.listUnlocked(ctx, &TenantReq{req.TenantId})
		if err != nil {
			return nil, err
		}
		if len(list.Pods) == int(req.Count) {
			return &SetPodsCountResp{}, nil
		} else if len(list.Pods) < int(req.Count) {
			if err := s.startTenantUnlocked(ctx, req.TenantId); err != nil {
				return nil, err
			}
		} else {
			// Shutdown some pods here
			return &SetPodsCountResp{}, nil
		}
	}

	return &SetPodsCountResp{}, nil
}

// RunTestDirectorySvr runs a simple test directory services server.
func RunTestDirectory(options TestDirectoryOptions) error {
	port := strconv.Itoa(options.Port)
	lis, err := net.Listen("tcp", ":"+port)
	if err != nil {
		return err
	}
	s := grpc.NewServer()
	dir := &TestDirectoryServer{
		processByAddrByTenantID: make(map[uint64]map[net.Addr]TenantProcess),
	}
	RegisterDirectoryServer(s, dir)
	log.Infof(context.Background(), "server starting on %s", port)
	return s.Serve(lis)
}

func (s *TestDirectoryServer) listUnlocked(ctx context.Context, req *TenantReq) (*ListResp, error) {
	processByAddr, ok := s.processByAddrByTenantID[req.TenantId]
	if !ok {
		return &ListResp{}, nil
	}
	resp := ListResp{}
	for addr, _ := range processByAddr {
		resp.Pods = append(resp.Pods, &Pod{Ip: addr.String()})
	}
	return &resp, nil
}

func (s *TestDirectoryServer) startTenantUnlocked(ctx context.Context, tenantID uint64) error {
	// A hackish way to have the sql tenant process listen on known ports.
	sql, err := net.Listen("tcp", "")
	if err != nil {
		return err
	}
	http, err := net.Listen("tcp", "")
	if err != nil {
		return err
	}
	process := TenantProcess{sql: sql.Addr()}
	args := []string{
		"mt", "start-sql", "--kv-addrs=127.0.0.1:26257", "--idle-exit-after=30s",
		fmt.Sprintf("--sql-addr=%s", sql.Addr().String()),
		fmt.Sprintf("--http-addr=%s", http.Addr().String()),
		fmt.Sprintf("--tenant-id=%d", tenantID),
	}
	if err = sql.Close(); err != nil {
		return err
	}
	if err = http.Close(); err != nil {
		return err
	}

	process.cmd = exec.Command("cockroach", args...)
	process.cmd.Env = append(os.Environ(), "COCKROACH_TRUST_CLIENT_PROVIDED_SQL_REMOTE_ADDR=true")

	processByAddr, ok := s.processByAddrByTenantID[tenantID]
	if !ok {
		processByAddr = map[net.Addr]TenantProcess{}
		s.processByAddrByTenantID[tenantID] = processByAddr
	}
	processByAddr[process.sql] = process

	s.notifyEventListenersUnlocked(&EventResp{
		Typ:      ADDED,
		Ip:       sql.Addr().String(),
		TenantId: tenantID,
	})

	go func() {
		stdoutStderr, err := process.cmd.CombinedOutput()
		if err != nil {
			log.Infof(ctx, "finished %s with err %s", process.cmd.Args, err)
			log.Infof(ctx, "output %s", stdoutStderr)
		} else {
			log.Infof(ctx, "finished %s with success", process.cmd.Args)
		}

		s.Lock()
		defer s.Unlock()
		delete(processByAddr, process.sql)
		s.notifyEventListenersUnlocked(&EventResp{
			Typ:      DELETED,
			Ip:       process.sql.String(),
			TenantId: tenantID,
		})
	}()

	// Need to wait here for the spawned cockroach tenant process to get ready.
	time.Sleep(500 * time.Millisecond)
	return nil
}
