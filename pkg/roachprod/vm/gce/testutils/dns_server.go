// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package testutils

import (
	"encoding/json"
	"fmt"
	"math/rand"
	"os/exec"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/roachprod/vm"
	"github.com/cockroachdb/cockroach/pkg/roachprod/vm/gce"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/errors"
)

type testProvider struct {
	vm.Provider
	vm.DNSProvider
}

type testDNSRecord struct {
	Name       string   `json:"name"`
	Kind       string   `json:"kind"`
	RecordType string   `json:"type"`
	TTL        int      `json:"ttl"`
	RRDatas    []string `json:"rrdatas"`
}

type Metrics struct {
	ListCalls, CreateCalls, UpdateCalls, DeleteCalls int
}

func createTestJSONRecord(records []testDNSRecord) ([]byte, error) {
	return json.Marshal(records)
}

// TestDNSServer is a DNS "server" that can be used for testing purposes. It
// emulates the `gcloud dns` command responses, by storing the records in memory
// and providing the same output as the `gcloud dns` command. It will fail in
// the same way as the `gcloud dns` command if a record already exists or does
// not exist, depending on the command.
type TestDNSServer interface {
	Metrics() Metrics
	Count() int
}

type testDNSServer struct {
	mu      syncutil.Mutex
	records map[string]vm.DNSRecord
	metrics Metrics
}

func (t *testDNSServer) storedName(name string) string {
	// Google DNS always returns the name with a trailing ".",
	// so we emulate that here.
	if !strings.HasSuffix(name, ".") {
		name += "."
	}
	return name
}

func (t *testDNSServer) list(filter string) ([]byte, error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.metrics.ListCalls++
	records := make([]testDNSRecord, 0)
	for name, r := range t.records {
		if filter == "" || strings.Contains(name, filter) {
			records = append(records, testDNSRecord{
				Name:       r.Name,
				Kind:       "dns#resourceRecordSet",
				RecordType: string(r.Type),
				TTL:        vm.DNSRecordTTL,
				RRDatas:    strings.Split(r.Data, ","),
			})
		}
	}
	return createTestJSONRecord(records)
}

func (t *testDNSServer) create(name string, data string) ([]byte, error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.metrics.CreateCalls++
	if _, ok := t.records[t.storedName(name)]; ok {
		return nil, errors.Newf("can't create record %s already exists", name)
	}
	// Keep the data grouped to avoid having to join it later.
	t.records[t.storedName(name)] = vm.DNSRecord{
		Type: vm.SRV,
		Name: t.storedName(name),
		Data: data,
	}
	return []byte{}, nil
}

func (t *testDNSServer) update(name string, data string) ([]byte, error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.metrics.UpdateCalls++
	if _, ok := t.records[t.storedName(name)]; !ok {
		return nil, errors.Newf("can't update record %s it does not exist", name)
	}
	t.records[t.storedName(name)] = vm.DNSRecord{
		Type: vm.SRV,
		Name: t.storedName(name),
		Data: data,
	}
	return []byte{}, nil
}

func (t *testDNSServer) delete(name string) ([]byte, error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.metrics.DeleteCalls++
	if _, ok := t.records[t.storedName(name)]; !ok {
		return nil, errors.Newf("can't delete record it does not exist", name)
	}
	delete(t.records, t.storedName(name))
	return []byte{}, nil
}

func (t *testDNSServer) Count() int {
	t.mu.Lock()
	defer t.mu.Unlock()
	return len(t.records)
}

func (t *testDNSServer) Metrics() Metrics {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.metrics
}

func (t *testDNSServer) execFunc(cmd *exec.Cmd) ([]byte, error) {
	getArg := func(args []string, arg string) string {
		for i, a := range args {
			if a == arg {
				return args[i+1]
			}
		}
		return ""
	}
	for _, arg := range cmd.Args {
		switch arg {
		case "list":
			return t.list(getArg(cmd.Args, "--filter"))
		case "create":
			return t.create(getArg(cmd.Args, "create"), getArg(cmd.Args, "--rrdatas"))
		case "update":
			return t.update(getArg(cmd.Args, "update"), getArg(cmd.Args, "--rrdatas"))
		case "delete":
			return t.delete(getArg(cmd.Args, "delete"))
		}
	}
	return nil, errors.New("unknown command")
}

// ProviderWithTestDNSServer initializes a test DNS server and a DNS capable
// provider pointing to the test server. It returns the test DNS server, the
// test DNS provider, and the provider name.
func ProviderWithTestDNSServer(rng *rand.Rand) (TestDNSServer, vm.DNSProvider, string) {
	testServer := &testDNSServer{records: make(map[string]vm.DNSRecord)}
	testDNS := gce.NewDNSProviderWithExec(testServer.execFunc)
	// Since this is a global variable, we need to make sure the provider name is
	// unique, in order to avoid conflicts with other tests.
	providerName := fmt.Sprintf("testProvider-%d", rng.Uint32())
	vm.Providers[providerName] = &testProvider{DNSProvider: testDNS}
	return testServer, testDNS, providerName
}
