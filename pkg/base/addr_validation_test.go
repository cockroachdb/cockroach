// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package base_test

import (
	"context"
	"fmt"
	"net"
	"os"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
)

type addrs struct{ listen, adv, http, advhttp, sql, advsql string }

func (a *addrs) String() string {
	return fmt.Sprintf(""+
		"--listen-addr=%s --advertise-addr=%s "+
		"--http-addr=%s (http adv: %s) "+
		"--sql-addr=%s (sql adv: %s)",
		a.listen, a.adv, a.http, a.advhttp, a.sql, a.advsql)
}

func TestValidateAddrs(t *testing.T) {
	defer leaktest.AfterTest(t)()

	// Prepare some reference strings that will be checked in the
	// test below.
	hostname, err := os.Hostname()
	if err != nil {
		t.Fatal(err)
	}
	hostAddr, err := base.LookupAddr(context.Background(), net.DefaultResolver, hostname)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(hostAddr, ":") {
		hostAddr = "[" + hostAddr + "]"
	}
	localAddr, err := base.LookupAddr(context.Background(), net.DefaultResolver, "localhost")
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(localAddr, ":") {
		localAddr = "[" + localAddr + "]"
	}

	// We need to know what a port name resolution error look like,
	// because they may be different across systems/libcs.
	_, err = net.DefaultResolver.LookupPort(context.Background(), "tcp", "nonexistent")
	if err == nil {
		t.Fatal("expected port resolution failure, got no error")
	}
	portExpectedErr := err.Error()
	// For the host name resolution error we can reliably expect "no such host"
	// below, but before we test anything we need to ensure we indeed have
	// a reliably non-resolvable host name.
	_, err = net.DefaultResolver.LookupIPAddr(context.Background(), "nonexistent.example.com")
	if err == nil {
		t.Fatal("expected host resolution failure, got no error")
	}

	// The test cases.
	testData := []struct {
		in          addrs
		expectedErr string
		expected    addrs
	}{
		// Common case: no server flags, all defaults.
		{addrs{":26257", "", ":8080", "", ":5432", ""}, "",
			addrs{":26257", hostname + ":26257", ":8080", hostname + ":8080", ":5432", hostname + ":5432"}},

		// Another common case: --listen-addr=<somehost>
		{addrs{hostname + ":26257", "", ":8080", "", ":5432", ""}, "",
			addrs{hostAddr + ":26257", hostname + ":26257", hostAddr + ":8080", hostname + ":8080", hostAddr + ":5432", hostname + ":5432"}},

		// Another common case: --listen-addr=localhost
		{addrs{"localhost:26257", "", ":8080", "", ":5432", ""}, "",
			addrs{localAddr + ":26257", "localhost:26257", localAddr + ":8080", "localhost:8080", localAddr + ":5432", "localhost:5432"}},

		// Correct use: --listen-addr=<someaddr> --advertise-host=<somehost>
		{addrs{hostAddr + ":26257", hostname + ":", ":8080", "", ":5432", ""}, "",
			addrs{hostAddr + ":26257", hostname + ":26257", hostAddr + ":8080", hostname + ":8080", hostAddr + ":5432", hostname + ":5432"}},

		// Explicit port number in advertise addr.
		{addrs{hostAddr + ":26257", hostname + ":12345", ":8080", "", ":5432", ""}, "",
			addrs{hostAddr + ":26257", hostname + ":12345", hostAddr + ":8080", hostname + ":8080", hostAddr + ":5432", hostname + ":5432"}},

		// Use a non-numeric port number.
		{addrs{":postgresql", "", ":http", "", ":postgresql", ""}, "",
			addrs{":5432", hostname + ":5432", ":80", hostname + ":80", ":5432", hostname + ":5432"}},

		// Make HTTP local only.
		{addrs{":26257", "", "localhost:8080", "", ":5432", ""}, "",
			addrs{":26257", hostname + ":26257", localAddr + ":8080", "localhost:8080", ":5432", hostname + ":5432"}},

		// Local server but public HTTP.
		{addrs{"localhost:26257", "", hostname + ":8080", "", ":5432", ""}, "",
			addrs{localAddr + ":26257", "localhost:26257", hostAddr + ":8080", hostname + ":8080", localAddr + ":5432", "localhost:5432"}},

		// Make SQL and tenant local only.
		{addrs{":26257", "", ":8080", "", "localhost:5432", ""}, "",
			addrs{":26257", hostname + ":26257", ":8080", hostname + ":8080", localAddr + ":5432", "localhost:5432"}},

		// Not-unreasonable case: addresses set empty. Means using port 0.
		{addrs{"", "", "", "", "", ""}, "",
			addrs{":0", hostname + ":0", ":0", hostname + ":0", ":0", hostname + ":0"}},
		// A colon means "all-addr, auto-port".
		{addrs{":", "", "", "", "", ""}, "",
			addrs{":0", hostname + ":0", ":0", hostname + ":0", ":0", hostname + ":0"}},
		{addrs{"", ":", "", "", "", ""}, "",
			addrs{":0", hostname + ":0", ":0", hostname + ":0", ":0", hostname + ":0"}},
		{addrs{"", "", ":", "", "", ""}, "",
			addrs{":0", hostname + ":0", ":0", hostname + ":0", ":0", hostname + ":0"}},
		{addrs{"", "", "", "", ":", ""}, "",
			addrs{":0", hostname + ":0", ":0", hostname + ":0", ":0", hostname + ":0"}},
		{addrs{"", "", "", "", "", ":"}, "",
			addrs{":0", hostname + ":0", ":0", hostname + ":0", ":0", hostname + ":0"}},
		{addrs{"", "", "", "", "", ""}, "",
			addrs{":0", hostname + ":0", ":0", hostname + ":0", ":0", hostname + ":0"}},
		{addrs{"", "", "", "", "", ""}, "",
			addrs{":0", hostname + ":0", ":0", hostname + ":0", ":0", hostname + ":0"}},

		// Advertise port 0 means reuse listen port. We don't
		// auto-allocate ports for advertised addresses.
		{addrs{":12345", ":0", "", "", ":5432", ""}, "",
			addrs{":12345", hostname + ":12345", ":0", hostname + ":0", ":5432", hostname + ":5432"}},
		{addrs{":12345", "", "", "", ":5432", ":0"}, "",
			addrs{":12345", hostname + ":12345", ":0", hostname + ":0", ":5432", hostname + ":5432"}},

		// Expected errors.

		// Missing port number.
		{addrs{"localhost", "", "", "", "", ""}, "invalid --listen-addr.*missing port in address", addrs{}},
		{addrs{":26257", "", "localhost", "", "", ""}, "invalid --http-addr.*missing port in address", addrs{}},
		// Invalid port number.
		{addrs{"localhost:-1231", "", "", "", "", ""}, "invalid port", addrs{}},
		{addrs{"localhost:nonexistent", "", "", "", "", ""}, portExpectedErr, addrs{}},
		// Invalid address.
		{addrs{"nonexistent.example.com:26257", "", "", "", "", ""}, "no such host", addrs{}},
		{addrs{"333.333.333.333:26257", "", "", "", "", ""}, "no such host", addrs{}},
	}

	for i, test := range testData {
		t.Run(fmt.Sprintf("%d/%s", i, test.in), func(t *testing.T) {
			cfg := base.Config{
				Addr:              test.in.listen,
				AdvertiseAddr:     test.in.adv,
				HTTPAddr:          test.in.http,
				HTTPAdvertiseAddr: test.in.advhttp,
				SQLAddr:           test.in.sql,
				SQLAdvertiseAddr:  test.in.advsql,
			}

			if err := cfg.ValidateAddrs(context.Background()); err != nil {
				if !testutils.IsError(err, test.expectedErr) {
					t.Fatalf("expected error %q, got %v", test.expectedErr, err)
				}
				return
			}
			if test.expectedErr != "" {
				t.Fatalf("expected error %q, got success", test.expectedErr)
			}

			got := addrs{
				listen:  cfg.Addr,
				adv:     cfg.AdvertiseAddr,
				http:    cfg.HTTPAddr,
				advhttp: cfg.HTTPAdvertiseAddr,
				sql:     cfg.SQLAddr,
				advsql:  cfg.SQLAdvertiseAddr,
			}
			gotStr := got.String()
			expStr := test.expected.String()

			if gotStr != expStr {
				t.Fatalf("expected %q,\ngot %q", expStr, gotStr)
			}
		})
	}
}
