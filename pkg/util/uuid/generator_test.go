// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

// Copyright (C) 2013-2018 by Maxim Bublis <b@codemonkey.ru>
// Use of this source code is governed by a MIT-style
// license that can be found in licenses/MIT-gofrs.txt.

// This code originated in github.com/gofrs/uuid.

package uuid

import (
	"bytes"
	"crypto/rand"
	"fmt"
	"net"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
)

func TestGenerator(t *testing.T) {
	t.Run("NewV1", testNewV1)
	t.Run("NewV3", testNewV3)
	t.Run("NewV4", testNewV4)
	t.Run("NewV5", testNewV5)
}

func testNewV1(t *testing.T) {
	t.Run("Basic", testNewV1Basic)
	t.Run("DifferentAcrossCalls", testNewV1DifferentAcrossCalls)
	t.Run("StaleEpoch", testNewV1StaleEpoch)
	t.Run("FaultyRand", testNewV1FaultyRand)
	t.Run("MissingNetwork", testNewV1MissingNetwork)
	t.Run("MissingNetworkFaultyRand", testNewV1MissingNetworkFaultyRand)
}

func TestNewGenWithHWAF(t *testing.T) {
	addr := []byte{0, 1, 2, 3, 4, 42}

	fn := func() (net.HardwareAddr, error) {
		return addr, nil
	}

	var g *Gen
	var err error
	var uuid UUID

	g = NewGenWithHWAF(fn)

	if g == nil {
		t.Fatal("g is unexpectedly nil")
	}

	uuid, err = g.NewV1()
	if err != nil {
		t.Fatalf("g.NewV1() err = %v, want <nil>", err)
	}

	node := uuid[10:]

	if !bytes.Equal(addr, node) {
		t.Fatalf("node = %v, want %v", node, addr)
	}
}

func testNewV1Basic(t *testing.T) {
	u, err := NewV1()
	if err != nil {
		t.Fatal(err)
	}
	if got, want := u.Version(), V1; got != want {
		t.Errorf("generated UUID with version %d, want %d", got, want)
	}
	if got, want := u.Variant(), VariantRFC4122; got != want {
		t.Errorf("generated UUID with variant %d, want %d", got, want)
	}
}

func testNewV1DifferentAcrossCalls(t *testing.T) {
	u1, err := NewV1()
	if err != nil {
		t.Fatal(err)
	}
	u2, err := NewV1()
	if err != nil {
		t.Fatal(err)
	}
	if u1 == u2 {
		t.Errorf("generated identical UUIDs across calls: %v", u1)
	}
}

func testNewV1StaleEpoch(t *testing.T) {
	g := &Gen{
		epochFunc: func() time.Time {
			return timeutil.Unix(0, 0)
		},
		hwAddrFunc: defaultHWAddrFunc,
		rand:       rand.Reader,
	}
	u1, err := g.NewV1()
	if err != nil {
		t.Fatal(err)
	}
	u2, err := g.NewV1()
	if err != nil {
		t.Fatal(err)
	}
	if u1 == u2 {
		t.Errorf("generated identical UUIDs across calls: %v", u1)
	}
}

func testNewV1FaultyRand(t *testing.T) {
	g := &Gen{
		epochFunc:  time.Now,
		hwAddrFunc: defaultHWAddrFunc,
		rand: &faultyReader{
			readToFail: 0, // fail immediately
		},
	}
	u, err := g.NewV1()
	if err == nil {
		t.Fatalf("got %v, want error", u)
	}
	if u != Nil {
		t.Fatalf("got %v on error, want Nil", u)
	}
}

func testNewV1MissingNetwork(t *testing.T) {
	g := &Gen{
		epochFunc: time.Now,
		hwAddrFunc: func() (net.HardwareAddr, error) {
			return []byte{}, fmt.Errorf("uuid: no hw address found")
		},
		rand: rand.Reader,
	}
	_, err := g.NewV1()
	if err != nil {
		t.Errorf("did not handle missing network interfaces: %v", err)
	}
}

func testNewV1MissingNetworkFaultyRand(t *testing.T) {
	g := &Gen{
		epochFunc: time.Now,
		hwAddrFunc: func() (net.HardwareAddr, error) {
			return []byte{}, fmt.Errorf("uuid: no hw address found")
		},
		rand: &faultyReader{
			readToFail: 1,
		},
	}
	u, err := g.NewV1()
	if err == nil {
		t.Errorf("did not error on faulty reader and missing network, got %v", u)
	}
}

func testNewV3(t *testing.T) {
	t.Run("Basic", testNewV3Basic)
	t.Run("EqualNames", testNewV3EqualNames)
	t.Run("DifferentNamespaces", testNewV3DifferentNamespaces)
}

func testNewV3Basic(t *testing.T) {
	ns := NamespaceDNS
	name := "www.example.com"
	u := NewV3(ns, name)
	if got, want := u.Version(), V3; got != want {
		t.Errorf("NewV3(%v, %q): got version %d, want %d", ns, name, got, want)
	}
	if got, want := u.Variant(), VariantRFC4122; got != want {
		t.Errorf("NewV3(%v, %q): got variant %d, want %d", ns, name, got, want)
	}
	want := "5df41881-3aed-3515-88a7-2f4a814cf09e"
	if got := u.String(); got != want {
		t.Errorf("NewV3(%v, %q) = %q, want %q", ns, name, got, want)
	}
}

func testNewV3EqualNames(t *testing.T) {
	for _, ns := range []UUID{
		NamespaceDNS,
		NamespaceOID,
		NamespaceURL,
		NamespaceX500,
	} {
		name := "example.com"
		u1 := NewV3(ns, name)
		u2 := NewV3(ns, name)
		if u1 != u2 {
			t.Errorf("NewV3(%v, %q) generated %v and %v across two calls", ns, name, u1, u2)
		}
	}
}

func testNewV3DifferentNamespaces(t *testing.T) {
	name := "example.com"
	ns1 := NamespaceDNS
	ns2 := NamespaceURL
	u1 := NewV3(ns1, name)
	u2 := NewV3(ns2, name)
	if u1 == u2 {
		t.Errorf("NewV3(%v, %q) == NewV3(%d, %q) (%v)", ns1, name, ns2, name, u1)
	}
}

func testNewV4(t *testing.T) {
	t.Run("Basic", testNewV4Basic)
	t.Run("DifferentAcrossCalls", testNewV4DifferentAcrossCalls)
	t.Run("FaultyRand", testNewV4FaultyRand)
	t.Run("ShortRandomRead", testNewV4ShortRandomRead)
}

func testNewV4Basic(t *testing.T) {
	u, err := NewV4()
	if err != nil {
		t.Fatal(err)
	}
	if got, want := u.Version(), V4; got != want {
		t.Errorf("got version %d, want %d", got, want)
	}
	if got, want := u.Variant(), VariantRFC4122; got != want {
		t.Errorf("got variant %d, want %d", got, want)
	}
}

func testNewV4DifferentAcrossCalls(t *testing.T) {
	u1, err := NewV4()
	if err != nil {
		t.Fatal(err)
	}
	u2, err := NewV4()
	if err != nil {
		t.Fatal(err)
	}
	if u1 == u2 {
		t.Errorf("generated identical UUIDs across calls: %v", u1)
	}
}

func testNewV4FaultyRand(t *testing.T) {
	g := &Gen{
		epochFunc:  time.Now,
		hwAddrFunc: defaultHWAddrFunc,
		rand: &faultyReader{
			readToFail: 0, // fail immediately
		},
	}
	u, err := g.NewV4()
	if err == nil {
		t.Errorf("got %v, nil error", u)
	}
}

func testNewV4ShortRandomRead(t *testing.T) {
	g := &Gen{
		epochFunc: time.Now,
		hwAddrFunc: func() (net.HardwareAddr, error) {
			return []byte{}, fmt.Errorf("uuid: no hw address found")
		},
		rand: bytes.NewReader([]byte{42}),
	}
	u, err := g.NewV4()
	if err == nil {
		t.Errorf("got %v, nil error", u)
	}
}

func testNewV5(t *testing.T) {
	t.Run("Basic", testNewV5Basic)
	t.Run("EqualNames", testNewV5EqualNames)
	t.Run("DifferentNamespaces", testNewV5DifferentNamespaces)
}

func testNewV5Basic(t *testing.T) {
	ns := NamespaceDNS
	name := "www.example.com"
	u := NewV5(ns, name)
	if got, want := u.Version(), V5; got != want {
		t.Errorf("NewV5(%v, %q): got version %d, want %d", ns, name, got, want)
	}
	if got, want := u.Variant(), VariantRFC4122; got != want {
		t.Errorf("NewV5(%v, %q): got variant %d, want %d", ns, name, got, want)
	}
	want := "2ed6657d-e927-568b-95e1-2665a8aea6a2"
	if got := u.String(); got != want {
		t.Errorf("NewV5(%v, %q) = %q, want %q", ns, name, got, want)
	}
}

func testNewV5EqualNames(t *testing.T) {
	ns := NamespaceDNS
	name := "example.com"
	u1 := NewV5(ns, name)
	u2 := NewV5(ns, name)
	if u1 != u2 {
		t.Errorf("NewV5(%v, %q) generated %v and %v across two calls", ns, name, u1, u2)
	}
}

func testNewV5DifferentNamespaces(t *testing.T) {
	name := "example.com"
	ns1 := NamespaceDNS
	ns2 := NamespaceURL
	u1 := NewV5(ns1, name)
	u2 := NewV5(ns2, name)
	if u1 == u2 {
		t.Errorf("NewV5(%v, %q) == NewV5(%v, %q) (%v)", ns1, name, ns2, name, u1)
	}
}

func BenchmarkGenerator(b *testing.B) {
	b.Run("V1", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			Must(NewV1())
		}
	})
	b.Run("V3", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			NewV3(NamespaceDNS, "www.example.com")
		}
	})
	b.Run("V4", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			Must(NewV4())
		}
	})
	b.Run("FastV4", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			FastMakeV4()
		}
	})
	b.Run("V5", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			NewV5(NamespaceDNS, "www.example.com")
		}
	})
}

type faultyReader struct {
	callsNum   int
	readToFail int // Read call number to fail
}

func (r *faultyReader) Read(dest []byte) (int, error) {
	r.callsNum++
	if (r.callsNum - 1) == r.readToFail {
		return 0, fmt.Errorf("io: reader is faulty")
	}
	return rand.Read(dest)
}
