// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package cloud

import (
	"context"
	"io/ioutil"
	"math/rand"
	"net"
	"net/http"
	"os"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
	"github.com/cockroachdb/cockroach/pkg/util/sysutil"
	"github.com/stretchr/testify/require"
)

var econnrefused = &net.OpError{Err: &os.SyscallError{
	Syscall: "test",
	Err:     sysutil.ECONNREFUSED,
}}

var econnreset = &net.OpError{Err: &os.SyscallError{
	Syscall: "test",
	Err:     sysutil.ECONNRESET,
}}

type antagonisticDialer struct {
	net.Dialer
	rnd               *rand.Rand
	numRepeatFailures *int
}

type antagonisticConn struct {
	net.Conn
	rnd               *rand.Rand
	numRepeatFailures *int
}

func (d *antagonisticDialer) DialContext(
	ctx context.Context, network, addr string,
) (net.Conn, error) {
	if network == "tcp" && addr == "storage.googleapis.com:443" {
		if *d.numRepeatFailures < maxNoProgressReads && d.rnd.Int()%2 == 0 {
			*(d.numRepeatFailures)++
			return nil, econnrefused
		}
		c, err := d.Dialer.DialContext(ctx, network, addr)
		if err != nil {
			return nil, err
		}

		return &antagonisticConn{Conn: c, rnd: d.rnd, numRepeatFailures: d.numRepeatFailures}, nil
	}
	return d.Dialer.DialContext(ctx, network, addr)
}

func (c *antagonisticConn) Read(b []byte) (int, error) {
	if *c.numRepeatFailures < maxNoProgressReads && c.rnd.Int()%2 == 0 {
		*(c.numRepeatFailures)++
		return 0, econnreset
	}
	return c.Conn.Read(b)
}

func TestAntagonisticRead(t *testing.T) {
	if os.Getenv("GOOGLE_APPLICATION_CREDENTIALS") == "" {
		// This test requires valid GS credential file.
		return
	}

	rnd, _ := randutil.NewPseudoRand()
	failures := 0
	// Override DialContext implementation in http transport.
	dialer := &antagonisticDialer{rnd: rnd, numRepeatFailures: &failures}

	// Override transport to return antagonistic connection.
	transport := http.DefaultTransport.(*http.Transport)
	transport.DialContext =
		func(ctx context.Context, network, addr string) (net.Conn, error) {
			return dialer.DialContext(ctx, network, addr)
		}
	transport.DisableKeepAlives = true

	defer func() {
		transport.DialContext = nil
		transport.DisableKeepAlives = false
	}()

	gsFile := "gs://cockroach-fixtures/tpch-csv/sf-1/region.tbl?AUTH=implicit"
	conf, err := ExternalStorageConfFromURI(gsFile)
	require.NoError(t, err)

	s, err := MakeExternalStorage(
		context.TODO(), conf, base.ExternalIOConfig{}, testSettings, nil)
	require.NoError(t, err)
	stream, err := s.ReadFile(context.TODO(), "")
	require.NoError(t, err)
	defer stream.Close()
	_, err = ioutil.ReadAll(stream)
	require.NoError(t, err)
}
