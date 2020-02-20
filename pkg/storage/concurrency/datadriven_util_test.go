// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package concurrency_test

import (
	"strconv"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/uint128"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/cockroachdb/datadriven"
)

func nextUUID(counter *uint128.Uint128) uuid.UUID {
	*counter = counter.Add(1)
	return uuid.FromUint128(*counter)
}

func scanTimestamp(t *testing.T, d *datadriven.TestData) hlc.Timestamp {
	var ts hlc.Timestamp
	var tsS string
	d.ScanArgs(t, "ts", &tsS)
	parts := strings.Split(tsS, ",")

	// Find the wall time part.
	tsW, err := strconv.ParseInt(parts[0], 10, 64)
	if err != nil {
		d.Fatalf(t, "%v", err)
	}
	ts.WallTime = tsW

	// Find the logical part, if there is one.
	var tsL int64
	if len(parts) > 1 {
		tsL, err = strconv.ParseInt(parts[1], 10, 32)
		if err != nil {
			d.Fatalf(t, "%v", err)
		}
	}
	ts.Logical = int32(tsL)
	return ts
}

func scanSingleRequest(t *testing.T, d *datadriven.TestData, line string) roachpb.Request {
	cmd, cmdArgs, err := datadriven.ParseLine(line)
	if err != nil {
		d.Fatalf(t, "error parsing single request: %v", err)
		return nil
	}

	fields := make(map[string]string, len(cmdArgs))
	for _, cmdArg := range cmdArgs {
		if len(cmdArg.Vals) != 1 {
			d.Fatalf(t, "unexpected command values: %+v", cmdArg)
			return nil
		}
		fields[cmdArg.Key] = cmdArg.Vals[0]
	}
	mustGetField := func(f string) string {
		v, ok := fields[f]
		if !ok {
			d.Fatalf(t, "missing required field: %s", f)
		}
		return v
	}

	switch cmd {
	case "get":
		var r roachpb.GetRequest
		r.Key = roachpb.Key(mustGetField("key"))
		return &r

	case "scan":
		var r roachpb.ScanRequest
		r.Key = roachpb.Key(mustGetField("key"))
		if v, ok := fields["endkey"]; ok {
			r.EndKey = roachpb.Key(v)
		}
		return &r

	case "put":
		var r roachpb.PutRequest
		r.Key = roachpb.Key(mustGetField("key"))
		r.Value.SetString(mustGetField("value"))
		return &r

	case "request-lease":
		var r roachpb.RequestLeaseRequest
		return &r

	default:
		d.Fatalf(t, "unknown request type: %s", cmd)
		return nil
	}
}
