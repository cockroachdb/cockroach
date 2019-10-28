// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package engine

import (
	"bytes"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/storage/engine/enginepb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/datadriven"
	"github.com/cockroachdb/pebble"
)

func TestPebbleTimeBoundPropCollector(t *testing.T) {
	defer leaktest.AfterTest(t)()

	datadriven.RunTest(t, "testdata/time_bound_props", func(d *datadriven.TestData) string {
		c := &pebbleTimeBoundPropCollector{}
		switch d.Cmd {
		case "build":
			for _, line := range strings.Split(d.Input, "\n") {
				parts := strings.Fields(line)
				if len(parts) != 2 {
					return fmt.Sprintf("malformed line: %s, expected: <key>/<timestamp> <value>", line)
				}
				keyParts := strings.Split(parts[0], "/")
				if len(keyParts) != 2 {
					return fmt.Sprintf("malformed key: %s, expected: <key>/<timestamp>", parts[0])
				}

				key := []byte(keyParts[0])
				timestamp, err := strconv.Atoi(keyParts[1])
				if err != nil {
					return err.Error()
				}
				ikey := pebble.InternalKey{
					UserKey: EncodeKey(MVCCKey{
						Key:       key,
						Timestamp: hlc.Timestamp{WallTime: int64(timestamp)},
					}),
				}

				value := []byte(parts[1])
				if timestamp == 0 {
					if n, err := fmt.Sscanf(string(value), "timestamp=%d", &timestamp); err != nil {
						return err.Error()
					} else if n != 1 {
						return fmt.Sprintf("malformed txn timestamp: %s, expected timestamp=<value>", value)
					}
					meta := &enginepb.MVCCMetadata{}
					meta.Timestamp.WallTime = int64(timestamp)
					meta.Txn = &enginepb.TxnMeta{}
					var err error
					value, err = protoutil.Marshal(meta)
					if err != nil {
						return err.Error()
					}
				}

				if err := c.Add(ikey, value); err != nil {
					return err.Error()
				}
			}

			// Retrieve the properties and sort them for test determinism.
			m := make(map[string]string)
			if err := c.Finish(m); err != nil {
				return err.Error()
			}
			var keys []string
			for k := range m {
				keys = append(keys, k)
			}
			sort.Strings(keys)

			var buf bytes.Buffer
			for _, k := range keys {
				fmt.Fprintf(&buf, "%s: %x\n", k, m[k])
			}
			return buf.String()

		default:
			return fmt.Sprintf("unknown command: %s", d.Cmd)
		}
	})
}
