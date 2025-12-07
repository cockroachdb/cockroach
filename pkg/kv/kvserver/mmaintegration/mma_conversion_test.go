// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package mmaintegration

import (
	"fmt"
	"strconv"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/allocator"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/allocator/mmaprototype"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/testutils/datapathutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/datadriven"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

func TestConvertReplicaChangeToMMA(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	var desc roachpb.RangeDescriptor
	var leaseholderStoreID roachpb.StoreID
	rangeUsageInfo := allocator.RangeUsageInfo{
		LogicalBytes:             1000,
		WriteBytesPerSecond:      100,
		RequestCPUNanosPerSecond: 10,
		RaftCPUNanosPerSecond:    1,
	}
	datadriven.RunTest(t, datapathutils.TestDataPath(t, t.Name()),
		func(t *testing.T, d *datadriven.TestData) string {
			switch d.Cmd {
			case "define-range":
				var rangeID int
				d.ScanArgs(t, "id", &rangeID)
				desc.RangeID = roachpb.RangeID(rangeID)
				lines := strings.Split(d.Input, "\n")
				for _, line := range lines {
					var repl roachpb.ReplicaDescriptor
					fields := strings.Fields(line)
					isLeaseholder := false
					for _, field := range fields {
						parts := strings.Split(field, "=")
						require.Equal(t, len(parts), 2)
						switch parts[0] {
						case "store-id":
							repl.StoreID = roachpb.StoreID(parseInt(t, parts[1]))
							repl.NodeID = roachpb.NodeID(repl.StoreID)
						case "replica-id":
							repl.ReplicaID = roachpb.ReplicaID(parseInt(t, parts[1]))
						case "leaseholder":
							isLeaseholder = parseBool(t, parts[1])
						case "type":
							replType, err := parseReplicaType(parts[1])
							require.NoError(t, err)
							repl.Type = replType
						default:
							panic(fmt.Sprintf("unknown argument: %s", parts[0]))
						}
					}
					if isLeaseholder {
						leaseholderStoreID = repl.StoreID
					}
					desc.InternalReplicas = append(desc.InternalReplicas, repl)
				}
				return fmt.Sprintf("desc:%s\nleaseholder=s%v", desc.String(), leaseholderStoreID)

			case "convert-replica-changes":
				var expectPanic bool
				if d.HasArg("expect-panic") {
					expectPanic = true
				}
				var changes kvpb.ReplicationChanges
				lines := strings.Split(d.Input, "\n")
				for _, line := range lines {
					var chg kvpb.ReplicationChange
					fields := strings.Fields(line)
					for _, field := range fields {
						parts := strings.Split(field, "=")
						require.Equal(t, len(parts), 2)
						switch parts[0] {
						case "store-id":
							chg.Target.StoreID = roachpb.StoreID(parseInt(t, parts[1]))
							chg.Target.NodeID = roachpb.NodeID(chg.Target.StoreID)
						case "type":
							changeType, err := parseReplicaChange(parts[1])
							require.NoError(t, err)
							chg.ChangeType = changeType
						default:
							panic(fmt.Sprintf("unknown argument: %s", parts[0]))
						}
					}
					changes = append(changes, chg)
				}
				if expectPanic {
					require.Panics(t,
						func() {
							_, _ = convertReplicaChangeToMMA(&desc, rangeUsageInfo, changes, leaseholderStoreID)
						})
					return "panicked as expected"
				} else {
					mmaChanges, err := convertReplicaChangeToMMA(&desc, rangeUsageInfo, changes, leaseholderStoreID)
					if err != nil {
						return fmt.Sprintf("error: %s", err.Error())
					}
					mmaChanges.SortForTesting()
					var b strings.Builder
					fmt.Fprintf(&b, "PendingRangeChange: %s", mmaChanges.StringForTesting())
					externalChange := mmaprototype.MakeExternalRangeChange(mmaChanges)
					if externalChange.IsChangeReplicas() {
						fmt.Fprintf(&b, "As kvpb.ReplicationChanges:\n %v\n", externalChange.ReplicationChanges())
					} else if externalChange.IsPureTransferLease() {
						fmt.Fprintf(&b, "Lease transfer from s%v to s%v\n",
							externalChange.LeaseTransferFrom(), externalChange.LeaseTransferTarget())
					}
					return b.String()
				}

			default:
				return fmt.Sprintf("unknown command: %s", d.Cmd)
			}
		})

}

func parseInt(t *testing.T, in string) int {
	i, err := strconv.Atoi(strings.TrimSpace(in))
	require.NoError(t, err)
	return i
}

func parseBool(t *testing.T, in string) bool {
	b, err := strconv.ParseBool(strings.TrimSpace(in))
	require.NoError(t, err)
	return b
}

func parseReplicaType(val string) (roachpb.ReplicaType, error) {
	typ, ok := roachpb.ReplicaType_value[val]
	if !ok {
		return 0, errors.AssertionFailedf("unknown replica type %s", val)
	}
	return roachpb.ReplicaType(typ), nil
}

func parseReplicaChange(val string) (roachpb.ReplicaChangeType, error) {
	typ, ok := roachpb.ReplicaChangeType_value[val]
	if !ok {
		return 0, errors.AssertionFailedf("unknown replica change type %s", val)
	}
	return roachpb.ReplicaChangeType(typ), nil
}
