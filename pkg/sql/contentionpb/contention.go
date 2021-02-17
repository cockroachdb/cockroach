// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package contentionpb

import (
	"bytes"
	"fmt"
	"strings"
)

const singleIndentation = "  "
const doubleIndentation = singleIndentation + singleIndentation
const tripleIndentation = doubleIndentation + singleIndentation

func (ice IndexContentionEvents) String() string {
	var b strings.Builder
	b.WriteString(fmt.Sprintf("tableID=%d indexID=%d\n", ice.TableID, ice.IndexID))
	b.WriteString(fmt.Sprintf("%snum contention events: %d\n", singleIndentation, ice.NumContentionEvents))
	b.WriteString(fmt.Sprintf("%scumulative contention time: %s\n", singleIndentation, ice.CumulativeContentionTime))
	b.WriteString(fmt.Sprintf("%skeys:\n", singleIndentation))
	for i := range ice.Events {
		b.WriteString(ice.Events[i].String())
	}
	return b.String()
}

func (skc SingleKeyContention) String() string {
	var b strings.Builder
	b.WriteString(fmt.Sprintf("%s%s contending txns:\n", doubleIndentation, skc.Key))
	for i := range skc.Txns {
		b.WriteString(skc.Txns[i].String())
	}
	return b.String()
}

func (stx SingleKeyContention_SingleTxnContention) String() string {
	return fmt.Sprintf("%sid=%s count=%d\n", tripleIndentation, stx.TxnID, stx.Count)
}

// Merge merges two lists of contention events that occurred on the same index
// updating the receiver in-place. It will panic if the indexes are different.
func (ice *IndexContentionEvents) Merge(other IndexContentionEvents) {
	if ice.TableID != other.TableID || ice.IndexID != other.IndexID {
		panic(fmt.Sprintf("attempting to merge contention events from different indexes\n%s%s", ice.String(), other.String()))
	}
	ice.NumContentionEvents += other.NumContentionEvents
	ice.CumulativeContentionTime += other.CumulativeContentionTime
	for o := range other.Events {
		found := false
		for i := range ice.Events {
			if ice.Events[i].Key.Compare(other.Events[o].Key) == 0 {
				ice.Events[i].Txns = merge(ice.Events[i].Txns, other.Events[o].Txns)
				found = true
				break
			}
		}
		if !found {
			ice.Events = append(ice.Events, other.Events[o])
		}
	}
}

// merge merges two lists of contention events that occurred on the same key
// updating first in-place.
func merge(
	first, second []SingleKeyContention_SingleTxnContention,
) []SingleKeyContention_SingleTxnContention {
	for s := range second {
		found := false
		for f := range first {
			if bytes.Equal(first[f].TxnID.GetBytes(), second[s].TxnID.GetBytes()) {
				first[f].Count += second[s].Count
				found = true
				break
			}
		}
		if !found {
			first = append(first, second[s])
		}
	}
	return first
}
