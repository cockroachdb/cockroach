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
	"fmt"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
)

const singleIndentation = "  "
const doubleIndentation = singleIndentation + singleIndentation
const tripleIndentation = doubleIndentation + singleIndentation

const contentionEventsStr = "num contention events:"
const cumulativeContentionTimeStr = "cumulative contention time:"
const contendingTxnsStr = "contending txns:"

func (ice IndexContentionEvents) String() string {
	var b strings.Builder
	b.WriteString(fmt.Sprintf("tableID=%d indexID=%d\n", ice.TableID, ice.IndexID))
	b.WriteString(fmt.Sprintf("%s%s %d\n", singleIndentation, contentionEventsStr, ice.NumContentionEvents))
	b.WriteString(fmt.Sprintf("%s%s %s\n", singleIndentation, cumulativeContentionTimeStr, ice.CumulativeContentionTime))
	b.WriteString(fmt.Sprintf("%skeys:\n", singleIndentation))
	for i := range ice.Events {
		b.WriteString(ice.Events[i].String())
	}
	return b.String()
}

func (skc SingleKeyContention) String() string {
	var b strings.Builder
	b.WriteString(fmt.Sprintf("%s%s %s\n", doubleIndentation, skc.Key, contendingTxnsStr))
	for i := range skc.Txns {
		b.WriteString(skc.Txns[i].String())
	}
	return b.String()
}

func toString(stx SingleTxnContention, indentation string) string {
	return fmt.Sprintf("%sid=%s count=%d\n", indentation, stx.TxnID, stx.Count)
}

func (stx SingleTxnContention) String() string {
	return toString(stx, tripleIndentation)
}

func (skc SingleNonSQLKeyContention) String() string {
	var b strings.Builder
	b.WriteString(fmt.Sprintf("non-SQL key %s %s\n", skc.Key, contendingTxnsStr))
	b.WriteString(fmt.Sprintf("%s%s %d\n", singleIndentation, contentionEventsStr, skc.NumContentionEvents))
	b.WriteString(fmt.Sprintf("%s%s %s\n", singleIndentation, cumulativeContentionTimeStr, skc.CumulativeContentionTime))
	for i := range skc.Txns {
		b.WriteString(toString(skc.Txns[i], doubleIndentation))
	}
	return b.String()
}

// Valid returns if the ResolvedTxnID is valid.
func (r *ResolvedTxnID) Valid() bool {
	return !uuid.Nil.Equal(r.TxnID)
}

// Valid returns if the ExtendedContentionEvent is valid.
func (e *ExtendedContentionEvent) Valid() bool {
	return e.BlockingEvent.TxnMeta.ID != uuid.UUID{}
}

// Hash returns a hash that's unique to ExtendedContentionEvent using
// blocking txn's txnID, waiting txn's txnID and the event collection timestamp.
func (e *ExtendedContentionEvent) Hash() uint64 {
	hash := util.MakeFNV64()
	hashUUID(e.BlockingEvent.TxnMeta.ID, &hash)
	hashUUID(e.WaitingTxnID, &hash)
	hash.Add(uint64(e.CollectionTs.UnixMilli()))
	return hash.Sum()
}

func hashUUID(u uuid.UUID, fnv *util.FNV64) {
	b := u.GetBytes()
	// Hash the blocking txn id.
	b, val, err := encoding.DecodeUint64Descending(b)
	if err != nil {
		panic(err)
	}
	fnv.Add(val)
	_, val, err = encoding.DecodeUint64Descending(b)
	if err != nil {
		panic(err)
	}
	fnv.Add(val)
}
