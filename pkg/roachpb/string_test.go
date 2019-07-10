// Copyright 2016 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package roachpb_test

import (
	"fmt"
	"testing"

	// Hook up the pretty printer.
	_ "github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage/engine/enginepb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
)

func TestTransactionString(t *testing.T) {
	txnID, err := uuid.FromBytes([]byte("ת\x0f^\xe4-Fؽ\xf7\x16\xe4\xf9\xbe^\xbe"))
	if err != nil {
		t.Fatal(err)
	}
	txn := roachpb.Transaction{
		TxnMeta: enginepb.TxnMeta{
			Key:          roachpb.Key("foo"),
			ID:           txnID,
			Epoch:        2,
			Timestamp:    hlc.Timestamp{WallTime: 20, Logical: 21},
			MinTimestamp: hlc.Timestamp{WallTime: 10, Logical: 11},
			Priority:     957356782,
			Sequence:     15,
		},
		Name:          "name",
		Status:        roachpb.COMMITTED,
		LastHeartbeat: hlc.Timestamp{WallTime: 10, Logical: 11},
		OrigTimestamp: hlc.Timestamp{WallTime: 30, Logical: 31},
		MaxTimestamp:  hlc.Timestamp{WallTime: 40, Logical: 41},
	}
	expStr := `"name" id=d7aa0f5e key="foo" rw=true pri=44.58039917 stat=COMMITTED ` +
		`epo=2 ts=0.000000020,21 orig=0.000000030,31 min=0.000000010,11 max=0.000000040,41 wto=false seq=15`

	if str := txn.String(); str != expStr {
		t.Errorf("expected txn %s; got %s", expStr, str)
	}
}

func TestBatchRequestString(t *testing.T) {
	br := roachpb.BatchRequest{}
	txn := roachpb.MakeTransaction(
		"test",
		nil, /* baseKey */
		roachpb.NormalUserPriority,
		hlc.Timestamp{}, // now
		0,               // maxOffsetNs
	)
	br.Txn = &txn
	for i := 0; i < 100; i++ {
		var ru roachpb.RequestUnion
		ru.MustSetInner(&roachpb.GetRequest{})
		br.Requests = append(br.Requests, ru)
	}
	var ru roachpb.RequestUnion
	ru.MustSetInner(&roachpb.EndTransactionRequest{})
	br.Requests = append(br.Requests, ru)

	e := fmt.Sprintf(`[txn: %s], Get [/Min,/Min), Get [/Min,/Min), Get [/Min,/Min), Get [/Min,/Min), Get [/Min,/Min), Get [/Min,/Min), Get [/Min,/Min), Get [/Min,/Min), Get [/Min,/Min), Get [/Min,/Min), Get [/Min,/Min), Get [/Min,/Min), Get [/Min,/Min), Get [/Min,/Min), Get [/Min,/Min), Get [/Min,/Min), Get [/Min,/Min), Get [/Min,/Min), Get [/Min,/Min), Get [/Min,/Min), ... 76 skipped ..., Get [/Min,/Min), Get [/Min,/Min), Get [/Min,/Min), Get [/Min,/Min), EndTransaction(commit:false) [/Min]`,
		br.Txn.Short())
	if e != br.String() {
		t.Fatalf("e = %s\nv = %s", e, br.String())
	}
}
