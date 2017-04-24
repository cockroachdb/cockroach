// Copyright 2016 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.
//
// Author: Tamir Duberstein (tamird@gmail.com)

package roachpb_test

import (
	"strings"
	"testing"

	// Hook up the pretty printer.
	_ "github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"

	"github.com/cockroachdb/cockroach/pkg/storage/engine/enginepb"
	"github.com/cockroachdb/cockroach/pkg/storage/storagebase"
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
			Isolation: enginepb.SERIALIZABLE,
			Key:       roachpb.Key("foo"),
			ID:        &txnID,
			Epoch:     2,
			Timestamp: hlc.Timestamp{WallTime: 20, Logical: 21},
			Priority:  957356782,
			Sequence:  15,
		},
		Name:          "name",
		Status:        roachpb.COMMITTED,
		LastHeartbeat: hlc.Timestamp{WallTime: 10, Logical: 11},
		OrigTimestamp: hlc.Timestamp{WallTime: 30, Logical: 31},
		MaxTimestamp:  hlc.Timestamp{WallTime: 40, Logical: 41},
	}
	expStr := `"name" id=d7aa0f5e key="foo" rw=false pri=44.58039917 iso=SERIALIZABLE stat=COMMITTED ` +
		`epo=2 ts=0.000000020,21 orig=0.000000030,31 max=0.000000040,41 wto=false rop=false seq=15`

	if str := txn.String(); str != expStr {
		t.Errorf("expected txn %s; got %s", expStr, str)
	}

	var txnEmpty roachpb.Transaction
	_ = txnEmpty.String() // prevent regression of NPE

	cmd := storagebase.RaftCommand{
		BatchRequest: &roachpb.BatchRequest{},
	}
	cmd.BatchRequest.Txn = &txn
	if actStr, idStr := cmd.String(), txnID.String(); !strings.Contains(actStr, idStr) {
		t.Fatalf("expected to find '%s' in '%s'", idStr, actStr)
	}
}

func TestBatchRequestString(t *testing.T) {
	br := roachpb.BatchRequest{}
	br.Txn = new(roachpb.Transaction)
	for i := 0; i < 100; i++ {
		br.Requests = append(br.Requests, roachpb.RequestUnion{Get: &roachpb.GetRequest{}})
	}
	br.Requests = append(br.Requests, roachpb.RequestUnion{EndTransaction: &roachpb.EndTransactionRequest{}})

	e := `[txn: <nil>], Get [/Min,/Min), Get [/Min,/Min), Get [/Min,/Min), Get [/Min,/Min), Get [/Min,/Min), Get [/Min,/Min), Get [/Min,/Min), Get [/Min,/Min), Get [/Min,/Min), Get [/Min,/Min), Get [/Min,/Min), Get [/Min,/Min), Get [/Min,/Min), Get [/Min,/Min), Get [/Min,/Min), Get [/Min,/Min), Get [/Min,/Min), Get [/Min,/Min), Get [/Min,/Min), Get [/Min,/Min), ... 76 skipped ..., Get [/Min,/Min), Get [/Min,/Min), Get [/Min,/Min), Get [/Min,/Min), EndTransaction [/Min,/Min)`
	if e != br.String() {
		t.Fatalf("e = %s, v = %s", e, br.String())
	}
}
