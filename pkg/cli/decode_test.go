// Copyright 2016 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package cli

import (
	"bytes"
	"encoding/base64"
	gohex "encoding/hex"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/protoreflect"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

func TestStreamMap(t *testing.T) {
	defer leaktest.AfterTest(t)()

	tests := []struct {
		name    string
		in      string
		fn      func(string) (bool, string, error)
		wantOut string
		wantErr bool
	}{
		{
			name:    "id map",
			in:      "\na\n  b\tc\nd  e \t f",
			wantOut: "\na\t\nb\tc\t\nd\te\tf\t\n",
			fn:      func(s string) (bool, string, error) { return true, s, nil },
		},
		{
			name:    "mixed",
			in:      "a  b   c",
			wantOut: "x\tb\twarning:  error\n",
			fn: func(s string) (bool, string, error) {
				switch s {
				case "a":
					return true, "x", nil
				case "b":
					return false, "y", nil
				default:
					return false, "z", errors.New("error")
				}
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var out bytes.Buffer
			err := streamMap(&out, strings.NewReader(tt.in), tt.fn)
			if (err != nil) != tt.wantErr {
				t.Errorf("streamMap() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			require.Equal(t, tt.wantOut, out.String())
		})
	}
}

func TestTryDecodeValue(t *testing.T) {
	defer leaktest.AfterTest(t)()

	const defaultProtoType = "cockroach.sql.sqlbase.TableDescriptor"
	marshal := func(pb protoutil.Message) []byte {
		s, err := protoutil.Marshal(pb)
		require.NoError(t, err)
		return s
	}
	toJSON := func(pb protoutil.Message) string {
		j, err := protoreflect.MessageToJSON(pb, protoreflect.FmtFlags{EmitDefaults: true})
		require.NoError(t, err)
		return j.String()
	}
	tableDesc := &descpb.TableDescriptor{ID: 42, ParentID: 7, Name: "foo"}

	tests := []struct {
		name      string
		protoType string
		s         string
		wantOK    bool
		wantVal   string
	}{
		{
			name:    "from hex",
			s:       gohex.EncodeToString(marshal(tableDesc)),
			wantOK:  true,
			wantVal: toJSON(tableDesc),
		},
		{
			name:    "from base64",
			s:       base64.StdEncoding.EncodeToString(marshal(tableDesc)),
			wantOK:  true,
			wantVal: toJSON(tableDesc),
		},
		{
			name: "junk",
			s:    "@#$@#%$%@",
		},
		{
			name: "hex not proto",
			s:    gohex.EncodeToString([]byte("@#$@#%$%@")),
		},
		{
			name: "base64 not proto",
			s:    base64.StdEncoding.EncodeToString([]byte("@#$@#%$%@")),
		},
		{
			// This is the POST data of an HTTP tsdb query taken from Chrome using
			// "Copy as cURL". It is a quoted string, containing UTF-8 encoded bytes.
			name:      "Chrome-encoded",
			s:         `\u0008\u0080ì¿ùÛ\u008bù\u0083\u0017\u0010\u0080¬¢ÿ¾ôù\u0083\u0017\u001a \n\u0018cr.node.sql.select.count\u0010\u0001\u0018\u0002 \u0002\u001a \n\u0018cr.node.sql.update.count\u0010\u0001\u0018\u0002 \u0002\u001a \n\u0018cr.node.sql.insert.count\u0010\u0001\u0018\u0002 \u0002\u001a \n\u0018cr.node.sql.delete.count\u0010\u0001\u0018\u0002 \u0002\u001a*\n\u001fcr.node.sql.service.latency-p99\u0010\u0003\u0018\u0002 \u0000*\u00011\u001a3\n+cr.node.sql.distsql.contended_queries.count\u0010\u0001\u0018\u0002 \u0002\u001a\u001c\n\u0011cr.store.replicas\u0010\u0001\u0018\u0002 \u0000*\u00011\u001a\u0019\n\u0011cr.store.capacity\u0010\u0001\u0018\u0002 \u0000\u001a#\n\u001bcr.store.capacity.available\u0010\u0001\u0018\u0002 \u0000\u001a\u001e\n\u0016cr.store.capacity.used\u0010\u0001\u0018\u0002 \u0000 \u0080Ø\u008eáo`,
			wantOK:    true,
			protoType: "cockroach.ts.tspb.TimeSeriesQueryRequest",
			wantVal:   `{"endNanos": "1659549679000000000", "queries": [{"derivative": "NON_NEGATIVE_DERIVATIVE", "downsampler": "AVG", "name": "cr.node.sql.select.count", "sourceAggregator": "SUM", "sources": [], "tenantId": {"id": "0"}}, {"derivative": "NON_NEGATIVE_DERIVATIVE", "downsampler": "AVG", "name": "cr.node.sql.update.count", "sourceAggregator": "SUM", "sources": [], "tenantId": {"id": "0"}}, {"derivative": "NON_NEGATIVE_DERIVATIVE", "downsampler": "AVG", "name": "cr.node.sql.insert.count", "sourceAggregator": "SUM", "sources": [], "tenantId": {"id": "0"}}, {"derivative": "NON_NEGATIVE_DERIVATIVE", "downsampler": "AVG", "name": "cr.node.sql.delete.count", "sourceAggregator": "SUM", "sources": [], "tenantId": {"id": "0"}}, {"derivative": "NONE", "downsampler": "MAX", "name": "cr.node.sql.service.latency-p99", "sourceAggregator": "SUM", "sources": ["1"], "tenantId": {"id": "0"}}, {"derivative": "NON_NEGATIVE_DERIVATIVE", "downsampler": "AVG", "name": "cr.node.sql.distsql.contended_queries.count", "sourceAggregator": "SUM", "sources": [], "tenantId": {"id": "0"}}, {"derivative": "NONE", "downsampler": "AVG", "name": "cr.store.replicas", "sourceAggregator": "SUM", "sources": ["1"], "tenantId": {"id": "0"}}, {"derivative": "NONE", "downsampler": "AVG", "name": "cr.store.capacity", "sourceAggregator": "SUM", "sources": [], "tenantId": {"id": "0"}}, {"derivative": "NONE", "downsampler": "AVG", "name": "cr.store.capacity.available", "sourceAggregator": "SUM", "sources": [], "tenantId": {"id": "0"}}, {"derivative": "NONE", "downsampler": "AVG", "name": "cr.store.capacity.used", "sourceAggregator": "SUM", "sources": [], "tenantId": {"id": "0"}}], "sampleNanos": "30000000000", "startNanos": "1659546079000000000"}`,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			protoType := tt.protoType
			if protoType == "" {
				protoType = defaultProtoType
			}
			msg := tryDecodeValue(tt.s, protoType)
			if !tt.wantOK {
				if msg != nil {
					t.Fatal("decoding succeeded unexpectedly")
				}
				return
			}
			if msg == nil {
				t.Fatal("decoding failed")
			}
			json, err := protoreflect.MessageToJSON(msg, protoreflect.FmtFlags{EmitDefaults: true})
			require.NoError(t, err)
			require.Equal(t, tt.wantVal, json.String())
		})
	}
}
