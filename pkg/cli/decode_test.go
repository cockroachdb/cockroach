// Copyright 2016 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

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

	protoName := "cockroach.sql.sqlbase.TableDescriptor"
	marshal := func(pb protoutil.Message) []byte {
		s, err := protoutil.Marshal(pb)
		require.NoError(t, err)
		return s
	}
	toJSON := func(pb protoutil.Message) string {
		j, err := protoreflect.MessageToJSON(pb, true)
		require.NoError(t, err)
		return j.String()
	}
	tableDesc := &descpb.TableDescriptor{ID: 42, ParentID: 7, Name: "foo"}

	tests := []struct {
		name    string
		s       string
		wantOK  bool
		wantVal string
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
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotOk, gotVal, err := tryDecodeValue(tt.s, protoName, true /* emitDefaults */)
			require.Equal(t, tt.wantOK, gotOk)
			require.NoError(t, err)
			require.Equal(t, gotVal, tt.wantVal)
		})
	}
}
