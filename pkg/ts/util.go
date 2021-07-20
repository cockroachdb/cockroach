// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package ts

import (
	"encoding/gob"
	"io"

	"github.com/cockroachdb/cockroach/pkg/ts/tspb"
)

// DumpRawTo is a helper that gob-encodes all messages received from the
// source stream to the given WriteCloser.
func DumpRawTo(src tspb.TimeSeries_DumpRawClient, out io.Writer) error {
	enc := gob.NewEncoder(out)
	for {
		data, err := src.Recv()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}
		if err := enc.Encode(data); err != nil {
			return err
		}
	}
}
