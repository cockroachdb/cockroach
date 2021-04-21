// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package ssh

import "io"

// ProgressWriter TODO(peter): document
type ProgressWriter struct {
	Writer   io.Writer
	Done     int64
	Total    int64
	Progress func(float64)
}

func (p *ProgressWriter) Write(b []byte) (int, error) {
	n, err := p.Writer.Write(b)
	if err == nil {
		p.Done += int64(n)
		p.Progress(float64(p.Done) / float64(p.Total))
	}
	return n, err
}

// InsecureIgnoreHostKey TODO(peter): document
var InsecureIgnoreHostKey bool
