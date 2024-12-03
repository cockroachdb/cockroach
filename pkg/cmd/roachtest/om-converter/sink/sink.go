// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package sink

import (
	"bytes"
	"context"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/om-converter/model"
	"github.com/cockroachdb/errors"
)

type Sink interface {
	Sink(*bytes.Buffer, string, string) error
	Close()
}

func GetSink(ctx context.Context, config *model.Config) (Sink, error) {
	switch config.Sink.Type {
	case "gcp":
		return NewGCPSink(ctx, config.Sink.Bucket, config.Sink.Directory)
	case "file":
		return NewFileSink(config.Sink.Directory), nil
	default:
		return nil, errors.Errorf("unknown sink type: %s", config.Sink.Type)
	}
}
