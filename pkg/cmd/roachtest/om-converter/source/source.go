// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package source

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/om-converter/model"
	"github.com/cockroachdb/errors"
)

type Source interface {
	Start(chan model.FileInfo) error
	Close()
}

func GetSource(ctx context.Context, config *model.Config) (Source, error) {
	switch config.Source.Type {
	case "gcp":
		return NewGCPSource(ctx, config.Source.Bucket, config.Source.Directory)
	case "file":
		return NewFile(config.Source.Directory), nil
	default:
		return nil, errors.Errorf("unknown source type: %s", config.Source.Type)
	}
}
