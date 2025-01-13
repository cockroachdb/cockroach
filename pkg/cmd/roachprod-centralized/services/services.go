// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package services

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/utils"
)

// IService is an interface for services.
type IService interface {
	StartBackgroundWork(ctx context.Context, l *utils.Logger, errChan chan<- error) error
	Shutdown(ctx context.Context) error
}
