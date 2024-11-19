// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package tablemetadatacacheutil

import "context"

// ITableMetadataUpdater is an interface that exposes a RunUpdater
// to be tableMetadataUpdateJobResumer. This interface primarily
// serves as a way to facilitate better testing of tableMetadataUpdater
// and tableMetadataUpdateJobResumer.
type ITableMetadataUpdater interface {
	RunUpdater(ctx context.Context) error
}
