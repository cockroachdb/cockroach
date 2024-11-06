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

// NoopUpdater is an implementation of ITableMetadataUpdater that performs a noop when RunUpdater is called.
// This should only be used in tests when the updating of the table_metadata system table isn't necessary.
type NoopUpdater struct{}

func (nu *NoopUpdater) RunUpdater(_ctx context.Context) error {
	return nil
}

var _ ITableMetadataUpdater = &NoopUpdater{}
