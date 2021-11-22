// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

// Package svprovider plumbs dependencies into the implementation constructors.
package svprovider

import (
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/kv/kvclient/rangefeed"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/singleversion/svacquirer"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/singleversion/svpreemptor"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/singleversion/svstorage"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlliveness"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
)

// Provider encapsulates the singleversion subsystem for use in the sql server.
type Provider struct {
	*svacquirer.Acquirer
	*svpreemptor.Preemptor
}

// NewProvider constructs a new Provider.
func NewProvider(
	ambientCtx log.AmbientContext,
	kvDB *kv.DB,
	codec keys.SQLCodec,
	tableID descpb.ID,
	stopper *stop.Stopper,
	instance sqlliveness.Instance,
	reader sqlliveness.Reader,
	rf *rangefeed.Factory,
) *Provider {
	storage := svstorage.NewStorage(codec, tableID)
	return &Provider{
		Acquirer:  svacquirer.NewAcquirer(ambientCtx, stopper, kvDB, rf, storage, instance),
		Preemptor: svpreemptor.NewPreemptor(stopper, kvDB, rf, storage, reader),
	}
}
