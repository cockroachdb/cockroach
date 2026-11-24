// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

// Package evalcatalog provides the concrete implementation of
// eval.CatalogBuiltins.
package evalcatalog

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descs"
	"github.com/cockroachdb/cockroach/pkg/sql/privilege"
)

// AuthorizationAccessor is an interface for checking if the current user has
// any privilege on a descriptor. This matches the planner's authorization
// interface to avoid closure allocations.
type AuthorizationAccessor interface {
	HasAnyPrivilege(ctx context.Context, obj privilege.Object) (bool, error)
}

// Builtins implements methods to evaluate logic that depends on having
// catalog access. It implements the eval.Catalog interface. Note that it
// importantly is not the planner directly.
//
// NOTE: The hope is that many of the methods of the planner will make their
// way to this object and that this object may subsume awareness of session
// information.
//
// TODO(ajwerner): Extract the sql.schemaResolver and consider unifying with
// this thing or wrapping that thing.
type Builtins struct {
	codec         keys.SQLCodec
	dc            *descs.Collection
	txn           *kv.Txn
	authzAccessor AuthorizationAccessor
}

// Init initializes the fields of a Builtins. The object should not be used
// before being initialized. The authzAccessor is optional and is used to check
// if the current user has any privilege on a descriptor.
func (ec *Builtins) Init(
	codec keys.SQLCodec,
	txn *kv.Txn,
	descriptors *descs.Collection,
	authzAccessor AuthorizationAccessor,
) {
	ec.codec = codec
	ec.txn = txn
	ec.dc = descriptors
	ec.authzAccessor = authzAccessor
}

// SetTxn updates the kv.Txn used by the Builtins.
func (ec *Builtins) SetTxn(txn *kv.Txn) {
	ec.txn = txn
}
