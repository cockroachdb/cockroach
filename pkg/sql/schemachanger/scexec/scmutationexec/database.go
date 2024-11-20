// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package scmutationexec

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/config/zonepb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catprivilege"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/dbdesc"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scop"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
)

func (i *immediateVisitor) CreateDatabaseDescriptor(
	ctx context.Context, op scop.CreateDatabaseDescriptor,
) error {
	db := &descpb.DatabaseDescriptor{
		Name:              "", // Set by `DatabaseName` element
		ID:                op.DatabaseID,
		Version:           1,
		Privileges:        &catpb.PrivilegeDescriptor{Version: catpb.Version23_2}, // Populated by `UserPrivileges` elements and `Owner` element,
		Schemas:           map[string]descpb.DatabaseDescriptor_SchemaInfo{},      // Populated by `SchemaParent` element
		State:             descpb.DescriptorState_ADD,
		RegionConfig:      nil,
		DefaultPrivileges: catprivilege.MakeDefaultPrivilegeDescriptor(catpb.DefaultPrivilegeDescriptor_DATABASE),
	}
	mut := dbdesc.NewBuilder(db).BuildCreatedMutableDatabase()
	i.CreateDescriptor(mut)
	return nil
}

func (i *immediateVisitor) AddDatabaseZoneConfig(
	ctx context.Context, op scop.AddDatabaseZoneConfig,
) error {
	i.ImmediateMutationStateUpdater.UpdateZoneConfig(op.DatabaseID,
		protoutil.Clone(&op.ZoneConfig).(*zonepb.ZoneConfig))
	return nil
}
