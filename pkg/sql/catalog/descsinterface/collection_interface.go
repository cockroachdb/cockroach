package descsinterface

import (
	"context"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/nstree"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlliveness"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
)

// DescriptorCollection is an interface for a descriptor collection.
type DescriptorCollection interface {
	MaybeUpdateDeadline(ctx context.Context, txn *kv.Txn) (err error)
	SetMaxTimestampBound(maxTimestampBound hlc.Timestamp)
	ResetMaxTimestampBound()
	SkipValidationOnWrite()
	ReleaseLeases(ctx context.Context)
	ReleaseAll(ctx context.Context)
	ResetSyntheticDescriptors()
	HasUncommittedTables() bool
	HasUncommittedTypes() bool
	AddUncommittedDescriptor(desc catalog.MutableDescriptor) error
	WriteDescToBatch(
		ctx context.Context, kvTrace bool, desc catalog.MutableDescriptor, b *kv.Batch,
	) error
	WriteDesc(
		ctx context.Context, kvTrace bool, desc catalog.MutableDescriptor, txn *kv.Txn,
	) error
	GetUncommittedTables() (tables []catalog.TableDescriptor)
	GetAllDescriptors(ctx context.Context, txn *kv.Txn) (nstree.Catalog, error)
	GetAllDatabaseDescriptors(
		ctx context.Context, txn *kv.Txn,
	) ([]catalog.DatabaseDescriptor, error)
	GetAllTableDescriptorsInDatabase(
		ctx context.Context, txn *kv.Txn, dbID descpb.ID,
	) ([]catalog.TableDescriptor, error)
	GetSchemasForDatabase(
		ctx context.Context, txn *kv.Txn, dbDesc catalog.DatabaseDescriptor,
	) (map[descpb.ID]string, error)
	GetObjectNamesAndIDs(
		ctx context.Context,
		txn *kv.Txn,
		dbDesc catalog.DatabaseDescriptor,
		scName string,
		flags tree.DatabaseListFlags,
	) (tree.TableNames, descpb.IDs, error)
	SetSyntheticDescriptors(descs []catalog.Descriptor)
	AddSyntheticDescriptor(desc catalog.Descriptor)
	RemoveSyntheticDescriptor(id descpb.ID)
	AddDeletedDescriptor(id descpb.ID)
	SetSession(session sqlliveness.Session)
}
