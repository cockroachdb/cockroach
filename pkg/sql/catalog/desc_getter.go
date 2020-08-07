package catalog

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
)

// DescGetter is an interface to retrieve descriptors.
type DescGetter interface {
	GetDesc(ctx context.Context, id descpb.ID) (Descriptor, error)
	GetDescs(ctx context.Context, reqs []descpb.ID) ([]Descriptor, error)
}

// GetTypeDescFromID retrieves the type descriptor for the type ID passed
// in using an existing descGetter. It returns an error if the descriptor
// doesn't exist or if it exists and is not a type descriptor.
func GetTypeDescFromID(ctx context.Context, dg DescGetter, id descpb.ID) (TypeDescriptor, error) {
	desc, err := dg.GetDesc(ctx, id)
	if err != nil {
		return nil, err
	}
	typ, ok := desc.(TypeDescriptor)
	if !ok {
		return nil, ErrDescriptorNotFound
	}
	return typ, nil
}

// GetTableDescFromID retrieves the table descriptor for the table
// ID passed in using an existing proto getter. Returns an error if the
// descriptor doesn't exist or if it exists and is not a table.
func GetTableDescFromID(ctx context.Context, dg DescGetter, id descpb.ID) (TableDescriptor, error) {
	desc, err := dg.GetDesc(ctx, id)
	if err != nil {
		return nil, err
	}
	table, ok := desc.(TableDescriptor)
	if !ok {
		return nil, ErrDescriptorNotFound
	}
	return table, nil
}

// MapDescGetter is a protoGetter that has a hard-coded map of keys to proto
// messages.
type MapDescGetter map[descpb.ID]Descriptor

// GetDesc implements the catalog.DescGetter interface.
func (m MapDescGetter) GetDesc(ctx context.Context, id descpb.ID) (Descriptor, error) {
	desc := m[id]
	return desc, nil
}

// GetDescs implements the catalog.DescGetter interface.
func (m MapDescGetter) GetDescs(ctx context.Context, ids []descpb.ID) ([]Descriptor, error) {
	ret := make([]Descriptor, len(ids))
	for i, id := range ids {
		ret[i], _ = m.GetDesc(ctx, id)
	}
	return ret, nil
}
