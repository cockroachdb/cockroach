package storage

import (
	"github.com/cockroachdb/cockroach/roachpb"
	"github.com/pkg/errors"
	"golang.org/x/net/context"
)

// InternalServer implements the storage parts of the roachpb.InternalServer
// interface.
type InternalServer struct {
	descriptor *roachpb.NodeDescriptor
	stores     *Stores
}

// MakeInternalServer returns a new instance of InternalServer.
func MakeInternalServer(descriptor *roachpb.NodeDescriptor, stores *Stores) InternalServer {
	return InternalServer{descriptor, stores}
}

func (is InternalServer) execStoreCommand(
	h roachpb.StoreRequestHeader, f func(*Store) error,
) error {
	if h.NodeID != is.descriptor.NodeID {
		return errors.Errorf("request for NodeID %d cannot be served by NodeID %d",
			h.NodeID, is.descriptor.NodeID)
	}
	store, err := is.stores.GetStore(h.StoreID)
	if err != nil {
		return err
	}
	return f(store)
}

// PollFrozen implements the roachpb.InternalServer interface.
func (is InternalServer) PollFrozen(
	ctx context.Context, args *roachpb.PollFrozenRequest,
) (*roachpb.PollFrozenResponse, error) {
	resp := &roachpb.PollFrozenResponse{}
	err := is.execStoreCommand(args.StoreRequestHeader,
		func(s *Store) error {
			resp.Results = s.FrozenStatus(args.CollectFrozen)
			return nil
		})
	return resp, err
}

// Reserve implements the roachpb.InternalServer interface.
func (is InternalServer) Reserve(
	ctx context.Context, req *roachpb.ReservationRequest,
) (*roachpb.ReservationResponse, error) {
	resp := &roachpb.ReservationResponse{}
	err := is.execStoreCommand(req.StoreRequestHeader,
		func(s *Store) error {
			*resp = s.Reserve(ctx, *req)
			return nil
		})
	return resp, err
}

// Batch implements the roachpb.InternalServer interface.
func (is InternalServer) Batch(
	ctx context.Context, args *roachpb.BatchRequest,
) (*roachpb.BatchResponse, error) {
	panic("not implemented")
}
