package server

import (
	"bytes"
	"context"
	"errors"
	"io/ioutil"

	"github.com/golang/protobuf/proto"
	"google.golang.org/genproto/googleapis/rpc/code"
	"google.golang.org/genproto/googleapis/rpc/status"
	"google.golang.org/grpc/codes"
	grpc_status "google.golang.org/grpc/status"

	pb "github.com/buchgr/bazel-remote/genproto/build/bazel/remote/execution/v2"

	"github.com/buchgr/bazel-remote/cache"
)

var (
	errBadSize      = errors.New("Unexpected size")
	errBlobNotFound = errors.New("Blob not found")
)

// ContentAddressableStorageServer interface:

func (s *grpcServer) FindMissingBlobs(ctx context.Context,
	req *pb.FindMissingBlobsRequest) (*pb.FindMissingBlobsResponse, error) {

	resp := pb.FindMissingBlobsResponse{}

	errorPrefix := "GRPC CAS GET"
	for _, digest := range req.BlobDigests {
		hash := digest.GetHash()
		err := s.validateHash(hash, digest.SizeBytes, errorPrefix)
		if err != nil {
			return nil, err
		}

		found, _ := s.cache.Contains(cache.CAS, hash, digest.GetSizeBytes())
		if !found {
			s.accessLogger.Printf("GRPC CAS HEAD %s NOT FOUND", hash)
			resp.MissingBlobDigests = append(resp.MissingBlobDigests, digest)
		} else {
			s.accessLogger.Printf("GRPC CAS HEAD %s OK", hash)
		}
	}

	return &resp, nil
}

func (s *grpcServer) BatchUpdateBlobs(ctx context.Context,
	in *pb.BatchUpdateBlobsRequest) (*pb.BatchUpdateBlobsResponse, error) {

	resp := pb.BatchUpdateBlobsResponse{
		Responses: make([]*pb.BatchUpdateBlobsResponse_Response,
			0, len(in.Requests)),
	}

	errorPrefix := "GRPC CAS PUT"
	for _, req := range in.Requests {
		// TODO: consider fanning-out goroutines here.
		err := s.validateHash(req.Digest.Hash, req.Digest.SizeBytes, errorPrefix)
		if err != nil {
			return nil, err
		}

		rr := pb.BatchUpdateBlobsResponse_Response{
			Digest: &pb.Digest{
				Hash:      req.Digest.Hash,
				SizeBytes: req.Digest.SizeBytes,
			},
			Status: &status.Status{},
		}
		resp.Responses = append(resp.Responses, &rr)

		err = s.cache.Put(cache.CAS, req.Digest.Hash,
			int64(len(req.Data)), bytes.NewReader(req.Data))
		if err != nil {
			s.errorLogger.Printf("%s %s %s", errorPrefix, req.Digest.Hash, err)
			rr.Status.Code = int32(code.Code_UNKNOWN)
			continue
		}

		s.accessLogger.Printf("GRPC CAS PUT %s OK", req.Digest.Hash)
	}

	return &resp, nil
}

// Return the data for a blob, or an error.  If the blob was not
// found, the returned error is errBlobNotFound. Only use this
// function when it's OK to buffer the entire blob in memory.
func (s *grpcServer) getBlobData(hash string, size int64) ([]byte, error) {
	if size < 0 {
		return []byte{}, errBadSize
	}

	if size == 0 {
		return []byte{}, nil
	}

	rdr, sizeBytes, err := s.cache.Get(cache.CAS, hash, size)
	if err != nil {
		rdr.Close()
		return []byte{}, err
	}

	if rdr == nil {
		return []byte{}, errBlobNotFound
	}

	if sizeBytes != size {
		rdr.Close()
		return []byte{}, errBadSize
	}

	data, err := ioutil.ReadAll(rdr)
	if err != nil {
		rdr.Close()
		return []byte{}, err
	}

	return data, rdr.Close()
}

func (s *grpcServer) getBlobResponse(digest *pb.Digest) *pb.BatchReadBlobsResponse_Response {
	r := pb.BatchReadBlobsResponse_Response{Digest: digest}

	data, err := s.getBlobData(digest.Hash, digest.SizeBytes)
	if err == errBlobNotFound {
		s.accessLogger.Printf("GRPC CAS GET %s NOT FOUND", digest.Hash)
		r.Status = &status.Status{Code: int32(code.Code_NOT_FOUND)}
		return &r
	}

	if err != nil {
		s.errorLogger.Printf("GRPC CAS GET %s INTERNAL ERROR: %v",
			digest.Hash, err)
		r.Status = &status.Status{Code: int32(code.Code_INTERNAL)}
		return &r
	}

	r.Data = data

	s.accessLogger.Printf("GRPC CAS GET %s OK", digest.Hash)
	r.Status = &status.Status{Code: int32(codes.OK)}
	return &r
}

func (s *grpcServer) BatchReadBlobs(ctx context.Context,
	in *pb.BatchReadBlobsRequest) (*pb.BatchReadBlobsResponse, error) {

	resp := pb.BatchReadBlobsResponse{
		Responses: make([]*pb.BatchReadBlobsResponse_Response,
			0, len(in.Digests)),
	}

	errorPrefix := "GRPC CAS GET"
	for _, digest := range in.Digests {
		// TODO: consider fanning-out goroutines here.
		err := s.validateHash(digest.Hash, digest.SizeBytes, errorPrefix)
		if err != nil {
			return nil, err
		}
		resp.Responses = append(resp.Responses, s.getBlobResponse(digest))
	}

	return &resp, nil
}

func (s *grpcServer) GetTree(in *pb.GetTreeRequest,
	stream pb.ContentAddressableStorage_GetTreeServer) error {

	resp := pb.GetTreeResponse{
		Directories: make([]*pb.Directory, 0),
	}
	errorPrefix := "GRPC CAS GETTREEREQUEST"
	err := s.validateHash(in.RootDigest.Hash, in.RootDigest.SizeBytes, errorPrefix)
	if err != nil {
		return err
	}

	data, err := s.getBlobData(in.RootDigest.Hash, in.RootDigest.SizeBytes)
	if err == errBlobNotFound {
		s.accessLogger.Printf("GRPC CAS GETTREEREQUEST %s NOT FOUND",
			in.RootDigest.Hash)
		return grpc_status.Error(codes.NotFound, "Item not found")
	}
	if err != nil {
		s.accessLogger.Printf("%s %s %s", errorPrefix, in.RootDigest.Hash, err)
		return grpc_status.Error(codes.Unknown, err.Error())
	}

	dir := pb.Directory{}
	err = proto.Unmarshal(data, &dir)
	if err != nil {
		s.errorLogger.Printf("%s %s %s", errorPrefix, in.RootDigest.Hash, err)
		return grpc_status.Error(codes.DataLoss, err.Error())
	}

	err = s.fillDirectories(&resp, &dir, errorPrefix)
	if err != nil {
		return err
	}

	stream.Send(&resp)
	// TODO: if resp is too large, split it up and call Send multiple times,
	// with resp.NextPageToken set for all but the last Send call?

	s.accessLogger.Printf("GRPC GETTREEREQUEST %s OK", in.RootDigest.Hash)
	return nil
}

// Attempt to populate `resp`. Return errors for invalid requests, but
// otherwise attempt to return as many blobs as possible.
func (s *grpcServer) fillDirectories(resp *pb.GetTreeResponse, dir *pb.Directory, errorPrefix string) error {

	// Add this dir.
	resp.Directories = append(resp.Directories, dir)

	// Recursively append all the child dirs.
	for _, dirNode := range dir.Directories {

		err := s.validateHash(dirNode.Digest.Hash, dirNode.Digest.SizeBytes, errorPrefix)
		if err != nil {
			return err
		}

		data, err := s.getBlobData(dirNode.Digest.Hash, dirNode.Digest.SizeBytes)
		if err == errBlobNotFound {
			s.accessLogger.Printf("GRPC GETTREEREQUEST BLOB %s NOT FOUND",
				dirNode.Digest.Hash)
			continue
		}
		if err != nil {
			s.accessLogger.Printf("GRPC GETTREEREQUEST BLOB %s ERR: %v", err)
			continue
		}

		dirMsg := pb.Directory{}
		err = proto.Unmarshal(data, &dirMsg)
		if err != nil {
			s.accessLogger.Printf("GRPC GETTREEREQUEST BAD BLOB: %v", err)
			continue
		}

		s.accessLogger.Printf("GRPC GETTREEREQUEST BLOB %s ADDED OK",
			dirNode.Digest.Hash)

		err = s.fillDirectories(resp, &dirMsg, errorPrefix)
		if err != nil {
			return err
		}
	}

	return nil
}
