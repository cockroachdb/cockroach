// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package blobfixture

import (
	"context"
	"net/url"
	"path"
	"regexp"
	"strings"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/cloud"
	"github.com/cockroachdb/cockroach/pkg/roachprod/logger"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/util/ioctx"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
)

// Registry is used to manage fixtures stored in object storage. It tracks
// which fixtures are ready for use and uses garbage collection to delete
// leaked clusters.
type Registry struct {
	// storage is used to read and write fixture data and metadata. storage is
	// configured using the root of the bucket. For example, if the uri is
	// gs://cockroach-fixtures/roachprod/v25.1, the 'roachprod/v25.1' part is
	// removed when creating the storage instance. This improves the legibility
	// of paths in fixture metadata objects.
	storage cloud.ExternalStorage

	// clock is used to generate timestamps for fixtures. It is a variable so that
	// it can be replaced in tests.
	clock func() time.Time

	// uri is the prefix for all fixture data and metadata. Do not log the uri.
	// It may contain authentication information.
	//
	// Fixtures are stored in the following layout:
	// <uri>/metadata/<kind>/<timestamp> contains metadata for a fixture instance
	// <uri>/<kind>/<timestamp> contains the actual fixture data
	//
	// The uri ispassed to the registry at construction time. The baseURI is
	// expected to be of the form "scheme://<bucket>/roachprod/<version>". So a
	// full metadata path looks like:
	// gs://cockroach-fixtures/roachprod/v25.1/metadata/backup-tpcc-30k/20220101-1504
	//
	uri url.URL
}

// NewRegistry creates a new Registry instance. The uri parameter is the prefix
// for all fixture data and metadata. See the comment on the uri field for the
// structure of a fixture directory.
func NewRegistry(ctx context.Context, uri url.URL) (*Registry, error) {
	supportedSchemes := map[string]bool{"gs": true, "s3": true, "azure": true}
	if !supportedSchemes[uri.Scheme] {
		return nil, errors.Errorf("unsupported scheme %q", uri.Scheme)
	}

	storage, err := cloud.EarlyBootExternalStorageFromURI(
		ctx,
		getBaseURI(uri),
		base.ExternalIODirConfig{},
		cluster.MakeTestingClusterSettings(),
		nil, /*limiters */
		cloud.NilMetrics,
	)
	if err != nil {
		return nil, err
	}

	return &Registry{
		storage: storage,
		uri:     uri,
		clock:   timeutil.Now,
	}, nil
}

func getBaseURI(uri url.URL) string {
	baseUri := uri
	baseUri.Path = ""
	return baseUri.String()
}

// GetLatest returns the most recent ready fixture of the given kind.
func (r *Registry) GetLatest(ctx context.Context, kind string) (FixtureMetadata, error) {
	metadataPrefix := path.Join(r.uri.Path, "metadata", kind)

	fixtures, err := r.listFixtures(ctx, metadataPrefix, nil)
	if err != nil {
		return FixtureMetadata{}, errors.Wrapf(err, "failed to fetch metadata in %q", metadataPrefix)
	}

	found := mostRecent(fixtures)
	if found == nil {
		return FixtureMetadata{}, errors.Errorf("no fixtures found for kind %q in %q", kind, metadataPrefix)
	}

	return *found, nil
}

// kindRegex is used to validate the kind name when creating fixtures. Fixture
// names are allowed to be lower-snake-case-with-1337-numbers. For example:
// backup-tpcc-30k is an allowed kind name.
var kindRegex = regexp.MustCompile(`^[a-z0-9\-]+$`)

// Create creates a new fixture of the given kind. Once the fixture generator
// is finished creating the fixture, it should call SetReadyAt to mark the
// fixture as ready for use.
func (r *Registry) Create(
	ctx context.Context, kind string, l *logger.Logger,
) (ScratchHandle, error) {
	if !kindRegex.MatchString(kind) {
		return ScratchHandle{}, errors.Errorf("invalid kind name %q", kind)
	}

	if strings.Contains(kind, "metadata") {
		return ScratchHandle{}, errors.Errorf("kind name %q contains reserved word 'metadata'", kind)
	}

	now := r.clock().UTC()
	basename := now.Format("20060102-1504")

	metadata := FixtureMetadata{
		CreatedAt:    now,
		Kind:         kind,
		DataPath:     path.Join(r.uri.Path, kind, basename),
		MetadataPath: path.Join(r.uri.Path, "metadata", kind, basename),
	}

	if err := r.upsertMetadata(metadata); err != nil {
		return ScratchHandle{}, err
	}

	l.Printf("creating fixture %q", metadata.DataPath)

	return ScratchHandle{registry: r, logger: l, metadata: metadata}, nil
}

// GC deletes fixtures that were leaked or obsolete. See the comment on the
// fixturesToGc for details about the GC policy.
func (r *Registry) GC(ctx context.Context, l *logger.Logger) error {
	fixtures, err := r.listFixtures(ctx, path.Join(r.uri.Path, "metadata"), l)
	if err != nil {
		return errors.Wrap(err, "gc failed to list metadata")
	}

	toDelete := fixturesToGc(r.clock(), fixtures)

	l.Printf("running gc: deleting %d out of %d fixtures", len(toDelete), len(fixtures))

	for _, f := range toDelete {
		l.Printf("deleting fixture %q: %s", f.metadata.DataPath, f.reason)
		if err := r.deleteBlobsMatchingPrefix(f.metadata.DataPath); err != nil {
			return err
		}
		if err := r.deleteMetadata(f.metadata); err != nil {
			return err
		}
	}

	return nil
}

func (r *Registry) Close() {
	_ = r.storage.Close()
}

// URI returns a new URL with the given path appended to the registry's URI.
//
// Be careful when logging the URI. The query paremeters may contain
// authentication information.
//
// Example:
// fixture = r.GetLatest(ctx, "backup-tpcc-30k")
// r.URI(fixture.DataPath) returns 'gs://cockroach-fixtures/roachprod/v25.1/backup-tpcc-30k/20220101-1504'
func (r *Registry) URI(path string) url.URL {
	copy := r.uri
	copy.Path = path
	return copy
}

// maybeReadFile attempts to read a file and returns its contents. Returns nil bytes
// if the file does not exist. This is useful for handling cases where files may have
// been concurrently deleted by GC.
func (r *Registry) maybeReadFile(ctx context.Context, filename string) ([]byte, error) {
	bytes, err := func() ([]byte, error) {
		file, _, err := r.storage.ReadFile(ctx, filename, cloud.ReadOptions{})
		if err != nil {
			return nil, err
		}
		defer func() { _ = file.Close(ctx) }()

		bytes, err := ioctx.ReadAll(ctx, file)
		if err != nil {
			return nil, err
		}
		return bytes, nil
	}()
	if errors.Is(err, cloud.ErrFileDoesNotExist) {
		return nil, nil
	}
	return bytes, err
}

// listFixtures lists all fixtures of the given kind.
func (r *Registry) listFixtures(
	ctx context.Context, kindPrefix string, l *logger.Logger,
) ([]FixtureMetadata, error) {
	if l != nil {
		l.Printf("listing fixtures: %s", kindPrefix)
	}
	var result []FixtureMetadata

	err := r.storage.List(ctx, kindPrefix /*delimiter*/, "", func(found string) error {
		json, err := r.maybeReadFile(ctx, path.Join(kindPrefix, found))
		if err != nil {
			return err
		}
		if json == nil {
			return nil // Skip files that don't exist (may have been GC'd)
		}

		metadata := FixtureMetadata{}
		if err := metadata.UnmarshalJson(json); err != nil {
			return err
		}

		result = append(result, metadata)
		return nil
	})
	if err != nil {
		return nil, err
	}

	return result, nil
}

func (r *Registry) upsertMetadata(metadata FixtureMetadata) error {
	json, err := metadata.MarshalJson()
	if err != nil {
		return err
	}

	writer, err := r.storage.Writer(context.Background(), metadata.MetadataPath)
	if err != nil {
		return err
	}

	if _, err := writer.Write(json); err != nil {
		_ = writer.Close()
		return err
	}

	return writer.Close()
}

func (r *Registry) deleteMetadata(metadata FixtureMetadata) error {
	return errors.Wrap(r.storage.Delete(context.Background(), metadata.MetadataPath), "failed to delete metadata")
}

func (r *Registry) deleteBlobsMatchingPrefix(prefix string) error {
	err := r.storage.List(context.Background(), prefix, "", func(path string) error {
		return r.storage.Delete(context.Background(), prefix+path)
	})
	return errors.Wrapf(err, "failed to delete blobs matching prefix %q", prefix)
}

// ScratchHandle is returned by Registry.Create and is used to mark a fixture
// as ready for use.
type ScratchHandle struct {
	registry *Registry
	logger   *logger.Logger
	metadata FixtureMetadata
}

func (s *ScratchHandle) Metadata() FixtureMetadata {
	return s.metadata
}

// SetReadyAt marks the fixture as ready for use. This should be called after
// the fixture generator has finished creating the fixture and is no longer
// writing to the data path.
func (s *ScratchHandle) SetReadyAt(ctx context.Context) error {
	ready := s.registry.clock()
	s.metadata.ReadyAt = &ready
	if err := s.registry.upsertMetadata(s.metadata); err != nil {
		return err
	}
	s.logger.Printf("fixture '%s' ready at '%s'", s.metadata.DataPath, s.metadata.ReadyAt)
	return nil
}
