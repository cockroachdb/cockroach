// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package userfile

import (
	"context"
	"fmt"
	"io"
	"net/url"
	"path"
	"sort"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/cloud"
	"github.com/cockroachdb/cockroach/pkg/cloud/userfile/filetable"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/server/telemetry"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlutil"
	"github.com/cockroachdb/cockroach/pkg/util/ioctx"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/errors/oserror"
)

const (
	// DefaultQualifiedNamespace is the default FQN namespace prefix
	// used when referencing tables in userfile.
	DefaultQualifiedNamespace = "defaultdb.public."
	// DefaultQualifiedNamePrefix is the default FQN table name prefix.
	DefaultQualifiedNamePrefix = "userfiles_"
)

func parseUserfileURL(
	args cloud.ExternalStorageURIContext, uri *url.URL,
) (roachpb.ExternalStorage, error) {
	conf := roachpb.ExternalStorage{}
	qualifiedTableName := uri.Host
	if args.CurrentUser.Undefined() {
		return conf, errors.Errorf("user creating the FileTable ExternalStorage must be specified")
	}
	normUser := args.CurrentUser.Normalized()

	// If the import statement does not specify a qualified table name then use
	// the default to attempt to locate the file(s).
	if qualifiedTableName == "" {
		composedTableName := security.MakeSQLUsernameFromPreNormalizedString(
			DefaultQualifiedNamePrefix + normUser)
		qualifiedTableName = DefaultQualifiedNamespace +
			// Escape special identifiers as needed.
			composedTableName.SQLIdentifier()
	}

	conf.Provider = roachpb.ExternalStorageProvider_userfile
	conf.FileTableConfig.User = normUser
	conf.FileTableConfig.QualifiedTableName = qualifiedTableName
	conf.FileTableConfig.Path = uri.Path
	return conf, nil
}

type fileTableStorage struct {
	fs       *filetable.FileToTableSystem
	cfg      roachpb.ExternalStorage_FileTable
	ioConf   base.ExternalIODirConfig
	db       *kv.DB
	ie       sqlutil.InternalExecutor
	prefix   string // relative filepath
	settings *cluster.Settings
}

var _ cloud.ExternalStorage = &fileTableStorage{}

func makeFileTableStorage(
	ctx context.Context, args cloud.ExternalStorageContext, dest roachpb.ExternalStorage,
) (cloud.ExternalStorage, error) {
	telemetry.Count("external-io.filetable")

	cfg := dest.FileTableConfig
	if cfg.User == "" || cfg.QualifiedTableName == "" {
		return nil, errors.Errorf("FileTable storage requested but username or qualified table name" +
			" not provided")
	}

	// FileTableStorage is not backed by a file system and so the name of the file
	// written to the underlying SQL tables will be the entire path of the
	// userfile URI. We ensure that the path post normalization is the same as the
	// path which the user inputted in the userfile URI to reject paths which may
	// lead to user surprises.
	// For example, users may expect:
	// - a/./b == a/b
	// - test/../test.csv == test/test.csv
	// but this is not the case since FileTableStorage does not offer file system
	// semantics.
	if path.Clean(cfg.Path) != cfg.Path {
		// Userfile upload writes files with a .tmp prefix. For better error
		// messages we trim this suffix before bubbling the error up.
		trimmedPath := strings.TrimSuffix(cfg.Path, ".tmp")
		return nil, errors.Newf("path %s changes after normalization to %s. "+
			"userfile upload does not permit such path constructs",
			trimmedPath, path.Clean(trimmedPath))
	}

	// cfg.User is already a normalized SQL username.
	username := security.MakeSQLUsernameFromPreNormalizedString(cfg.User)
	executor := filetable.MakeInternalFileToTableExecutor(args.InternalExecutor, args.DB)
	fileToTableSystem, err := filetable.NewFileToTableSystem(ctx,
		cfg.QualifiedTableName, executor, username)
	if err != nil {
		return nil, err
	}
	return &fileTableStorage{
		fs:       fileToTableSystem,
		cfg:      cfg,
		ioConf:   args.IOConf,
		db:       args.DB,
		ie:       args.InternalExecutor,
		prefix:   cfg.Path,
		settings: args.Settings,
	}, nil
}

// MakeSQLConnFileTableStorage returns an instance of a FileTableStorage which
// uses a network connection backed SQL executor. This is used by the CLI to
// interact with the underlying FileToTableSystem. It only supports a subset of
// methods compared to the internal SQL connection backed FileTableStorage.
func MakeSQLConnFileTableStorage(
	ctx context.Context, cfg roachpb.ExternalStorage_FileTable, conn cloud.SQLConnI,
) (cloud.ExternalStorage, error) {
	executor := filetable.MakeSQLConnFileToTableExecutor(conn)

	// cfg.User is already a normalized username,
	username := security.MakeSQLUsernameFromPreNormalizedString(cfg.User)

	fileToTableSystem, err := filetable.NewFileToTableSystem(ctx,
		cfg.QualifiedTableName, executor, username)
	if err != nil {
		return nil, err
	}
	prefix := cfg.Path
	if !strings.HasPrefix(prefix, "/") {
		prefix = "/" + prefix
	}
	return &fileTableStorage{
		fs:       fileToTableSystem,
		cfg:      cfg,
		ioConf:   base.ExternalIODirConfig{},
		prefix:   prefix,
		settings: nil,
	}, nil
}

// MakeUserFileStorageURI converts a qualified table name and filename
// to a valid userfile URI.
func MakeUserFileStorageURI(qualifiedTableName, filename string) string {
	return fmt.Sprintf("userfile://%s/%s", qualifiedTableName, filename)
}

// Close implements the ExternalStorage interface and is a no-op.
func (f *fileTableStorage) Close() error {
	return nil
}

// Conf implements the ExternalStorage interface and returns the FileTable
// configuration.
func (f *fileTableStorage) Conf() roachpb.ExternalStorage {
	return roachpb.ExternalStorage{
		Provider:        roachpb.ExternalStorageProvider_userfile,
		FileTableConfig: f.cfg,
	}
}

// ExternalIOConf implements the ExternalStorage interface and returns the
// server configuration for the ExternalStorage implementation.
func (f *fileTableStorage) ExternalIOConf() base.ExternalIODirConfig {
	return f.ioConf
}

func (f *fileTableStorage) Settings() *cluster.Settings {
	return f.settings
}

// Userfile storage does not provide file system semantics and thus to prevent
// user surprises we reject file paths which are different pre- and
// post-normalization. We already enforce this on prefix when the
// fileTableStorage is instantiated, so this method enforces the same on
// basename.
func checkBaseAndJoinFilePath(prefix, basename string) (string, error) {
	if basename == "" {
		return prefix, nil
	}

	if path.Clean(basename) != basename {
		return "", errors.Newf("basename %s changes to %s on normalization. "+
			"userfile does not permit such constructs.", basename, path.Clean(basename))
	}
	return path.Join(prefix, basename), nil
}

// ReadFile is shorthand for ReadFileAt with offset 0.
func (f *fileTableStorage) ReadFile(
	ctx context.Context, basename string,
) (ioctx.ReadCloserCtx, error) {
	body, _, err := f.ReadFileAt(ctx, basename, 0)
	return body, err
}

// ReadFile implements the ExternalStorage interface and returns the contents of
// the file stored in the user scoped FileToTableSystem.
func (f *fileTableStorage) ReadFileAt(
	ctx context.Context, basename string, offset int64,
) (ioctx.ReadCloserCtx, int64, error) {
	filepath, err := checkBaseAndJoinFilePath(f.prefix, basename)
	if err != nil {
		return nil, 0, err
	}
	reader, size, err := f.fs.ReadFile(ctx, filepath, offset)
	if oserror.IsNotExist(err) {
		return nil, 0, errors.Wrapf(cloud.ErrFileDoesNotExist,
			"file %s does not exist in the UserFileTableSystem", filepath)
	}

	return reader, size, err
}

// Writer implements the ExternalStorage interface and writes the file to the
// user scoped FileToTableSystem.
func (f *fileTableStorage) Writer(ctx context.Context, basename string) (io.WriteCloser, error) {
	filepath, err := checkBaseAndJoinFilePath(f.prefix, basename)
	if err != nil {
		return nil, err
	}

	// This is only possible if the method is invoked by a SQLConnFileTableStorage
	// which should never be the case.
	if f.ie == nil {
		return nil, errors.New("cannot Write without a configured internal executor")
	}

	return f.fs.NewFileWriter(ctx, filepath, filetable.ChunkDefaultSize)
}

// List implements the ExternalStorage interface.
func (f *fileTableStorage) List(
	ctx context.Context, prefix, delim string, fn cloud.ListingFn,
) error {
	dest := cloud.JoinPathPreservingTrailingSlash(f.prefix, prefix)

	res, err := f.fs.ListFiles(ctx, dest)
	if err != nil {
		return errors.Wrap(err, "fail to list destination")
	}

	sort.Strings(res)
	var prevPrefix string

	for _, f := range res {
		f = strings.TrimPrefix(f, dest)
		if delim != "" {
			if i := strings.Index(f, delim); i >= 0 {
				f = f[:i+len(delim)]
			}
			if f == prevPrefix {
				continue
			}
			prevPrefix = f
		}
		if err := fn(f); err != nil {
			return err
		}
	}

	return nil
}

// Delete implements the ExternalStorage interface and deletes the file from the
// user scoped FileToTableSystem.
func (f *fileTableStorage) Delete(ctx context.Context, basename string) error {
	filepath, err := checkBaseAndJoinFilePath(f.prefix, basename)
	if err != nil {
		return err
	}
	return f.fs.DeleteFile(ctx, filepath)
}

// Size implements the ExternalStorage interface and returns the size of the
// file stored in the user scoped FileToTableSystem.
func (f *fileTableStorage) Size(ctx context.Context, basename string) (int64, error) {
	filepath, err := checkBaseAndJoinFilePath(f.prefix, basename)
	if err != nil {
		return 0, err
	}
	return f.fs.FileSize(ctx, filepath)
}

func init() {
	cloud.RegisterExternalStorageProvider(roachpb.ExternalStorageProvider_userfile,
		parseUserfileURL, makeFileTableStorage, cloud.RedactedParams(), "userfile")
}
