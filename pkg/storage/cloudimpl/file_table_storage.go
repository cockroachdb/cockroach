// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package cloudimpl

import (
	"context"
	"fmt"
	"io"
	"net/url"
	"path"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/storage/cloud"
	"github.com/cockroachdb/cockroach/pkg/storage/cloudimpl/filetable"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/errors/oserror"
)

const (
	// DefaultUserfileScheme is the default scheme used in a userfile URI.
	DefaultUserfileScheme = "userfile"
	// DefaultQualifiedNamespace is the default FQN namespace prefix
	// used when referencing tables in userfile.
	DefaultQualifiedNamespace = "defaultdb.public."
	// DefaultQualifiedNamePrefix is the default FQN table name prefix.
	DefaultQualifiedNamePrefix = "userfiles_"
)

type fileTableStorage struct {
	fs       *filetable.FileToTableSystem
	cfg      roachpb.ExternalStorage_FileTable
	ioConf   base.ExternalIODirConfig
	db       *kv.DB
	ie       *sql.InternalExecutor
	prefix   string // relative filepath
	settings *cluster.Settings
}

var _ cloud.ExternalStorage = &fileTableStorage{}

func makeFileTableStorage(
	ctx context.Context,
	cfg roachpb.ExternalStorage_FileTable,
	ie *sql.InternalExecutor,
	db *kv.DB,
	settings *cluster.Settings,
	ioConf base.ExternalIODirConfig,
) (cloud.ExternalStorage, error) {
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
	executor := filetable.MakeInternalFileToTableExecutor(ie, db)
	fileToTableSystem, err := filetable.NewFileToTableSystem(ctx,
		cfg.QualifiedTableName, executor, username)
	if err != nil {
		return nil, err
	}
	return &fileTableStorage{
		fs:       fileToTableSystem,
		cfg:      cfg,
		ioConf:   ioConf,
		db:       db,
		ie:       ie,
		prefix:   cfg.Path,
		settings: settings,
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

func makeUserFileURIWithQualifiedName(qualifiedTableName, path string) string {
	userfileURL := url.URL{
		Scheme: DefaultUserfileScheme,
		Host:   qualifiedTableName,
		Path:   path,
	}
	return userfileURL.String()
}

// Close implements the ExternalStorage interface and is a no-op.
func (f *fileTableStorage) Close() error {
	return nil
}

// Conf implements the ExternalStorage interface and returns the FileTable
// configuration.
func (f *fileTableStorage) Conf() roachpb.ExternalStorage {
	return roachpb.ExternalStorage{
		Provider:        roachpb.ExternalStorageProvider_FileTable,
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
func (f *fileTableStorage) ReadFile(ctx context.Context, basename string) (io.ReadCloser, error) {
	body, _, err := f.ReadFileAt(ctx, basename, 0)
	return body, err
}

// ReadFile implements the ExternalStorage interface and returns the contents of
// the file stored in the user scoped FileToTableSystem.
func (f *fileTableStorage) ReadFileAt(
	ctx context.Context, basename string, offset int64,
) (io.ReadCloser, int64, error) {
	filepath, err := checkBaseAndJoinFilePath(f.prefix, basename)
	if err != nil {
		return nil, 0, err
	}
	reader, size, err := f.fs.ReadFile(ctx, filepath, offset)
	if oserror.IsNotExist(err) {
		return nil, 0, errors.Wrapf(ErrFileDoesNotExist,
			"file %s does not exist in the UserFileTableSystem", filepath)
	}

	return reader, size, err
}

// WriteFile implements the ExternalStorage interface and writes the file to the
// user scoped FileToTableSystem.
func (f *fileTableStorage) WriteFile(
	ctx context.Context, basename string, content io.ReadSeeker,
) error {
	filepath, err := checkBaseAndJoinFilePath(f.prefix, basename)
	if err != nil {
		return err
	}

	// This is only possible if the method is invoked by a SQLConnFileTableStorage
	// which should never be the case.
	if f.ie == nil {
		return errors.New("cannot WriteFile without a configured internal executor")
	}

	defer func() {
		_, _ = f.ie.Exec(ctx, "userfile-write-file-commit", nil /* txn */, `COMMIT`)
	}()

	// We open an explicit txn within which we will write the file metadata entry
	// and payload chunks to the userfile tables. We cannot perform these
	// operations within a db.Txn retry loop because when coming from the
	// copyMachine (which backs the userfile CLI upload command), we do not have
	// access to all the file data at once. As a result of which, if a txn were to
	// retry we are not able to seek to the start of `content` and try again,
	// resulting in bytes being missed across txn retry attempts.
	// See chunkWriter.WriteFile for more information about writing semantics.
	_, err = f.ie.Exec(ctx, "userfile-write-file-txn", nil /* txn */, `BEGIN`)
	if err != nil {
		return err
	}

	writer, err := f.fs.NewFileWriter(ctx, filepath, filetable.ChunkDefaultSize)
	if err != nil {
		return err
	}

	if _, err = io.Copy(writer, content); err != nil {
		return errors.Wrap(err, "failed to write using the FileTable writer")
	}

	if err := writer.Close(); err != nil {
		return errors.Wrap(err, "failed to close the FileTable writer")
	}

	return err
}

// getPrefixAndPattern takes a prefix and optionally suffix of a path pattern
// and derives the constant prefix which would be shared by all matching files
// and potentially a glob-pattern to match against any remaining suffix of names
// within that prefix. For example, given "/a/b" and "c/*.go", it returns the
// constant prefix "/a/b/c" and the pattern "*.go" while given "/a/b/c/*.go" and
// the pattern "" returns the same. This is intended to be used in tandem with
// matchesPrefixAndPattern, transforming a storage prefix and pattern suffix as
// passed to a ListFiles call into the arguments it needs to check a match.
// This function also does validation of the prefix and pattern, ensuring that
// if the prefix contains globs patterns there is no pattern suffix and that
// neither the prefix or pattern change after path normalization, to reject ../
// or similar invalid paths (though an exception is made for trailing slash.)
func getPrefixAndPattern(prefix, pattern string) (string, string, error) {
	if trimmed := strings.TrimSuffix(prefix, "/"); trimmed != "" {
		if clean := path.Clean(trimmed); clean != trimmed {
			return "", "", errors.Newf("invalid path %s changes to %s after normalization", prefix, clean)
		}
	}

	globInPrefix := strings.IndexAny(prefix, "*?[")
	if globInPrefix >= 0 {
		if pattern != "" {
			return "", "", errors.New("prefix cannot contain globs pattern when passing an explicit pattern")
		}
		return prefix[:globInPrefix], prefix[globInPrefix:], nil
	}

	if pattern == "" {
		return prefix, "", nil
	}

	if clean := path.Clean(pattern); clean != pattern {
		return "", "", errors.Newf("invalid path %s changes to %s after normalization", pattern, clean)
	}

	globIndex := strings.IndexAny(pattern, "*?[")
	if globIndex < 0 {
		return path.Join(prefix, pattern), "", nil
	}
	constSuffix, patternSuffix := pattern[:globIndex], pattern[globIndex:]
	return path.Join(prefix, constSuffix), patternSuffix, nil
}

func matchesPrefixAndPattern(name, prefix, pattern string) (bool, error) {
	if !strings.HasPrefix(name, prefix) {
		return false, nil
	}
	// If filtering matches within this prefix by a pattern, check remainder of
	// name after prefix against that pattern.
	if pattern != "" {
		rest := strings.TrimPrefix(name[len(prefix):], "/")
		return path.Match(pattern, rest)
	}
	return true, nil
}

// ListFiles implements the ExternalStorage interface and lists the files stored
// in the user scoped FileToTableSystem.
func (f *fileTableStorage) ListFiles(ctx context.Context, patternSuffix string) ([]string, error) {
	prefix, pattern, err := getPrefixAndPattern(f.prefix, patternSuffix)
	if err != nil {
		return nil, err
	}

	var fileList []string
	matches, err := f.fs.ListFiles(ctx, prefix)
	if err != nil {
		return nil, errors.Wrap(err, "unable to match pattern provided")
	}

	for _, match := range matches {
		if matches, err := matchesPrefixAndPattern(match, prefix, pattern); err != nil {
			return nil, err
		} else if matches {
			if strings.HasPrefix(match, f.prefix) {
				fileList = append(fileList, strings.TrimPrefix(strings.TrimPrefix(match, f.prefix), "/"))
			} else {
				match = strings.TrimPrefix(match, "/")
				unescapedURI, err := url.PathUnescape(makeUserFileURIWithQualifiedName(f.cfg.QualifiedTableName, match))
				if err != nil {
					return nil, err
				}
				fileList = append(fileList, unescapedURI)
			}
		}
	}

	return fileList, nil
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
