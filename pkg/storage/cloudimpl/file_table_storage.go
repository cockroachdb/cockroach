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
	"database/sql/driver"
	"fmt"
	"io"
	"net/url"
	"os"
	"path"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/storage/cloud"
	"github.com/cockroachdb/cockroach/pkg/storage/cloudimpl/filetable"
	"github.com/cockroachdb/errors"
)

const defaultUserfileScheme = "userfile"

type fileTableStorage struct {
	fs     *filetable.FileToTableSystem
	cfg    roachpb.ExternalStorage_FileTable
	db     *kv.DB
	prefix string // relative filepath
}

var _ cloud.ExternalStorage = &fileTableStorage{}

func makeFileTableStorage(
	ctx context.Context, cfg roachpb.ExternalStorage_FileTable, ie *sql.InternalExecutor, db *kv.DB,
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
		return nil, errors.Newf("path %s changes after normalization to %s. "+
			"userfile upload does not permit such path constructs",
			cfg.Path, path.Clean(cfg.Path))
	}

	executor := filetable.MakeInternalFileToTableExecutor(ie, db)
	fileToTableSystem, err := filetable.NewFileToTableSystem(ctx, cfg.QualifiedTableName, executor,
		cfg.User)
	if err != nil {
		return nil, err
	}
	return &fileTableStorage{
		fs:     fileToTableSystem,
		cfg:    cfg,
		db:     db,
		prefix: cfg.Path,
	}, nil
}

// MakeSQLConnFileTableStorage returns an instance of a FileTableStorage which
// uses a network connection backed SQL executor. This is used by the CLI to
// interact with the underlying FileToTableSystem. It only supports a subset of
// methods compared to the internal SQL connection backed FileTableStorage.
func MakeSQLConnFileTableStorage(
	ctx context.Context, cfg roachpb.ExternalStorage_FileTable, conn driver.QueryerContext,
) (cloud.ExternalStorage, error) {
	executor := filetable.MakeSQLConnFileToTableExecutor(conn)
	fileToTableSystem, err := filetable.NewFileToTableSystem(ctx, cfg.QualifiedTableName, executor,
		cfg.User)
	if err != nil {
		return nil, err
	}
	return &fileTableStorage{
		fs:     fileToTableSystem,
		cfg:    cfg,
		prefix: cfg.Path,
	}, nil
}

// MakeUserFileStorageURI converts a qualified table name and filename
// to a valid userfile URI.
func MakeUserFileStorageURI(qualifiedTableName, filename string) string {
	return fmt.Sprintf("userfile://%s/%s", qualifiedTableName, filename)
}

func makeUserFileURIWithQualifiedName(qualifiedTableName, path string) string {
	userfileURL := url.URL{
		Scheme: defaultUserfileScheme,
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

// ReadFile implements the ExternalStorage interface and returns the contents of
// the file stored in the user scoped FileToTableSystem.
func (f *fileTableStorage) ReadFile(ctx context.Context, basename string) (io.ReadCloser, error) {
	filepath, err := checkBaseAndJoinFilePath(f.prefix, basename)
	if err != nil {
		return nil, err
	}
	reader, err := f.fs.ReadFile(ctx, filepath)
	if os.IsNotExist(err) {
		return nil, errors.Wrapf(ErrFileDoesNotExist,
			"file %s does not exist in the UserFileTableSystem", filepath)
	}

	return reader, err
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
	err = f.db.Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
		writer, err := f.fs.NewFileWriter(ctx, filepath, filetable.ChunkDefaultSize, txn)
		if err != nil {
			return err
		}

		if _, err = io.Copy(writer, content); err != nil {
			return errors.Wrap(err, "failed to write using the FileTable writer")
		}

		if err := writer.Close(); err != nil {
			return errors.Wrap(err, "failed to close the FileTable writer")
		}

		return nil
	})

	return err
}

// This method is different from the utility method getPrefixBeforeWildcard() in
// external_storage.go in that it does not invoke path.Dir on the return value.
func getPrefixBeforeWildcardForFileTable(p string) string {
	globIndex := strings.IndexAny(p, "*?[")
	if globIndex < 0 {
		return p
	}
	return p[:globIndex]
}

// ListFiles implements the ExternalStorage interface and lists the files stored
// in the user scoped FileToTableSystem.
func (f *fileTableStorage) ListFiles(ctx context.Context, patternSuffix string) ([]string, error) {
	var fileList []string
	matches, err := f.fs.ListFiles(ctx, getPrefixBeforeWildcardForFileTable(f.prefix))
	if err != nil {
		return nil, errors.Wrap(err, "unable to match pattern provided")
	}

	pattern := f.prefix
	if patternSuffix != "" {
		if containsGlob(f.prefix) {
			return nil, errors.New("prefix cannot contain globs pattern when passing an explicit pattern")
		}
		pattern, err = checkBaseAndJoinFilePath(pattern, patternSuffix)
		if err != nil {
			return nil, err
		}
	}

	for _, match := range matches {
		// If there is no glob pattern, then the user wishes to list all the uploaded
		// files stored in the userfile table storage.
		if f.prefix == "" {
			match = strings.TrimPrefix(match, "/")
			unescapedURI, err := url.PathUnescape(makeUserFileURIWithQualifiedName(f.cfg.
				QualifiedTableName, match))
			if err != nil {
				return nil, err
			}
			fileList = append(fileList, unescapedURI)
			continue
		}

		doesMatch, matchErr := path.Match(pattern, match)
		if matchErr != nil {
			continue
		}

		if doesMatch {
			if patternSuffix != "" {
				if !strings.HasPrefix(match, f.prefix) {
					return nil, errors.New("pattern matched file outside of path")
				}
				fileList = append(fileList, strings.TrimPrefix(strings.TrimPrefix(match, f.prefix),
					"/"))
			} else {
				match = strings.TrimPrefix(match, "/")
				unescapedURI, err := url.PathUnescape(makeUserFileURIWithQualifiedName(f.cfg.
					QualifiedTableName, match))
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
