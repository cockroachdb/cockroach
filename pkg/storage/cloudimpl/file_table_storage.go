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

type fileTableStorage struct {
	fs     *filetable.FileToTableSystem
	cfg    roachpb.ExternalStorage_FileTable
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
	fileToTableSystem, err := filetable.NewFileToTableSystem(ctx, cfg.QualifiedTableName, ie, db,
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
	path = strings.TrimPrefix(path, "/")
	return fmt.Sprintf("userfile://%s/%s", qualifiedTableName, path)
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

// ReadFile implements the ExternalStorage interface and returns the contents of
// the file stored in the user scoped FileToTableSystem.
func (f *fileTableStorage) ReadFile(ctx context.Context, basename string) (io.ReadCloser, error) {
	reader, err := f.fs.ReadFile(ctx, path.Join(f.prefix, basename))
	if os.IsNotExist(err) {
		return nil, errors.Wrapf(ErrFileDoesNotExist,
			"file %s does not exist in the UserFileTableSystem", path.Join(f.prefix, basename))
	}

	return reader, err
}

// WriteFile implements the ExternalStorage interface and writes the file to the
// user scoped FileToTableSystem.
func (f *fileTableStorage) WriteFile(
	ctx context.Context, basename string, content io.ReadSeeker,
) error {
	writer, err := f.fs.NewFileWriter(ctx, path.Join(f.prefix, basename), filetable.ChunkDefaultSize)
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
}

// ListFiles implements the ExternalStorage interface and lists the files stored
// in the user scoped FileToTableSystem.
func (f *fileTableStorage) ListFiles(ctx context.Context, patternSuffix string) ([]string, error) {
	var fileList []string
	matches, err := f.fs.ListFiles(ctx, getPrefixBeforeWildcard(f.prefix))
	if err != nil {
		return nil, errors.Wrap(err, "unable to match pattern provided")
	}

	pattern := f.prefix
	if patternSuffix != "" {
		if containsGlob(f.prefix) {
			return nil, errors.New("prefix cannot contain globs pattern when passing an explicit pattern")
		}
		pattern = path.Join(pattern, patternSuffix)
	}

	for _, match := range matches {
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
				fileList = append(fileList, makeUserFileURIWithQualifiedName(f.cfg.QualifiedTableName,
					match))
			}
		}
	}

	return fileList, nil
}

// Delete implements the ExternalStorage interface and deletes the file from the
// user scoped FileToTableSystem.
func (f *fileTableStorage) Delete(ctx context.Context, basename string) error {
	return f.fs.DeleteFile(ctx, path.Join(f.prefix, basename))
}

// Size implements the ExternalStorage interface and returns the size of the
// file stored in the user scoped FileToTableSystem.
func (f *fileTableStorage) Size(ctx context.Context, basename string) (int64, error) {
	return f.fs.FileSize(ctx, path.Join(f.prefix, basename))
}
