// Copyright 2017 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package importccl

import (
	"bytes"
	"compress/bzip2"
	"compress/gzip"
	"context"
	"io"
	"io/ioutil"
	"net/url"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfra"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/row"
	"github.com/cockroachdb/cockroach/pkg/storage/cloud"
	"github.com/cockroachdb/cockroach/pkg/util/ctxgroup"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/cockroachdb/errors"
)

func runImport(
	ctx context.Context,
	flowCtx *execinfra.FlowCtx,
	spec *execinfrapb.ReadImportDataSpec,
	kvCh chan row.KVBatch,
) error {
	group := ctxgroup.WithContext(ctx)

	conv, err := makeInputConverter(spec, flowCtx.NewEvalCtx(), kvCh)
	if err != nil {
		return err
	}
	conv.start(group)

	// Read input files into kvs
	group.GoCtx(func(ctx context.Context) error {
		ctx, span := tracing.ChildSpan(ctx, "readImportFiles")
		defer tracing.FinishSpan(span)
		defer conv.inputFinished(ctx)
		return conv.readFiles(ctx, spec.Uri, spec.Format, flowCtx.Cfg.ExternalStorage)
	})

	return group.Wait()
}

type readFileFunc func(context.Context, *fileReader, int32, string, chan string) error

// readInputFile reads each of the passed dataFiles using the passed func. The
// key part of dataFiles is the unique index of the data file among all files in
// the IMPORT. progressFn, if not nil, is periodically invoked with a percentage
// of the total progress of reading through all of the files. This percentage
// attempts to use the Size() method of ExternalStorage to determine how many
// bytes must be read of the input files, and reports the percent of bytes read
// among all dataFiles. If any Size() fails for any file, then progress is
// reported only after each file has been read.
func readInputFiles(
	ctx context.Context,
	dataFiles map[int32]string,
	format roachpb.IOFileFormat,
	fileFunc readFileFunc,
	makeExternalStorage cloud.ExternalStorageFactory,
) error {
	done := ctx.Done()

	fileSizes := make(map[int32]int64, len(dataFiles))
	// Attempt to fetch total number of bytes for all files.
	for id, dataFile := range dataFiles {
		conf, err := cloud.ExternalStorageConfFromURI(dataFile)
		if err != nil {
			return err
		}
		es, err := makeExternalStorage(ctx, conf)
		if err != nil {
			return err
		}
		sz, err := es.Size(ctx, "")
		es.Close()
		if sz <= 0 {
			// Don't log dataFile here because it could leak auth information.
			log.Infof(ctx, "could not fetch file size; falling back to per-file progress: %v", err)
			break
		}
		fileSizes[id] = sz
	}

	currentFile := 0
	for dataFileIndex, dataFile := range dataFiles {
		currentFile++
		select {
		case <-done:
			return ctx.Err()
		default:
		}
		if err := func() error {
			conf, err := cloud.ExternalStorageConfFromURI(dataFile)
			if err != nil {
				return err
			}
			es, err := makeExternalStorage(ctx, conf)
			if err != nil {
				return err
			}
			defer es.Close()
			raw, err := es.ReadFile(ctx, "")
			if err != nil {
				return err
			}
			defer raw.Close()

			src := &fileReader{total: fileSizes[dataFileIndex], counter: byteCounter{r: raw}}
			decompressed, err := decompressingReader(&src.counter, dataFile, format.Compression)
			if err != nil {
				return err
			}
			defer decompressed.Close()
			src.Reader = decompressed

			var rejected chan string
			if format.Format == roachpb.IOFileFormat_MysqlOutfile && format.MysqlOut.SaveRejected {
				rejected = make(chan string)
			}
			if rejected != nil {
				grp := ctxgroup.WithContext(ctx)
				grp.GoCtx(func(ctx context.Context) error {
					var buf []byte
					atFirstLine := true
					for s := range rejected {
						buf = append(buf, s...)
						atFirstLine = false
					}
					if atFirstLine {
						// no rejected rows
						return nil
					}
					rejectedFile := dataFile
					parsedURI, err := url.Parse(rejectedFile)
					if err != nil {
						return err
					}
					parsedURI.Path = parsedURI.Path + ".rejected"
					conf, err := cloud.ExternalStorageConfFromURI(rejectedFile)
					if err != nil {
						return err
					}
					rejectedStorage, err := makeExternalStorage(ctx, conf)
					if err != nil {
						return err
					}
					defer rejectedStorage.Close()
					if err := rejectedStorage.WriteFile(ctx, "", bytes.NewReader(buf)); err != nil {
						return err
					}
					return nil
				})

				grp.GoCtx(func(ctx context.Context) error {
					defer close(rejected)
					if err := fileFunc(ctx, src, dataFileIndex, dataFile, rejected); err != nil {
						return errors.Wrap(err, dataFile)
					}
					return nil
				})

				if err := grp.Wait(); err != nil {
					return errors.Wrap(err, dataFile)
				}
			} else {
				if err := fileFunc(ctx, src, dataFileIndex, dataFile, rejected); err != nil {
					return errors.Wrap(err, dataFile)
				}
			}
			return nil
		}(); err != nil {
			return err
		}
	}
	return nil
}

func decompressingReader(
	in io.Reader, name string, hint roachpb.IOFileFormat_Compression,
) (io.ReadCloser, error) {
	switch guessCompressionFromName(name, hint) {
	case roachpb.IOFileFormat_Gzip:
		return gzip.NewReader(in)
	case roachpb.IOFileFormat_Bzip:
		return ioutil.NopCloser(bzip2.NewReader(in)), nil
	default:
		return ioutil.NopCloser(in), nil
	}
}

func guessCompressionFromName(
	name string, hint roachpb.IOFileFormat_Compression,
) roachpb.IOFileFormat_Compression {
	if hint != roachpb.IOFileFormat_Auto {
		return hint
	}
	switch {
	case strings.HasSuffix(name, ".gz"):
		return roachpb.IOFileFormat_Gzip
	case strings.HasSuffix(name, ".bz2") || strings.HasSuffix(name, ".bz"):
		return roachpb.IOFileFormat_Bzip
	default:
		if parsed, err := url.Parse(name); err == nil && parsed.Path != name {
			return guessCompressionFromName(parsed.Path, hint)
		}
		return roachpb.IOFileFormat_None
	}
}

type byteCounter struct {
	r io.Reader
	n int64
}

func (b *byteCounter) Read(p []byte) (int, error) {
	n, err := b.r.Read(p)
	b.n += int64(n)
	return n, err
}

type fileReader struct {
	io.Reader
	total   int64
	counter byteCounter
}

func (f fileReader) ReadFraction() float32 {
	if f.total == 0 {
		return 0.0
	}
	return float32(f.counter.n) / float32(f.total)
}

type inputConverter interface {
	start(group ctxgroup.Group)
	readFiles(
		ctx context.Context,
		dataFiles map[int32]string,
		format roachpb.IOFileFormat,
		makeExternalStorage cloud.ExternalStorageFactory,
	) error
	inputFinished(ctx context.Context)
}

func isMultiTableFormat(format roachpb.IOFileFormat_FileFormat) bool {
	switch format {
	case roachpb.IOFileFormat_Mysqldump,
		roachpb.IOFileFormat_PgDump:
		return true
	}
	return false
}

func makeRowErr(file string, row int64, code, format string, args ...interface{}) error {
	return pgerror.NewWithDepthf(1, code,
		"%q: row %d: "+format, append([]interface{}{file, row}, args...)...)
}

func wrapRowErr(err error, file string, row int64, code, format string, args ...interface{}) error {
	if format != "" || len(args) > 0 {
		err = errors.WrapWithDepthf(1, err, format, args...)
	}
	err = errors.WrapWithDepthf(1, err, "%q: row %d", file, row)
	if code != pgcode.Uncategorized {
		err = pgerror.WithCandidateCode(err, code)
	}
	return err
}
