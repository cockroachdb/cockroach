// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package importer

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"unicode"

	"github.com/cockroachdb/cockroach/pkg/cloud"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/row"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/eval"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/util/ctxgroup"
	"github.com/cockroachdb/errors"
)

type mysqloutfileReader struct {
	importCtx *parallelImportContext
	opts      roachpb.MySQLOutfileOptions
}

var _ inputConverter = &mysqloutfileReader{}

func newMysqloutfileReader(
	semaCtx *tree.SemaContext,
	opts roachpb.MySQLOutfileOptions,
	kvCh chan row.KVBatch,
	walltime int64,
	parallelism int,
	tableDesc catalog.TableDescriptor,
	targetCols tree.NameList,
	evalCtx *eval.Context,
	db *kv.DB,
) (*mysqloutfileReader, error) {
	return &mysqloutfileReader{
		importCtx: &parallelImportContext{
			semaCtx:    semaCtx,
			walltime:   walltime,
			numWorkers: parallelism,
			evalCtx:    evalCtx,
			tableDesc:  tableDesc,
			targetCols: targetCols,
			kvCh:       kvCh,
			db:         db,
		},
		opts: opts,
	}, nil
}

func (d *mysqloutfileReader) start(ctx ctxgroup.Group) {
}

func (d *mysqloutfileReader) readFiles(
	ctx context.Context,
	dataFiles map[int32]string,
	resumePos map[int32]int64,
	format roachpb.IOFileFormat,
	makeExternalStorage cloud.ExternalStorageFactory,
	user username.SQLUsername,
) error {
	return readInputFiles(ctx, dataFiles, resumePos, format, d.readFile, makeExternalStorage, user)
}

type delimitedProducer struct {
	importCtx *parallelImportContext
	opts      *roachpb.MySQLOutfileOptions
	input     *fileReader
	reader    *bufio.Reader
	row       []rune
	err       error
}

var _ importRowProducer = &delimitedProducer{}

// Scan implements importRowProducer
func (d *delimitedProducer) Scan() bool {
	d.row = nil
	var r rune
	var w int
	// gotEncloser represents whether the previous character we scanned is the
	// encloser (the character used for beginning and ending quoted fields).
	var gotEncloser bool
	// inEnclosedField represents whether we have started scanning an enclosed
	// field (more specific than inField).
	var inEnclosedField bool
	// inEscapeSeq represents whether we have started scanning an escape sequence.
	var inEscapeSeq bool
	// inField represents whether we have started scanning a field.
	var inField bool

	for {
		r, w, d.err = d.reader.ReadRune()
		if d.err == io.EOF {
			d.err = nil
			return d.row != nil
		}
		if d.err != nil {
			return false
		}

		if r == unicode.ReplacementChar && w == 1 {
			if d.err = d.reader.UnreadRune(); d.err != nil {
				return false
			}
			var raw byte
			raw, d.err = d.reader.ReadByte()
			if d.err != nil {
				return false
			}
			r = rune(raw)
		}

		if d.opts.Enclose != roachpb.MySQLOutfileOptions_Never {
			// We only care about well-formed, enclosed fields (i.e. fields that
			// start and end with the encloser rune with no additional runes either
			// before or after the field). More precisely: 1) an encloser only
			// starts a field if it is at the start of a row or immediately follows
			// a field terminator and 2) an encloser only ends a field if it is
			// immediately followed by the field terminator rune.
			// We let FillDatums take care of reporting and handling any errors.
			if inEnclosedField && gotEncloser && (r == d.opts.FieldSeparator || r == d.opts.RowSeparator) {
				inEnclosedField = false
			}
			gotEncloser = r == d.opts.Encloser
			if gotEncloser && !inField {
				inEnclosedField = true
			}
		}

		if r == d.opts.RowSeparator && !inEscapeSeq && !inEnclosedField {
			return true
		}

		inField = r != d.opts.FieldSeparator

		if d.opts.HasEscape {
			inEscapeSeq = !inEscapeSeq && r == d.opts.Escape
		}

		d.row = append(d.row, r)
	}
}

// Err implements importRowProducer
func (d *delimitedProducer) Err() error {
	return d.err
}

// Skip implements importRowProducer
func (d *delimitedProducer) Skip() error {
	return nil // no-op
}

// Row implements importRowProducer
func (d *delimitedProducer) Row() (interface{}, error) {
	return d.row, d.err
}

// Progress implements importRowProducer
func (d *delimitedProducer) Progress() float32 {
	return d.input.ReadFraction()
}

type delimitedConsumer struct {
	opts *roachpb.MySQLOutfileOptions
}

var _ importRowConsumer = &delimitedConsumer{}

// FillDatums implements importRowConsumer
func (d *delimitedConsumer) FillDatums(
	ctx context.Context, input interface{}, rowNum int64, conv *row.DatumRowConverter,
) error {
	data := input.([]rune)

	// The current field being read needs to be a list to be able to undo
	// field enclosures at end of field.
	var fieldParts []rune

	// If we have an escaping char defined, seeing it means the next char is to be
	// treated as escaped -- usually that means literal but has some specific
	// mappings defined as well.
	var nextLiteral bool

	// If we have an enclosing char defined, seeing it begins reading a field --
	// which means we do not look for separators until we see the end of the field
	// as indicated by the matching enclosing char.
	var readingField bool

	// If we have just encountered a potential encloser symbol.
	// That means if an end of field or line is next we should honor it.
	var gotEncloser bool

	var gotNull bool

	var datumIdx int

	addField := func() error {
		defer func() {
			fieldParts = fieldParts[:0]
			readingField = false
			gotEncloser = false
		}()
		if nextLiteral {
			return newImportRowError(errors.New("unmatched literal"), string(data), rowNum)
		}

		var datum tree.Datum

		// If previous symbol was field encloser it should be
		// dropped as it only marks end of field. Otherwise
		// throw an error since we don;t expect unmatched encloser.
		if gotEncloser {
			// If the encloser marked end of field
			// drop it.
			if readingField {
				fieldParts = fieldParts[:len(fieldParts)-1]
			} else {
				// Unexpected since we did not see one at start of field.
				gotEncloser = false
				return newImportRowError(errors.New("unmatched field enclosure at end of field"),
					string(data), rowNum)
			}
		} else if readingField {
			return newImportRowError(errors.New("unmatched field enclosure at start of field"),
				string(data), rowNum)
		}
		field := string(fieldParts)
		if datumIdx >= len(conv.VisibleCols) {
			return newImportRowError(
				fmt.Errorf("too many columns, got %d expected %d", datumIdx+1, len(conv.VisibleCols)),
				string(data), rowNum)
		}

		if gotNull {
			gotNull = false
			if len(field) != 0 {
				return newImportRowError(fmt.Errorf("unexpected data after null encoding: %q", field),
					string(data), rowNum)
			}
			datum = tree.DNull
		} else if (!d.opts.HasEscape && field == "NULL") || d.opts.NullEncoding != nil && field == *d.opts.NullEncoding {
			datum = tree.DNull
		} else {
			var err error
			datum, err = mysqlStrToDatum(conv.EvalCtx, field, conv.VisibleColTypes[datumIdx])
			if err != nil {
				col := conv.VisibleCols[datumIdx]
				return newImportRowError(
					errors.Wrapf(err, "error while parse %q as %s", col.GetName(), col.GetType().SQLString()),
					string(data), rowNum)
			}
		}
		conv.Datums[datumIdx] = datum
		datumIdx++
		return nil
	}

	// Main parsing loop body, returns true to indicate unrecoverable error.
	// We are being conservative and treating most errors as unrecoverable for now.
	for _, c := range data {
		// Do we need to check for escaping?
		if d.opts.HasEscape {
			if nextLiteral {
				nextLiteral = false
				// See https://dev.mysql.com/doc/refman/8.0/en/load-data.html.
				switch c {
				case '0':
					fieldParts = append(fieldParts, rune(0))
				case 'b':
					fieldParts = append(fieldParts, rune('\b'))
				case 'n':
					fieldParts = append(fieldParts, rune('\n'))
				case 'r':
					fieldParts = append(fieldParts, rune('\r'))
				case 't':
					fieldParts = append(fieldParts, rune('\t'))
				case 'Z':
					fieldParts = append(fieldParts, rune(byte(26)))
				case 'N':
					if gotNull {
						return newImportRowError(errors.New("unexpected null encoding"), string(data), rowNum)
					}
					gotNull = true
				default:
					fieldParts = append(fieldParts, c)
				}
				gotEncloser = false
				continue
			}

			if c == d.opts.Escape {
				nextLiteral = true
				gotEncloser = false
				continue
			}
		}

		// Are we done with the field, or even the whole row?
		if (!readingField || gotEncloser) && c == d.opts.FieldSeparator {
			if err := addField(); err != nil {
				return err
			}
			continue
		}

		if gotEncloser {
			gotEncloser = false
		}

		// If enclosing is not disabled, check for the encloser.
		// Technically when it is not optional, we could _require_ it to start and
		// end fields, but for the purposes of decoding, we don't actually care --
		// we'll handle it if we see it either way.
		if d.opts.Enclose != roachpb.MySQLOutfileOptions_Never && c == d.opts.Encloser {
			if !readingField && len(fieldParts) == 0 {
				readingField = true
				continue
			}
			gotEncloser = true
		}
		fieldParts = append(fieldParts, c)
	}

	if err := addField(); err != nil {
		return err
	}

	if datumIdx != len(conv.VisibleCols) {
		return newImportRowError(fmt.Errorf(
			"unexpected number of columns, expected %d got %d", len(conv.VisibleCols), datumIdx),
			string(data), rowNum)
	}

	return nil
}

func (d *mysqloutfileReader) readFile(
	ctx context.Context, input *fileReader, inputIdx int32, resumePos int64, rejected chan string,
) error {
	producer := &delimitedProducer{
		importCtx: d.importCtx,
		opts:      &d.opts,
		input:     input,
		reader:    bufio.NewReaderSize(input, 64*1024),
	}
	consumer := &delimitedConsumer{opts: &d.opts}

	if resumePos < int64(d.opts.Skip) {
		resumePos = int64(d.opts.Skip)
	}

	fileCtx := &importFileContext{
		source:   inputIdx,
		skip:     resumePos,
		rejected: rejected,
		rowLimit: d.opts.RowLimit,
	}

	return runParallelImport(ctx, d.importCtx, fileCtx, producer, consumer)
}
