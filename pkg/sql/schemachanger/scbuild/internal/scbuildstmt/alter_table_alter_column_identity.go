// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package scbuildstmt

import (
	"fmt"
	"reflect"
	"strconv"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/sql/catalog/schemaexpr"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scpb"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
)

func alterTableAlterColumnIdentity(
	b BuildCtx, tn *tree.TableName, tbl *scpb.Table, stmt tree.Statement, t *tree.AlterTableIdentity,
) {
	alterColumnPreChecks(b, tn, tbl, t.Column)

	columnID := getColumnIDFromColumnName(b, tbl.TableID, t.Column, true /* required */)
	// Block alters on system columns.
	column := mustRetrieveColumnElem(b, tbl.TableID, columnID)
	panicIfSystemColumn(column, t.Column)

	sequenceOwner := b.QueryByID(tbl.TableID).FilterSequenceOwner().Filter(func(_ scpb.Status, _ scpb.TargetStatus, e *scpb.SequenceOwner) bool {
		return e.TableID == tbl.TableID && e.ColumnID == columnID
	}).MustGetZeroOrOneElement()
	if sequenceOwner == nil {
		panic(pgerror.Newf(
			pgcode.ObjectNotInPrerequisiteState,
			"column %q of relation %q is not an identity column", tree.ErrString(&t.Column), tree.ErrString(&tn.ObjectName),
		))
	}

	// Get the current sequence options from elements and defaults.
	currentOpts := schemaexpr.DefaultSequenceOptions()
	b.QueryByID(sequenceOwner.SequenceID).FilterSequenceOption().ForEach(func(_ scpb.Status, _ scpb.TargetStatus, e *scpb.SequenceOption) {
		var err error
		switch name := e.Key; name {
		case tree.SeqOptAs:
			currentOpts.AsIntegerType = e.Value
		case tree.SeqOptCacheNode:
			currentOpts.NodeCacheSize, err = strconv.ParseInt(e.Value, 10, 64)
			if err != nil {
				panic(pgerror.Wrapf(err, pgcode.Internal, "invalid sequence option value for %q", name))
			}
		case tree.SeqOptCacheSession:
			currentOpts.SessionCacheSize, err = strconv.ParseInt(e.Value, 10, 64)
			if err != nil {
				panic(pgerror.Wrapf(err, pgcode.Internal, "invalid sequence option value for %q", name))
			}
		case tree.SeqOptIncrement:
			currentOpts.Increment, err = strconv.ParseInt(e.Value, 10, 64)
			if err != nil {
				panic(pgerror.Wrapf(err, pgcode.Internal, "invalid sequence option value for %q", name))
			}
		case tree.SeqOptMinValue:
			currentOpts.MinValue, err = strconv.ParseInt(e.Value, 10, 64)
			if err != nil {
				panic(pgerror.Wrapf(err, pgcode.Internal, "invalid sequence option value for %q", name))
			}
		case tree.SeqOptMaxValue:
			currentOpts.MaxValue, err = strconv.ParseInt(e.Value, 10, 64)
			if err != nil {
				panic(pgerror.Wrapf(err, pgcode.Internal, "invalid sequence option value for %q", name))
			}
		case tree.SeqOptStart:
			currentOpts.Start, err = strconv.ParseInt(e.Value, 10, 64)
			if err != nil {
				panic(pgerror.Wrapf(err, pgcode.Internal, "invalid sequence option value for %q", name))
			}
		case tree.SeqOptVirtual:
			currentOpts.Virtual, err = strconv.ParseBool(e.Value)
			if err != nil {
				panic(pgerror.Wrapf(err, pgcode.Internal, "invalid sequence option value for %q", name))
			}
		default:
			panic(pgerror.Newf(pgcode.Internal, "unexpected sequence option %q", name))
		}
	})

	// And the final state for the sequence options.
	updatedOpts := currentOpts
	if err := schemaexpr.AssignSequenceOptions(&updatedOpts,
		t.SeqOptions,
		64,
		false, /* setDefaults */
		nil,   /* existingTypes */
	); err != nil {
		panic(pgerror.Wrap(
			err,
			pgcode.FeatureNotSupported,
			"", /* message */
		))
	}

	defaultOpts := schemaexpr.DefaultSequenceOptions()

	updateElement := func(key string, defaultValue, value interface{}) {
		newSeqOption := scpb.SequenceOption{
			SequenceID: sequenceOwner.SequenceID,
			Key:        key,
			Value:      fmt.Sprintf("%v", value),
		}

		oldSeqOption := b.QueryByID(sequenceOwner.SequenceID).FilterSequenceOption().Filter(func(_ scpb.Status, _ scpb.TargetStatus, e *scpb.SequenceOption) bool {
			return e.Key == key
		}).MustGetZeroOrOneElement()
		if oldSeqOption != nil {
			// Skip a noop update.
			if oldSeqOption.Value == newSeqOption.Value {
				return
			}
			b.Drop(oldSeqOption)
		}

		// Skip setting to default values.
		if reflect.DeepEqual(defaultValue, value) {
			return
		}

		b.Add(&newSeqOption)
	}

	var updateSequenceValue bool
	var restartWith int64
	var useRestartWith bool
	for _, opt := range t.SeqOptions {
		switch name := opt.Name; name {
		case tree.SeqOptAs:
			updateElement(name, defaultOpts.AsIntegerType, updatedOpts.AsIntegerType)
		case tree.SeqOptCacheNode:
			updateElement(name, defaultOpts.NodeCacheSize, updatedOpts.NodeCacheSize)
		case tree.SeqOptCacheSession:
			updateElement(name, defaultOpts.SessionCacheSize, updatedOpts.SessionCacheSize)
		case tree.SeqOptIncrement:
			updateElement(name, defaultOpts.Increment, updatedOpts.Increment)
			updateSequenceValue = true
		case tree.SeqOptMinValue:
			updateElement(name, defaultOpts.MinValue, updatedOpts.MinValue)
			updateSequenceValue = true
		case tree.SeqOptMaxValue:
			updateElement(name, defaultOpts.MaxValue, updatedOpts.MaxValue)
			updateSequenceValue = true
		case tree.SeqOptStart:
			updateElement(name, defaultOpts.Start, updatedOpts.Start)
			updateSequenceValue = true
		case tree.SeqOptVirtual:
			updateElement(name, defaultOpts.Virtual, updatedOpts.Virtual)
		case tree.SeqOptRestart:
			useRestartWith = true
			if opt.IntVal != nil {
				restartWith = *opt.IntVal
			} else {
				restartWith = updatedOpts.Start
			}
		default:
			panic(fmt.Sprintf("unexpected sequence option: %q", name))
		}
	}

	if useRestartWith {
		restartSeqOption := scpb.SequenceOption{
			SequenceID: sequenceOwner.SequenceID,
			Key:        tree.SeqOptRestart,
			Value:      fmt.Sprintf("%v", restartWith-updatedOpts.Increment),
		}
		b.AddTransient(&restartSeqOption)
	} else if updateSequenceValue {
		// TODO(21564): Update the sequence value when changes in options expose implementation.
	}

	var seqOptions tree.SequenceOptions
	if updatedOpts.SessionCacheSize != defaultOpts.SessionCacheSize {
		seqOptions = append(seqOptions, tree.SequenceOption{Name: tree.SeqOptCacheSession, IntVal: &updatedOpts.SessionCacheSize})
	}
	if updatedOpts.NodeCacheSize != defaultOpts.NodeCacheSize {
		seqOptions = append(seqOptions, tree.SequenceOption{Name: tree.SeqOptCacheNode, IntVal: &updatedOpts.NodeCacheSize})
	}
	if updatedOpts.MinValue != defaultOpts.MinValue {
		seqOptions = append(seqOptions, tree.SequenceOption{Name: tree.SeqOptMinValue, IntVal: &updatedOpts.MinValue})
	}
	if updatedOpts.MaxValue != defaultOpts.MaxValue {
		seqOptions = append(seqOptions, tree.SequenceOption{Name: tree.SeqOptMaxValue, IntVal: &updatedOpts.MaxValue})
	}
	if updatedOpts.Increment != defaultOpts.Increment {
		seqOptions = append(seqOptions, tree.SequenceOption{Name: tree.SeqOptIncrement, IntVal: &updatedOpts.Increment})
	}
	if updatedOpts.Start != defaultOpts.Start {
		seqOptions = append(seqOptions, tree.SequenceOption{Name: tree.SeqOptStart, IntVal: &updatedOpts.Start})
	}
	if updatedOpts.Virtual != defaultOpts.Virtual {
		seqOptions = append(seqOptions, tree.SequenceOption{Name: tree.SeqOptVirtual})
	}
	seqOptionsString := strings.TrimSpace(tree.Serialize(&seqOptions))

	columnType := b.QueryByID(tbl.TableID).FilterColumnType().Filter(func(_ scpb.Status, _ scpb.TargetStatus, e *scpb.ColumnType) bool {
		return e.ColumnID == sequenceOwner.ColumnID
	}).MustGetOneElement()

	if columnType.ElementCreationMetadata.In_26_1OrLater {
		oldColumnGeneratedAsIdentity := b.QueryByID(tbl.TableID).FilterColumnGeneratedAsIdentity().Filter(func(_ scpb.Status, _ scpb.TargetStatus, e *scpb.ColumnGeneratedAsIdentity) bool {
			return e.ColumnID == sequenceOwner.ColumnID
		}).MustGetOneElement()

		if oldColumnGeneratedAsIdentity.SequenceOption != seqOptionsString {
			b.Drop(oldColumnGeneratedAsIdentity)
			newColumnGeneratedAsIdentity := *oldColumnGeneratedAsIdentity
			newColumnGeneratedAsIdentity.SequenceOption = seqOptionsString
			b.Add(&newColumnGeneratedAsIdentity)
		}
	} else {
		if column.GeneratedAsIdentitySequenceOption != seqOptionsString {
			b.Drop(column)
			newColumn := *column
			newColumn.GeneratedAsIdentitySequenceOption = seqOptionsString
			b.Add(&newColumn)
		}
	}
}
