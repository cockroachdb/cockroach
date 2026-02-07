// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package scbuildstmt

import (
	"fmt"
	"strconv"

	"github.com/cockroachdb/cockroach/pkg/sql/catalog/schemaexpr"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/privilege"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scpb"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/errorutil/unimplemented"
	"github.com/cockroachdb/cockroach/pkg/util/log/eventpb"
	"github.com/cockroachdb/errors"
)

// AlterSequence implements ALTER SEQUENCE.
func AlterSequence(b BuildCtx, n *tree.AlterSequence) {
	elts := b.ResolveSequence(n.Name, ResolveParams{
		IsExistenceOptional: n.IfExists,
		RequiredPrivilege:   privilege.CREATE,
	})
	seq := elts.FilterSequence().MustGetZeroOrOneElement()
	if seq == nil {
		tn := n.Name.ToTableName()
		b.MarkNameAsNonExistent(&tn)
		return
	}
	// Annotate the AST with the fully resolved name.
	tn := n.Name.ToTableName()
	tn.ObjectNamePrefix = b.NamePrefix(seq)
	b.SetUnresolvedNameAnnotation(n.Name, &tn)
	b.IncrementSchemaChangeAlterCounter("sequence")

	sequenceID := seq.SequenceID

	// Handle OWNED BY separately, since AssignSequenceOptions does not handle it.
	// SequenceOwner elements are stored on the owning table's descriptor, so we
	// find them via back-references from the sequence.
	for _, opt := range n.Options {
		if opt.Name == tree.SeqOptOwnedBy {
			// Drop the existing SequenceOwner element for this sequence.
			// We filter by SequenceID because BackReferences returns all
			// elements from the owning table, which may own other sequences.
			b.BackReferences(sequenceID).FilterSequenceOwner().Filter(
				func(_ scpb.Status, _ scpb.TargetStatus, e *scpb.SequenceOwner) bool {
					return e.SequenceID == sequenceID
				},
			).ForEach(
				func(_ scpb.Status, _ scpb.TargetStatus, e *scpb.SequenceOwner) {
					b.Drop(e)
				},
			)
			if opt.ColumnItemVal != nil {
				// OWNED BY table.column: add a new owner.
				seqNamespace := b.QueryByID(sequenceID).FilterNamespace().MustGetOneElement()
				maybeAssignSequenceOwner(b, seqNamespace, opt.ColumnItemVal)
			}
			// If ColumnItemVal is nil, this is OWNED BY NONE — dropping is sufficient.
		}
	}

	// Get the current sequence options from elements and defaults.
	currentOpts := schemaexpr.DefaultSequenceOptions()
	b.QueryByID(sequenceID).FilterSequenceOption().ForEach(
		func(_ scpb.Status, _ scpb.TargetStatus, e *scpb.SequenceOption) {
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
		},
	)

	// Determine the existing integer type for proper bounds adjustment when
	// the AS type changes.
	existingType := types.Int
	if currentOpts.AsIntegerType != "" {
		switch currentOpts.AsIntegerType {
		case types.Int2.SQLString():
			existingType = types.Int2
		case types.Int4.SQLString():
			existingType = types.Int4
		case types.Int.SQLString():
			existingType = types.Int
		default:
			panic(errors.AssertionFailedf(
				"sequence has unexpected type %s", currentOpts.AsIntegerType,
			))
		}
	}

	// Compute the updated sequence options.
	updatedOpts := currentOpts
	if err := schemaexpr.AssignSequenceOptions(
		&updatedOpts,
		n.Options,
		b.SessionData().DefaultIntSize,
		false,        /* setDefaults */
		existingType, /* existingType */
	); err != nil {
		panic(pgerror.Wrap(err, pgcode.FeatureNotSupported, ""))
	}

	defaultOpts := schemaexpr.DefaultSequenceOptions()

	// alteredElement tracks a SequenceOption that was modified (added or
	// dropped) during this ALTER SEQUENCE, used for event logging.
	var alteredElement scpb.Element

	// updateElement adds or drops elements to effect the changed option.
	// isDefault indicates whether the new value equals the sequence default,
	// in which case no element is added (defaults are implicit).
	// Returns true if a sequence value update may be needed.
	updateElement := func(key, value string, isDefault bool) bool {
		newSeqOption := scpb.SequenceOption{
			SequenceID: sequenceID,
			Key:        key,
			Value:      value,
		}

		oldSeqOption := b.QueryByID(sequenceID).FilterSequenceOption().Filter(
			func(_ scpb.Status, _ scpb.TargetStatus, e *scpb.SequenceOption) bool {
				return e.Key == key
			},
		).MustGetZeroOrOneElement()
		if oldSeqOption != nil {
			if oldSeqOption.Value == newSeqOption.Value {
				return false
			}
			b.Drop(oldSeqOption)
			if alteredElement == nil {
				alteredElement = oldSeqOption
			}
		}

		// Skip setting to default values.
		if isDefault {
			return true
		}

		b.Add(&newSeqOption)
		if alteredElement == nil {
			alteredElement = &newSeqOption
		}
		return true
	}

	fmtInt := func(v int64) string { return strconv.FormatInt(v, 10) }
	fmtBool := func(v bool) string { return strconv.FormatBool(v) }

	var updateSequenceValue bool
	var restartWith *int64
	optionsSeen := make(map[string]bool)
	for _, opt := range n.Options {
		optionsSeen[opt.Name] = true
		switch name := opt.Name; name {
		case tree.SeqOptAs:
			_ = updateElement(name, updatedOpts.AsIntegerType, updatedOpts.AsIntegerType == defaultOpts.AsIntegerType)
		case tree.SeqOptCycle:
			panic(unimplemented.NewWithIssue(20961, "CYCLE option is not supported"))
		case tree.SeqOptNoCycle:
			// Noop for a default that can't be modified.
		case tree.SeqOptCacheNode:
			_ = updateElement(name, fmtInt(updatedOpts.NodeCacheSize), updatedOpts.NodeCacheSize == defaultOpts.NodeCacheSize)
		case tree.SeqOptCacheSession:
			_ = updateElement(name, fmtInt(updatedOpts.SessionCacheSize), updatedOpts.SessionCacheSize == defaultOpts.SessionCacheSize)
		case tree.SeqOptIncrement:
			updated := updateElement(name, fmtInt(updatedOpts.Increment), updatedOpts.Increment == defaultOpts.Increment)
			updateSequenceValue = updateSequenceValue || updated
		case tree.SeqOptMinValue:
			updated := updateElement(name, fmtInt(updatedOpts.MinValue), updatedOpts.MinValue == defaultOpts.MinValue)
			updateSequenceValue = updateSequenceValue || updated
		case tree.SeqOptMaxValue:
			updated := updateElement(name, fmtInt(updatedOpts.MaxValue), updatedOpts.MaxValue == defaultOpts.MaxValue)
			updateSequenceValue = updateSequenceValue || updated
		case tree.SeqOptStart:
			updated := updateElement(name, fmtInt(updatedOpts.Start), updatedOpts.Start == defaultOpts.Start)
			updateSequenceValue = updateSequenceValue || updated
		case tree.SeqOptRestart:
			if opt.IntVal != nil {
				restartWith = opt.IntVal
			} else {
				restartWith = &updatedOpts.Start
			}
		case tree.SeqOptVirtual:
			_ = updateElement(name, fmtBool(updatedOpts.Virtual), updatedOpts.Virtual == defaultOpts.Virtual)
		case tree.SeqOptOwnedBy:
			// Already handled above.
		default:
			panic(fmt.Sprintf("unexpected sequence option: %q", name))
		}
	}

	// Handle implicit changes from AS type changes. AssignSequenceOptions may
	// have adjusted MinValue/MaxValue/Start even if the user didn't explicitly
	// set them (e.g., when changing AS INT2 to AS INT4, the bounds expand).
	// Only apply if the option wasn't already handled in the loop above.
	if !optionsSeen[tree.SeqOptMinValue] && currentOpts.MinValue != updatedOpts.MinValue {
		updated := updateElement(tree.SeqOptMinValue, fmtInt(updatedOpts.MinValue), updatedOpts.MinValue == defaultOpts.MinValue)
		updateSequenceValue = updateSequenceValue || updated
	}
	if !optionsSeen[tree.SeqOptMaxValue] && currentOpts.MaxValue != updatedOpts.MaxValue {
		updated := updateElement(tree.SeqOptMaxValue, fmtInt(updatedOpts.MaxValue), updatedOpts.MaxValue == defaultOpts.MaxValue)
		updateSequenceValue = updateSequenceValue || updated
	}
	if !optionsSeen[tree.SeqOptStart] && currentOpts.Start != updatedOpts.Start {
		updated := updateElement(tree.SeqOptStart, fmtInt(updatedOpts.Start), updatedOpts.Start == defaultOpts.Start)
		updateSequenceValue = updateSequenceValue || updated
	}

	if restartWith != nil {
		restartSeqOption := scpb.SequenceOption{
			SequenceID: sequenceID,
			Key:        tree.SeqOptRestart,
			Value:      fmtInt((*restartWith) - updatedOpts.Increment),
		}
		b.AddTransient(&restartSeqOption)
	} else if updateSequenceValue {
		seqValue := scpb.SequenceValue{
			SequenceID:       sequenceID,
			PrevIncrement:    currentOpts.Increment,
			UpdatedIncrement: updatedOpts.Increment,
			PrevMinValue:     currentOpts.MinValue,
			UpdatedMinValue:  updatedOpts.MinValue,
			PrevMaxValue:     currentOpts.MaxValue,
			UpdatedMaxValue:  updatedOpts.MaxValue,
			PrevStart:        currentOpts.Start,
			UpdatedStart:     updatedOpts.Start,
		}
		b.AddTransient(&seqValue)
	}

	// Log the alter_sequence event. We log on a modified child element
	// (SequenceOption) rather than the Sequence element itself, because
	// the Sequence element wasn't directly modified (b.Add/b.Drop) and
	// thus wouldn't be retained in the schema change output. This follows
	// the same pattern as ALTER TABLE, which logs on child elements.
	if alteredElement != nil {
		b.LogEventForExistingPayload(alteredElement, &eventpb.AlterSequence{
			SequenceName: tn.FQString(),
		})
	}
}
