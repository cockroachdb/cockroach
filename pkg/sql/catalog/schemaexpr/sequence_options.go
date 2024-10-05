// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package schemaexpr

import (
	"math"

	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/errorutil/unimplemented"
	"github.com/cockroachdb/errors"
)

// ParseSequenceOpts is to transform the sequence options saved the
// descriptor to a descpb.TableDescriptor_SequenceOpts.
// Note that this function is used to acquire the sequence option for the
// information schema table, so it doesn't parse for the sequence owner info.
func ParseSequenceOpts(
	s string, defaultIntSize int32,
) (*descpb.TableDescriptor_SequenceOpts, error) {
	stmt, err := parser.ParseOne("CREATE SEQUENCE fake_seq " + s)
	if err != nil {
		return nil, errors.Wrap(err, "cannot parse sequence option")
	}

	createSeqNode, ok := stmt.AST.(*tree.CreateSequence)
	if !ok {
		return nil, errors.New("cannot convert parsed result to tree.CreateSequence")
	}

	opts := &descpb.TableDescriptor_SequenceOpts{
		Increment: 1,
	}
	if err := AssignSequenceOptions(
		opts,
		createSeqNode.Options,
		defaultIntSize,
		true, /* setDefaults */
		nil,  /* existingType */
	); err != nil {
		return nil, err
	}

	return opts, nil
}

func getSequenceIntegerBounds(
	integerType *types.T,
) (lowerIntBound int64, upperIntBound int64, err error) {
	switch integerType {
	case types.Int2:
		return math.MinInt16, math.MaxInt16, nil
	case types.Int4:
		return math.MinInt32, math.MaxInt32, nil
	case types.Int:
		return math.MinInt64, math.MaxInt64, nil
	}

	return 0, 0, errors.AssertionFailedf(
		"CREATE SEQUENCE option AS received type %s, must be integer",
		integerType,
	)
}

// AssignSequenceOptions moves options from the AST node to the sequence options descriptor,
// starting with defaults and overriding them with user-provided options.
func AssignSequenceOptions(
	opts *descpb.TableDescriptor_SequenceOpts,
	optsNode tree.SequenceOptions,
	defaultIntSize int32,
	setDefaults bool,
	existingType *types.T,
) error {

	// Set the default integer type of a sequence.
	integerType := parser.NakedIntTypeFromDefaultIntSize(defaultIntSize)
	// All other defaults are dependent on the value of increment
	// and the AS integerType. (i.e. whether the sequence is ascending
	// or descending, bigint vs. smallint)
	for _, option := range optsNode {
		if option.Name == tree.SeqOptIncrement {
			opts.Increment = *option.IntVal
		} else if option.Name == tree.SeqOptAs {
			integerType = option.AsIntegerType
			opts.AsIntegerType = integerType.SQLString()
		}
	}
	if opts.Increment == 0 {
		return errors.New("INCREMENT must not be zero")
	}
	isAscending := opts.Increment > 0

	lowerIntBound, upperIntBound, err := getSequenceIntegerBounds(integerType)
	if err != nil {
		return err
	}

	// Set increment-dependent defaults.
	if setDefaults {
		if isAscending {
			opts.MinValue = 1
			opts.MaxValue = upperIntBound
			opts.Start = opts.MinValue
		} else {
			opts.MinValue = lowerIntBound
			opts.MaxValue = -1
			opts.Start = opts.MaxValue
		}
		opts.CacheSize = 1
	}

	// Set Minvalue and Maxvalue to new types bounds if at current bounds.
	if !setDefaults && existingType != nil && opts.AsIntegerType != "" {
		existingLowerIntBound, existingUpperIntBound, err := getSequenceIntegerBounds(existingType)
		if err != nil {
			return err
		}
		// If Minvalue is bounded to the existing type, set it to the bounds of the new type.
		if opts.MinValue == existingLowerIntBound {
			opts.MinValue = lowerIntBound
		}

		// If MaxValue is bounded to the existing type, set it to the bounds of the new type.
		if opts.MaxValue == existingUpperIntBound {
			opts.MaxValue = upperIntBound
		}
	}

	// Fill in all other options.
	var restartVal *int64
	optionsSeen := map[string]bool{}
	for _, option := range optsNode {
		// Error on duplicate options.
		_, seenBefore := optionsSeen[option.Name]
		if seenBefore {
			return errors.New("conflicting or redundant options")
		}
		optionsSeen[option.Name] = true

		switch option.Name {
		case tree.SeqOptCycle:
			return unimplemented.NewWithIssue(20961,
				"CYCLE option is not supported")
		case tree.SeqOptNoCycle:
			// Do nothing; this is the default.
		case tree.SeqOptCache:
			if v := *option.IntVal; v >= 1 {
				opts.CacheSize = v
			} else {
				return errors.Newf(
					"CACHE (%d) must be greater than zero", v)
			}
		case tree.SeqOptCacheNode:
			if v := *option.IntVal; v >= 1 {
				opts.NodeCacheSize = v
			} else {
				return errors.Newf(
					"PER NODE CACHE (%d) must be greater than zero", v)
			}
		case tree.SeqOptIncrement:
			// Do nothing; this has already been set.
		case tree.SeqOptMinValue:
			// A value of nil represents the user explicitly saying `NO MINVALUE`.
			if option.IntVal == nil {
				if isAscending {
					opts.MinValue = 1
				} else {
					opts.MinValue = lowerIntBound
				}
			} else {
				opts.MinValue = *option.IntVal
			}
		case tree.SeqOptMaxValue:
			// A value of nil represents the user explicitly saying `NO MAXVALUE`.
			if option.IntVal == nil {
				if isAscending {
					opts.MaxValue = upperIntBound
				} else {
					opts.MaxValue = -1
				}
			} else {
				opts.MaxValue = *option.IntVal
			}
		case tree.SeqOptStart:
			opts.Start = *option.IntVal
		case tree.SeqOptRestart:
			// The RESTART option does not get saved, but still gets validated below.
			restartVal = option.IntVal
		case tree.SeqOptVirtual:
			opts.Virtual = true
		}
	}

	if setDefaults {
		// If start option not specified, set it to MinValue (for ascending sequences)
		// or MaxValue (for descending sequences).
		// We only do this if we're setting it for the first time, or the sequence was
		// ALTERed with the default original values.
		if _, startSeen := optionsSeen[tree.SeqOptStart]; !startSeen {
			if opts.Increment > 0 {
				opts.Start = opts.MinValue
			} else {
				opts.Start = opts.MaxValue
			}
		}
	}

	if opts.MinValue < lowerIntBound {
		return errors.Newf(
			"MINVALUE (%d) must be greater than (%d) for type %s",
			opts.MinValue,
			lowerIntBound,
			integerType.SQLString(),
		)
	}
	if opts.MaxValue < lowerIntBound {
		return errors.Newf(
			"MAXVALUE (%d) must be greater than (%d) for type %s",
			opts.MaxValue,
			lowerIntBound,
			integerType.SQLString(),
		)
	}
	if opts.MinValue > upperIntBound {
		return errors.Newf(
			"MINVALUE (%d) must be less than (%d) for type %s",
			opts.MinValue,
			upperIntBound,
			integerType.SQLString(),
		)
	}
	if opts.MaxValue > upperIntBound {
		return errors.Newf(
			"MAXVALUE (%d) must be less than (%d) for type %s",
			opts.MaxValue,
			upperIntBound,
			integerType.SQLString(),
		)
	}
	if opts.Start > opts.MaxValue {
		return errors.Newf(
			"START value (%d) cannot be greater than MAXVALUE (%d)",
			opts.Start,
			opts.MaxValue,
		)
	}
	if opts.Start < opts.MinValue {
		return errors.Newf(
			"START value (%d) cannot be less than MINVALUE (%d)",
			opts.Start,
			opts.MinValue,
		)
	}
	if restartVal != nil {
		if *restartVal > opts.MaxValue {
			return errors.Newf(
				"RESTART value (%d) cannot be greater than MAXVALUE (%d)",
				*restartVal,
				opts.MaxValue,
			)
		}
		if *restartVal < opts.MinValue {
			return errors.Newf(
				"RESTART value (%d) cannot be less than MINVALUE (%d)",
				*restartVal,
				opts.MinValue,
			)
		}
	}
	return nil
}
