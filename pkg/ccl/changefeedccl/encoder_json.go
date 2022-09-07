// Copyright 2022 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package changefeedccl

import (
	"bytes"
	"context"
	gojson "encoding/json"
	"strings"
	"time"

	"github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl/cdcevent"
	"github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl/changefeedbase"
	"github.com/cockroachdb/cockroach/pkg/ccl/utilccl"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/eval"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/volatility"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondatapb"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/json"
	"github.com/cockroachdb/errors"
)

// jsonEncoder encodes changefeed entries as JSON. Keys are the primary key
// columns in a JSON array. Values are a JSON object mapping every column name
// to its value. Updated timestamps in rows and resolved timestamp payloads are
// stored in a sub-object under the `__crdb__` key in the top-level JSON object.
type jsonEncoder struct {
	updatedField, mvccTimestampField, beforeField, wrapped, keyOnly, keyInValue, topicInValue bool

	targets changefeedbase.Targets
	buf     bytes.Buffer
}

var _ Encoder = &jsonEncoder{}

func makeJSONEncoder(
	opts changefeedbase.EncodingOptions, targets changefeedbase.Targets,
) (*jsonEncoder, error) {
	e := &jsonEncoder{
		targets: targets,
		keyOnly: opts.Envelope == changefeedbase.OptEnvelopeKeyOnly,
		wrapped: opts.Envelope == changefeedbase.OptEnvelopeWrapped,
	}
	e.updatedField = opts.UpdatedTimestamps
	e.mvccTimestampField = opts.MVCCTimestamps
	e.beforeField = opts.Diff
	e.keyInValue = opts.KeyInValue
	if e.keyInValue && !e.wrapped {
		return nil, errors.Errorf(`%s is only usable with %s=%s`,
			changefeedbase.OptKeyInValue, changefeedbase.OptEnvelope, changefeedbase.OptEnvelopeWrapped)
	}
	e.topicInValue = opts.TopicInValue
	if e.topicInValue && !e.wrapped {
		return nil, errors.Errorf(`%s is only usable with %s=%s`,
			changefeedbase.OptTopicInValue, changefeedbase.OptEnvelope, changefeedbase.OptEnvelopeWrapped)
	}
	return e, nil
}

// EncodeKey implements the Encoder interface.
func (e *jsonEncoder) EncodeKey(_ context.Context, row cdcevent.Row) ([]byte, error) {
	jsonEntries, err := e.encodeKeyRaw(row)
	if err != nil {
		return nil, err
	}
	j, err := json.MakeJSON(jsonEntries)
	if err != nil {
		return nil, err
	}
	e.buf.Reset()
	j.Format(&e.buf)
	return e.buf.Bytes(), nil
}

func (e *jsonEncoder) encodeKeyRaw(row cdcevent.Row) ([]interface{}, error) {
	var jsonEntries []interface{}
	if err := row.ForEachKeyColumn().Datum(func(d tree.Datum, col cdcevent.ResultColumn) error {
		j, err := tree.AsJSON(d, sessiondatapb.DataConversionConfig{}, time.UTC)
		if err != nil {
			return err
		}
		jsonEntries = append(jsonEntries, j)
		return nil
	}); err != nil {
		return nil, err
	}

	return jsonEntries, nil
}

func rowAsGoNative(row cdcevent.Row) (map[string]interface{}, error) {
	if !row.HasValues() || row.IsDeleted() {
		return nil, nil
	}

	result := make(map[string]interface{})
	if err := row.ForEachColumn().Datum(func(d tree.Datum, col cdcevent.ResultColumn) (err error) {
		result[col.Name], err = tree.AsJSON(d, sessiondatapb.DataConversionConfig{}, time.UTC)
		return err
	}); err != nil {
		return nil, err
	}
	return result, nil
}

// EncodeValue implements the Encoder interface.
func (e *jsonEncoder) EncodeValue(
	ctx context.Context, evCtx eventContext, updatedRow cdcevent.Row, prevRow cdcevent.Row,
) ([]byte, error) {
	if e.keyOnly || (!e.wrapped && updatedRow.IsDeleted()) {
		return nil, nil
	}

	after, err := rowAsGoNative(updatedRow)
	if err != nil {
		return nil, err
	}

	before, err := rowAsGoNative(prevRow)
	if err != nil {
		return nil, err
	}

	var jsonEntries map[string]interface{}
	if e.wrapped {
		if after != nil {
			jsonEntries = map[string]interface{}{`after`: after}
		} else {
			jsonEntries = map[string]interface{}{`after`: nil}
		}
		if e.beforeField {
			if before != nil {
				jsonEntries[`before`] = before
			} else {
				jsonEntries[`before`] = nil
			}
		}
		if e.keyInValue {
			keyEntries, err := e.encodeKeyRaw(updatedRow)
			if err != nil {
				return nil, err
			}
			jsonEntries[`key`] = keyEntries
		}
		if e.topicInValue {
			jsonEntries[`topic`] = evCtx.topic
		}
	} else {
		jsonEntries = after
	}

	if e.updatedField || e.mvccTimestampField {
		var meta map[string]interface{}
		if e.wrapped {
			meta = jsonEntries
		} else {
			meta = make(map[string]interface{}, 1)
			jsonEntries[jsonMetaSentinel] = meta
		}
		if e.updatedField {
			meta[`updated`] = evCtx.updated.AsOfSystemTime()
		}
		if e.mvccTimestampField {
			meta[`mvcc_timestamp`] = evCtx.mvcc.AsOfSystemTime()
		}
	}

	j, err := json.MakeJSON(jsonEntries)
	if err != nil {
		return nil, err
	}
	e.buf.Reset()
	j.Format(&e.buf)
	return e.buf.Bytes(), nil
}

// EncodeResolvedTimestamp implements the Encoder interface.
func (e *jsonEncoder) EncodeResolvedTimestamp(
	_ context.Context, _ string, resolved hlc.Timestamp,
) ([]byte, error) {
	meta := map[string]interface{}{
		`resolved`: eval.TimestampToDecimalDatum(resolved).Decimal.String(),
	}
	var jsonEntries interface{}
	if e.wrapped {
		jsonEntries = meta
	} else {
		jsonEntries = map[string]interface{}{
			jsonMetaSentinel: meta,
		}
	}
	return gojson.Marshal(jsonEntries)
}

var placeholderCtx = eventContext{topic: "topic"}

// EncodeAsJSONChangefeedWithFlags implements the crdb_internal.to_json_as_changefeed_with_flags
// builtin.
func EncodeAsJSONChangefeedWithFlags(r cdcevent.Row, flags ...string) ([]byte, error) {
	optsMap := make(map[string]string, len(flags))
	for _, f := range flags {
		split := strings.SplitN(f, "=", 2)
		k := split[0]
		var v string
		if len(split) == 2 {
			v = split[1]
		}
		optsMap[k] = v
	}
	opts, err := changefeedbase.MakeStatementOptions(optsMap).GetEncodingOptions()
	if err != nil {
		return nil, err
	}
	// If this function ends up needing to be optimized, cache or pool these.
	// Nontrivial to do as an encoder generally isn't safe to call on different
	// rows in parallel.
	e, err := makeJSONEncoder(opts, changefeedbase.Targets{})
	if err != nil {
		return nil, err
	}
	return e.EncodeValue(context.TODO(), placeholderCtx, r, cdcevent.Row{})

}

func init() {

	overload := tree.Overload{
		Types:      tree.VariadicType{FixedTypes: []*types.T{types.AnyTuple}, VarType: types.String},
		ReturnType: tree.FixedReturnType(types.Bytes),
		Fn: func(evalCtx *eval.Context, args tree.Datums) (tree.Datum, error) {
			row := cdcevent.MakeRowFromTuple(evalCtx, tree.MustBeDTuple(args[0]))
			flags := make([]string, len(args)-1)
			for i, d := range args[1:] {
				flags[i] = string(tree.MustBeDString(d))
			}
			o, err := EncodeAsJSONChangefeedWithFlags(row, flags...)
			if err != nil {
				return nil, pgerror.Wrap(err, pgcode.InvalidParameterValue, ``)
			}
			return tree.NewDBytes(tree.DBytes(o)), nil
		},
		Info: "Strings can be of the form 'resolved' or 'resolved=1s'.",
		// Probably actually stable, but since this is tightly coupled to changefeed logic by design,
		// best to be defensive.
		Volatility: volatility.Volatile,
	}

	utilccl.RegisterCCLBuiltin("crdb_internal.to_json_as_changefeed_with_flags",
		`Encodes a tuple the way a changefeed would output it if it were inserted as a row or emitted by a changefeed expression, and returns the raw bytes. 
		Flags such as 'diff' modify the encoding as though specified in the WITH portion of a changefeed.`,
		overload)
}
