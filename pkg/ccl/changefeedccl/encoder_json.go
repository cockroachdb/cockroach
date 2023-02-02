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
	"github.com/cockroachdb/cockroach/pkg/util/cache"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/json"
	"github.com/cockroachdb/errors"
)

// jsonEncoder encodes changefeed entries as JSON. Keys are the primary key
// columns in a JSON array. Values are a JSON object mapping every column name
// to its value. Updated timestamps in rows and resolved timestamp payloads are
// stored in a sub-object under the `__crdb__` key in the top-level JSON object.
type jsonEncoder struct {
	updatedField, mvccTimestampField, beforeField, keyInValue, topicInValue bool
	envelopeType                                                            changefeedbase.EnvelopeType

	buf             bytes.Buffer
	versionEncoder  func(ed *cdcevent.EventDescriptor) *versionEncoder
	envelopeEncoder func(evCtx eventContext, updated, prev cdcevent.Row) (json.JSON, error)
}

var _ Encoder = &jsonEncoder{}

func canJSONEncodeMetadata(e changefeedbase.EnvelopeType) bool {
	// bare envelopes use the _crdb_ key to avoid collisions with column names.
	// wrapped envelopes can put metadata at the top level because the columns
	// are nested under the "after:" key.
	return e == changefeedbase.OptEnvelopeBare || e == changefeedbase.OptEnvelopeWrapped
}

func makeJSONEncoder(opts changefeedbase.EncodingOptions) (*jsonEncoder, error) {
	versionCache := cache.NewUnorderedCache(cdcevent.DefaultCacheConfig)
	e := &jsonEncoder{
		envelopeType:       opts.Envelope,
		updatedField:       opts.UpdatedTimestamps,
		mvccTimestampField: opts.MVCCTimestamps,
		// In the bare envelope we don't output diff directly, it's incorporated into the
		// projection as desired.
		beforeField:  opts.Diff && opts.Envelope != changefeedbase.OptEnvelopeBare,
		keyInValue:   opts.KeyInValue,
		topicInValue: opts.TopicInValue,
		versionEncoder: func(ed *cdcevent.EventDescriptor) *versionEncoder {
			key := cdcevent.CacheKey{
				ID:       ed.TableID,
				Version:  ed.Version,
				FamilyID: ed.FamilyID,
			}
			return cdcevent.GetCachedOrCreate(key, versionCache, func() interface{} {
				return &versionEncoder{}
			}).(*versionEncoder)
		},
	}

	if !canJSONEncodeMetadata(e.envelopeType) {
		if e.keyInValue {
			return nil, errors.Errorf(`%s is only usable with %s=%s`,
				changefeedbase.OptKeyInValue, changefeedbase.OptEnvelope, changefeedbase.OptEnvelopeWrapped)
		}
		if e.topicInValue {
			return nil, errors.Errorf(`%s is only usable with %s=%s`,
				changefeedbase.OptTopicInValue, changefeedbase.OptEnvelope, changefeedbase.OptEnvelopeWrapped)
		}
	}

	if e.envelopeType == changefeedbase.OptEnvelopeWrapped {
		if err := e.initWrappedEnvelope(); err != nil {
			return nil, err
		}
	} else {
		if err := e.initRawEnvelope(); err != nil {
			return nil, err
		}
	}

	return e, nil
}

// versionEncoder memoizes version specific encoding state.
type versionEncoder struct {
	valueBuilder *json.FixedKeysObjectBuilder
}

// EncodeKey implements the Encoder interface.
func (e *jsonEncoder) EncodeKey(_ context.Context, row cdcevent.Row) ([]byte, error) {
	j, err := e.versionEncoder(row.EventDescriptor).encodeKeyRaw(row)
	if err != nil {
		return nil, err
	}
	e.buf.Reset()
	j.Format(&e.buf)
	return e.buf.Bytes(), nil
}

func (e *versionEncoder) encodeKeyRaw(row cdcevent.Row) (json.JSON, error) {
	kb := json.NewArrayBuilder(1)
	if err := row.ForEachKeyColumn().Datum(func(d tree.Datum, col cdcevent.ResultColumn) error {
		j, err := tree.AsJSON(d, sessiondatapb.DataConversionConfig{}, time.UTC)
		if err != nil {
			return err
		}
		kb.Add(j)
		return nil
	}); err != nil {
		return nil, err
	}

	return kb.Build(), nil
}

func (e *versionEncoder) encodeKeyInValue(
	updated cdcevent.Row, b *json.FixedKeysObjectBuilder,
) error {
	keyEntries, err := e.encodeKeyRaw(updated)
	if err != nil {
		return err
	}
	return b.Set("key", keyEntries)
}

var emptyJSONValue = func() json.JSON {
	j, err := json.MakeJSON(map[string]interface{}{})
	if err != nil {
		panic(err)
	}
	return j
}()

func (e *versionEncoder) rowAsGoNative(row cdcevent.Row, meta json.JSON) (json.JSON, error) {
	if !row.HasValues() || row.IsDeleted() {
		if meta != nil {
			b := json.NewObjectBuilder(1)
			b.Add(jsonMetaSentinel, meta)
			return b.Build(), nil
		}
		return json.NullJSONValue, nil
	}

	if e.valueBuilder == nil {
		keys := make([]string, 0, len(row.ResultColumns()))
		_ = row.ForEachColumn().Col(func(col cdcevent.ResultColumn) error {
			keys = append(keys, col.Name)
			return nil
		})
		if meta != nil {
			keys = append(keys, jsonMetaSentinel)
		}
		b, err := json.NewFixedKeysObjectBuilder(keys)
		if err != nil {
			return nil, err
		}
		e.valueBuilder = b
	}

	if err := row.ForEachColumn().Datum(func(d tree.Datum, col cdcevent.ResultColumn) error {
		j, err := tree.AsJSON(d, sessiondatapb.DataConversionConfig{}, time.UTC)
		if err != nil {
			return err
		}
		return e.valueBuilder.Set(col.Name, j)
	}); err != nil {
		return nil, err
	}

	if meta != nil {
		if err := e.valueBuilder.Set(jsonMetaSentinel, meta); err != nil {
			return nil, err
		}
	}

	return e.valueBuilder.Build()
}

func (e *jsonEncoder) initRawEnvelope() error {
	// Determine if we need to add crdb meta.
	var metaKeys []string
	if e.updatedField {
		metaKeys = append(metaKeys, "updated")
	}
	if e.mvccTimestampField {
		metaKeys = append(metaKeys, "mvcc_timestamp")
	}
	if e.keyInValue {
		metaKeys = append(metaKeys, "key")
	}
	if e.topicInValue {
		metaKeys = append(metaKeys, "topic")
	}

	// Setup builder for crdb meta if needed.
	var metaBuilder *json.FixedKeysObjectBuilder
	if len(metaKeys) > 0 {
		b, err := json.NewFixedKeysObjectBuilder(metaKeys)
		if err != nil {
			return err
		}
		metaBuilder = b
	}

	e.envelopeEncoder = func(evCtx eventContext, updated, _ cdcevent.Row) (_ json.JSON, err error) {
		ve := e.versionEncoder(updated.EventDescriptor)
		if len(metaKeys) == 0 {
			if updated.IsDeleted() {
				return emptyJSONValue, nil
			}
			return ve.rowAsGoNative(updated, nil)
		}

		if e.updatedField {
			if err := metaBuilder.Set("updated", json.FromString(timestampToString(evCtx.updated))); err != nil {
				return nil, err
			}
		}

		if e.mvccTimestampField {
			if err := metaBuilder.Set("mvcc_timestamp", json.FromString(timestampToString(evCtx.mvcc))); err != nil {
				return nil, err
			}
		}

		if e.keyInValue {
			if err := ve.encodeKeyInValue(updated, metaBuilder); err != nil {
				return nil, err
			}
		}

		if e.topicInValue {
			if err := metaBuilder.Set("topic", json.FromString(evCtx.topic)); err != nil {
				return nil, err
			}
		}

		meta, err := metaBuilder.Build()
		if err != nil {
			return nil, err
		}
		return ve.rowAsGoNative(updated, meta)
	}
	return nil
}

func (e *jsonEncoder) initWrappedEnvelope() error {
	keys := []string{"after"}
	if e.beforeField {
		keys = append(keys, "before")
	}
	if e.keyInValue {
		keys = append(keys, "key")
	}
	if e.topicInValue {
		keys = append(keys, "topic")
	}
	if e.updatedField {
		keys = append(keys, "updated")
	}
	if e.mvccTimestampField {
		keys = append(keys, "mvcc_timestamp")
	}
	b, err := json.NewFixedKeysObjectBuilder(keys)
	if err != nil {
		return err
	}

	e.envelopeEncoder = func(evCtx eventContext, updated, prev cdcevent.Row) (json.JSON, error) {
		ve := e.versionEncoder(updated.EventDescriptor)
		after, err := ve.rowAsGoNative(updated, nil)
		if err != nil {
			return nil, err
		}
		if err := b.Set("after", after); err != nil {
			return nil, err
		}

		if e.beforeField {
			var before json.JSON
			if prev.IsInitialized() && !prev.IsDeleted() {
				before, err = e.versionEncoder(prev.EventDescriptor).rowAsGoNative(prev, nil)
				if err != nil {
					return nil, err
				}
			} else {
				before = json.NullJSONValue
			}

			if err := b.Set("before", before); err != nil {
				return nil, err
			}
		}

		if e.keyInValue {
			if err := ve.encodeKeyInValue(updated, b); err != nil {
				return nil, err
			}
		}

		if e.topicInValue {
			if err := b.Set("topic", json.FromString(evCtx.topic)); err != nil {
				return nil, err
			}
		}

		if e.updatedField {
			if err := b.Set("updated", json.FromString(timestampToString(evCtx.updated))); err != nil {
				return nil, err
			}
		}

		if e.mvccTimestampField {
			if err := b.Set("mvcc_timestamp", json.FromString(timestampToString(evCtx.mvcc))); err != nil {
				return nil, err
			}
		}

		return b.Build()
	}
	return nil
}

// EncodeValue implements the Encoder interface.
func (e *jsonEncoder) EncodeValue(
	ctx context.Context, evCtx eventContext, updatedRow cdcevent.Row, prevRow cdcevent.Row,
) ([]byte, error) {
	if e.envelopeType == changefeedbase.OptEnvelopeKeyOnly {
		return nil, nil
	}

	if updatedRow.IsDeleted() && !canJSONEncodeMetadata(e.envelopeType) {
		return nil, nil
	}

	j, err := e.envelopeEncoder(evCtx, updatedRow, prevRow)
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
	if e.envelopeType == changefeedbase.OptEnvelopeWrapped {
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
	e, err := makeJSONEncoder(opts)
	if err != nil {
		return nil, err
	}
	return e.EncodeValue(context.TODO(), placeholderCtx, r, cdcevent.Row{})

}

func init() {

	overload := tree.Overload{
		Types:      tree.VariadicType{FixedTypes: []*types.T{types.AnyTuple}, VarType: types.String},
		ReturnType: tree.FixedReturnType(types.Bytes),
		Fn: func(ctx context.Context, evalCtx *eval.Context, args tree.Datums) (tree.Datum, error) {
			row := cdcevent.MakeRowFromTuple(ctx, evalCtx, tree.MustBeDTuple(args[0]))
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
		Class: tree.NormalClass,
		Info:  "Strings can be of the form 'resolved' or 'resolved=1s'.",
		// Probably actually stable, but since this is tightly coupled to changefeed logic by design,
		// best to be defensive.
		Volatility: volatility.Volatile,
	}

	utilccl.RegisterCCLBuiltin("crdb_internal.to_json_as_changefeed_with_flags",
		`Encodes a tuple the way a changefeed would output it if it were inserted as a row or emitted by a changefeed expression, and returns the raw bytes. 
		Flags such as 'diff' modify the encoding as though specified in the WITH portion of a changefeed.`,
		overload)
}
