// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package sqlstatsutil

import (
	"encoding/hex"
	"time"

	"github.com/cockroachdb/apd/v3"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
	"github.com/cockroachdb/cockroach/pkg/util/json"
	"github.com/cockroachdb/errors"
)

type jsonDecoder interface {
	decodeJSON(js json.JSON) error
}

type jsonEncoder interface {
	encodeJSON() (json.JSON, error)
}

type jsonMarshaler interface {
	jsonEncoder
	jsonDecoder
}

var (
	_ jsonMarshaler = &stmtFingerprintIDArray{}
	_ jsonMarshaler = &stmtStats{}
	_ jsonMarshaler = &txnStats{}
	_ jsonMarshaler = &innerTxnStats{}
	_ jsonMarshaler = &innerStmtStats{}
	_ jsonMarshaler = &execStats{}
	_ jsonMarshaler = &numericStats{}
	_ jsonMarshaler = jsonFields{}
	_ jsonMarshaler = &decimal{}
	_ jsonMarshaler = (*jsonFloat)(nil)
	_ jsonMarshaler = (*jsonString)(nil)
	_ jsonMarshaler = (*jsonBool)(nil)
	_ jsonMarshaler = (*jsonInt)(nil)
	_ jsonMarshaler = (*stmtFingerprintID)(nil)
	_ jsonMarshaler = (*int64Array)(nil)
)

type txnStats roachpb.TransactionStatistics

func (t *txnStats) jsonFields() jsonFields {
	return jsonFields{
		{"statistics", (*innerTxnStats)(t)},
		{"execution_statistics", (*execStats)(&t.ExecStats)},
	}
}

func (t *txnStats) decodeJSON(js json.JSON) error {
	return t.jsonFields().decodeJSON(js)
}

func (t *txnStats) encodeJSON() (json.JSON, error) {
	return t.jsonFields().encodeJSON()
}

type stmtStats roachpb.StatementStatistics

func (s *stmtStats) jsonFields() jsonFields {
	return jsonFields{
		{"statistics", (*innerStmtStats)(s)},
		{"execution_statistics", (*execStats)(&s.ExecStats)},
	}
}

func (s *stmtStats) decodeJSON(js json.JSON) error {
	return s.jsonFields().decodeJSON(js)
}

func (s *stmtStats) encodeJSON() (json.JSON, error) {
	return s.jsonFields().encodeJSON()
}

type stmtStatsMetadata roachpb.CollectedStatementStatistics

func (s *stmtStatsMetadata) jsonFields() jsonFields {
	return jsonFields{
		{"stmtTyp", (*jsonString)(&s.Stats.SQLType)},
		{"query", (*jsonString)(&s.Key.Query)},
		{"querySummary", (*jsonString)(&s.Key.QuerySummary)},
		{"db", (*jsonString)(&s.Key.Database)},
		{"distsql", (*jsonBool)(&s.Key.DistSQL)},
		{"failed", (*jsonBool)(&s.Key.Failed)},
		{"implicitTxn", (*jsonBool)(&s.Key.ImplicitTxn)},
		{"vec", (*jsonBool)(&s.Key.Vec)},
		{"fullScan", (*jsonBool)(&s.Key.FullScan)},
	}
}

type int64Array []int64

func (a *int64Array) decodeJSON(js json.JSON) error {
	arrLen := js.Len()
	for i := 0; i < arrLen; i++ {
		var value jsonInt
		valJSON, err := js.FetchValIdx(i)
		if err != nil {
			return err
		}
		if err := value.decodeJSON(valJSON); err != nil {
			return err
		}
		*a = append(*a, int64(value))
	}

	return nil
}

func (a *int64Array) encodeJSON() (json.JSON, error) {
	builder := json.NewArrayBuilder(len(*a))

	for _, value := range *a {
		jsVal, err := (*jsonInt)(&value).encodeJSON()
		if err != nil {
			return nil, err
		}
		builder.Add(jsVal)
	}

	return builder.Build(), nil
}

type stringArray []string

func (a *stringArray) decodeJSON(js json.JSON) error {
	arrLen := js.Len()
	for i := 0; i < arrLen; i++ {
		var value jsonString
		valJSON, err := js.FetchValIdx(i)
		if err != nil {
			return err
		}
		if err := value.decodeJSON(valJSON); err != nil {
			return err
		}
		*a = append(*a, string(value))
	}

	return nil
}

func (a *stringArray) encodeJSON() (json.JSON, error) {
	builder := json.NewArrayBuilder(len(*a))

	for _, value := range *a {
		jsVal, err := (*jsonString)(&value).encodeJSON()
		if err != nil {
			return nil, err
		}
		builder.Add(jsVal)
	}

	return builder.Build(), nil
}

type stmtFingerprintIDArray []roachpb.StmtFingerprintID

func (s *stmtFingerprintIDArray) decodeJSON(js json.JSON) error {
	arrLen := js.Len()
	for i := 0; i < arrLen; i++ {
		var fingerprintID stmtFingerprintID
		fingerprintIDJSON, err := js.FetchValIdx(i)
		if err != nil {
			return err
		}
		if err := fingerprintID.decodeJSON(fingerprintIDJSON); err != nil {
			return err
		}
		*s = append(*s, roachpb.StmtFingerprintID(fingerprintID))
	}

	return nil
}

func (s *stmtFingerprintIDArray) encodeJSON() (json.JSON, error) {
	builder := json.NewArrayBuilder(len(*s))

	for _, fingerprintID := range *s {
		jsVal, err := (*stmtFingerprintID)(&fingerprintID).encodeJSON()
		if err != nil {
			return nil, err
		}
		builder.Add(jsVal)
	}

	return builder.Build(), nil
}

type stmtFingerprintID roachpb.StmtFingerprintID

func (s *stmtFingerprintID) decodeJSON(js json.JSON) error {
	var str jsonString
	if err := str.decodeJSON(js); err != nil {
		return err
	}

	decodedString, err := hex.DecodeString(string(str))
	if err != nil {
		return err
	}

	_, fingerprintID, err := encoding.DecodeUint64Ascending(decodedString)
	if err != nil {
		return err
	}

	*s = stmtFingerprintID(fingerprintID)
	return nil
}

func (s *stmtFingerprintID) encodeJSON() (json.JSON, error) {
	return json.FromString(
		encodeStmtFingerprintIDToString((roachpb.StmtFingerprintID)(*s))), nil
}

type innerTxnStats roachpb.TransactionStatistics

func (t *innerTxnStats) jsonFields() jsonFields {
	return jsonFields{
		{"cnt", (*jsonInt)(&t.Count)},
		{"maxRetries", (*jsonInt)(&t.MaxRetries)},
		{"numRows", (*numericStats)(&t.NumRows)},
		{"svcLat", (*numericStats)(&t.ServiceLat)},
		{"retryLat", (*numericStats)(&t.RetryLat)},
		{"commitLat", (*numericStats)(&t.CommitLat)},
		{"bytesRead", (*numericStats)(&t.BytesRead)},
		{"rowsRead", (*numericStats)(&t.RowsRead)},
		{"rowsWritten", (*numericStats)(&t.RowsWritten)},
	}
}

func (t *innerTxnStats) decodeJSON(js json.JSON) error {
	return t.jsonFields().decodeJSON(js)
}

func (t *innerTxnStats) encodeJSON() (json.JSON, error) {
	return t.jsonFields().encodeJSON()
}

type innerStmtStats roachpb.StatementStatistics

func (s *innerStmtStats) jsonFields() jsonFields {
	return jsonFields{
		{"cnt", (*jsonInt)(&s.Count)},
		{"firstAttemptCnt", (*jsonInt)(&s.FirstAttemptCount)},
		{"maxRetries", (*jsonInt)(&s.MaxRetries)},
		{"lastExecAt", (*jsonTime)(&s.LastExecTimestamp)},
		{"numRows", (*numericStats)(&s.NumRows)},
		{"parseLat", (*numericStats)(&s.ParseLat)},
		{"planLat", (*numericStats)(&s.PlanLat)},
		{"runLat", (*numericStats)(&s.RunLat)},
		{"svcLat", (*numericStats)(&s.ServiceLat)},
		{"ovhLat", (*numericStats)(&s.OverheadLat)},
		{"bytesRead", (*numericStats)(&s.BytesRead)},
		{"rowsRead", (*numericStats)(&s.RowsRead)},
		{"rowsWritten", (*numericStats)(&s.RowsWritten)},
		{"nodes", (*int64Array)(&s.Nodes)},
		{"planGists", (*stringArray)(&s.PlanGists)},
	}
}

func (s *innerStmtStats) decodeJSON(js json.JSON) error {
	return s.jsonFields().decodeJSON(js)
}

func (s *innerStmtStats) encodeJSON() (json.JSON, error) {
	return s.jsonFields().encodeJSON()
}

type execStats roachpb.ExecStats

func (e *execStats) jsonFields() jsonFields {
	return jsonFields{
		{"cnt", (*jsonInt)(&e.Count)},
		{"networkBytes", (*numericStats)(&e.NetworkBytes)},
		{"maxMemUsage", (*numericStats)(&e.MaxMemUsage)},
		{"contentionTime", (*numericStats)(&e.ContentionTime)},
		{"networkMsgs", (*numericStats)(&e.NetworkMessages)},
		{"maxDiskUsage", (*numericStats)(&e.MaxDiskUsage)},
	}
}

func (e *execStats) decodeJSON(js json.JSON) error {
	return e.jsonFields().decodeJSON(js)
}

func (e *execStats) encodeJSON() (json.JSON, error) {
	return e.jsonFields().encodeJSON()
}

type numericStats roachpb.NumericStat

func (n *numericStats) jsonFields() jsonFields {
	return jsonFields{
		{"mean", (*jsonFloat)(&n.Mean)},
		{"sqDiff", (*jsonFloat)(&n.SquaredDiffs)},
	}
}

func (n *numericStats) decodeJSON(js json.JSON) error {
	return n.jsonFields().decodeJSON(js)
}

func (n *numericStats) encodeJSON() (json.JSON, error) {
	return n.jsonFields().encodeJSON()
}

type jsonFields []jsonField

func (jf jsonFields) decodeJSON(js json.JSON) (err error) {
	var fieldName string
	defer func() {
		if err != nil {
			err = errors.Wrapf(err, "decoding field %s", fieldName)
		}
	}()

	for i := range jf {
		fieldName = jf[i].field
		field, err := js.FetchValKey(fieldName)
		if err != nil {
			return err
		}
		if field != nil {
			err = jf[i].val.decodeJSON(field)
			if err != nil {
				return err
			}
		}
	}

	return nil
}

func (jf jsonFields) encodeJSON() (json.JSON, error) {
	builder := json.NewObjectBuilder(len(jf))
	for i := range jf {
		jsVal, err := jf[i].val.encodeJSON()
		if err != nil {
			return nil, errors.Wrapf(err, "encoding field %s", jf[i].field)
		}
		builder.Add(jf[i].field, jsVal)
	}
	return builder.Build(), nil
}

type jsonField struct {
	field string
	val   jsonMarshaler
}

type jsonTime time.Time

func (t *jsonTime) decodeJSON(js json.JSON) error {
	var s jsonString
	if err := s.decodeJSON(js); err != nil {
		return err
	}

	tm := (*time.Time)(t)
	if err := tm.UnmarshalText([]byte(s)); err != nil {
		return err
	}

	return nil
}

func (t *jsonTime) encodeJSON() (json.JSON, error) {
	str, err := (time.Time)(*t).MarshalText()
	if err != nil {
		return nil, err
	}
	return json.FromString(string(str)), nil
}

type jsonString string

func (s *jsonString) decodeJSON(js json.JSON) error {
	text, err := js.AsText()
	if err != nil {
		return err
	}
	*s = (jsonString)(*text)
	return nil
}

func (s *jsonString) encodeJSON() (json.JSON, error) {
	return json.FromString(string(*s)), nil
}

type jsonFloat float64

func (f *jsonFloat) decodeJSON(js json.JSON) error {
	var d apd.Decimal
	if err := (*decimal)(&d).decodeJSON(js); err != nil {
		return err
	}

	val, err := d.Float64()
	if err != nil {
		return err
	}
	*f = (jsonFloat)(val)

	return nil
}

func (f *jsonFloat) encodeJSON() (json.JSON, error) {
	return json.FromFloat64(float64(*f))
}

type jsonBool bool

func (b *jsonBool) decodeJSON(js json.JSON) error {
	switch js.Type() {
	case json.TrueJSONType:
		*b = true
	case json.FalseJSONType:
		*b = false
	default:
		return errors.New("invalid boolean json value type")
	}
	return nil
}

func (b *jsonBool) encodeJSON() (json.JSON, error) {
	return json.FromBool(bool(*b)), nil
}

type jsonInt int64

func (i *jsonInt) decodeJSON(js json.JSON) error {
	var d apd.Decimal
	if err := (*decimal)(&d).decodeJSON(js); err != nil {
		return err
	}
	val, err := d.Int64()
	if err != nil {
		return err
	}
	*i = (jsonInt)(val)
	return nil
}

func (i *jsonInt) encodeJSON() (json.JSON, error) {
	return json.FromInt64(int64(*i)), nil
}

type decimal apd.Decimal

func (d *decimal) decodeJSON(js json.JSON) error {
	dec, ok := js.AsDecimal()
	if !ok {
		return errors.New("unable to decode decimal")
	}
	*d = (decimal)(*dec)
	return nil
}

func (d *decimal) encodeJSON() (json.JSON, error) {
	return json.FromDecimal(*(*apd.Decimal)(d)), nil
}
