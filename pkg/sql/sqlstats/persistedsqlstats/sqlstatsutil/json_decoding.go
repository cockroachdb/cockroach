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

	"github.com/cockroachdb/apd/v2"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
	"github.com/cockroachdb/cockroach/pkg/util/json"
	"github.com/cockroachdb/errors"
)

// DecodeTxnStatsMetadataJSON decodes the 'metadata' field of the JSON
// representation of transaction statistics into
// roachpb.CollectedTransactionStatistics.
func DecodeTxnStatsMetadataJSON(
	metadata json.JSON, result *roachpb.CollectedTransactionStatistics,
) error {
	fields := [...]jsonField{
		{"stmtFingerprintIDs", (*stmtFingerprintIDArray)(&result.StatementFingerprintIDs)},
	}

	return (jsonFields)(fields[:]).decodeJSON(metadata)
}

// DecodeTxnStatsStatisticsJSON decodes the 'statistics' section of the
// transaction statistics JSON payload into roachpb.TransactionStatistics
// protobuf.
func DecodeTxnStatsStatisticsJSON(jsonVal json.JSON, result *roachpb.TransactionStatistics) error {
	fields := [...]jsonField{
		{"statistics", (*txnStats)(result)},
		{"execution_statistics", (*execStats)(&result.ExecStats)},
	}
	return (jsonFields)(fields[:]).decodeJSON(jsonVal)
}

// DecodeStmtStatsMetadataJSON decodes the 'metadata' field of the JSON
// representation of the statement statistics into
// roachpb.CollectedStatementStatistics.
func DecodeStmtStatsMetadataJSON(
	metadata json.JSON, result *roachpb.CollectedStatementStatistics,
) error {
	fields := [...]jsonField{
		{"stmtTyp", (*jsonString)(&result.Stats.SQLType)},
		{"query", (*jsonString)(&result.Key.Query)},
		{"db", (*jsonString)(&result.Key.Database)},
		{"distsql", (*jsonBool)(&result.Key.DistSQL)},
		{"failed", (*jsonBool)(&result.Key.Failed)},
		{"opt", (*jsonBool)(&result.Key.Opt)},
		{"implicitTxn", (*jsonBool)(&result.Key.ImplicitTxn)},
		{"vec", (*jsonBool)(&result.Key.Vec)},
		{"fullScan", (*jsonBool)(&result.Key.FullScan)},
	}

	return (jsonFields)(fields[:]).decodeJSON(metadata)
}

// DecodeStmtStatsStatisticsJSON decodes the 'statistics' field and the
// 'execution_statistics' field in the given json into
// roachpb.StatementStatistics.
func DecodeStmtStatsStatisticsJSON(jsonVal json.JSON, result *roachpb.StatementStatistics) error {
	fields := [...]jsonField{
		{"statistics", (*stmtStats)(result)},
		{"execution_statistics", (*execStats)(&result.ExecStats)},
	}
	return (jsonFields)(fields[:]).decodeJSON(jsonVal)
}

type jsonDecoder interface {
	decodeJSON(js json.JSON) error
}

var (
	_ jsonDecoder = &stmtFingerprintIDArray{}
	_ jsonDecoder = &txnStats{}
	_ jsonDecoder = &stmtStats{}
	_ jsonDecoder = &execStats{}
	_ jsonDecoder = &numericStats{}
	_ jsonDecoder = &jsonField{}
	_ jsonDecoder = jsonFields{}
	_ jsonDecoder = &decimal{}
	_ jsonDecoder = (*jsonFloat)(nil)
	_ jsonDecoder = (*jsonString)(nil)
	_ jsonDecoder = (*jsonBool)(nil)
	_ jsonDecoder = (*jsonInt)(nil)
	_ jsonDecoder = (*stmtFingerprintID)(nil)
)

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

type txnStats roachpb.TransactionStatistics

func (t *txnStats) decodeJSON(js json.JSON) error {
	fields := [...]jsonField{
		{"maxRetries", (*jsonInt)(&t.MaxRetries)},
		{"numRows", (*numericStats)(&t.NumRows)},
		{"svcLat", (*numericStats)(&t.ServiceLat)},
		{"retryLat", (*numericStats)(&t.RetryLat)},
		{"commitLat", (*numericStats)(&t.CommitLat)},
		{"bytesRead", (*numericStats)(&t.BytesRead)},
		{"rowsRead", (*numericStats)(&t.RowsRead)},
	}
	return (jsonFields)(fields[:]).decodeJSON(js)
}

type stmtStats roachpb.StatementStatistics

func (s *stmtStats) decodeJSON(js json.JSON) error {
	fields := [...]jsonField{
		{"firstAttemptCnt", (*jsonInt)(&s.Count)},
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
	}
	return (jsonFields)(fields[:]).decodeJSON(js)
}

type execStats roachpb.ExecStats

func (e *execStats) decodeJSON(js json.JSON) error {
	fields := [...]jsonField{
		{"cnt", (*jsonInt)(&e.Count)},
		{"networkBytes", (*numericStats)(&e.NetworkBytes)},
		{"maxMemUsage", (*numericStats)(&e.MaxMemUsage)},
		{"contentionTime", (*numericStats)(&e.ContentionTime)},
		{"networkMsgs", (*numericStats)(&e.NetworkMessages)},
		{"maxDiskUsage", (*numericStats)(&e.MaxDiskUsage)},
	}

	return (jsonFields)(fields[:]).decodeJSON(js)
}

type numericStats roachpb.NumericStat

func (n *numericStats) decodeJSON(js json.JSON) error {
	fields := [...]jsonField{
		{"mean", (*jsonFloat)(&n.Mean)},
		{"sqDiff", (*jsonFloat)(&n.SquaredDiffs)},
	}
	return (jsonFields)(fields[:]).decodeJSON(js)
}

type jsonFields []jsonField

func (jf jsonFields) decodeJSON(js json.JSON) error {
	for i := range jf {
		if err := jf[i].decodeJSON(js); err != nil {
			return err
		}
	}

	return nil
}

type jsonField struct {
	field   string
	decoder jsonDecoder
}

func (jf *jsonField) decodeJSON(js json.JSON) (err error) {
	defer func() {
		if err != nil {
			err = errors.Wrapf(err, "decoding field %s", jf.field)
		}
	}()
	field, err := safeFetchVal(js, jf.field)
	if err != nil {
		return err
	}
	return jf.decoder.decodeJSON(field)
}

type jsonTime time.Time

func (t *jsonTime) decodeJSON(js json.JSON) error {
	var s jsonString
	if err := s.decodeJSON(js); err != nil {
		return err
	}

	tm := (time.Time)(*t)
	if err := tm.UnmarshalText([]byte(s)); err != nil {
		return err
	}

	return nil
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

type jsonBool bool

func (b *jsonBool) decodeJSON(js json.JSON) error {
	if js == json.TrueJSONValue {
		*b = true
	}
	*b = false

	return nil
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

type decimal apd.Decimal

func (d *decimal) decodeJSON(js json.JSON) error {
	dec, ok := js.AsDecimal()
	if !ok {
		return errors.New("unable to decode decimal")
	}
	*d = (decimal)(*dec)
	return nil
}

func safeFetchVal(jsonVal json.JSON, key string) (json.JSON, error) {
	field, err := jsonVal.FetchValKey(key)
	if err != nil {
		return nil, err
	}
	if field == nil {
		return nil, errors.Errorf("%s field is not found in the JSON payload", key)
	}
	return field, nil
}
