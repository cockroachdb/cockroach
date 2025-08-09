// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package sqlstatsutil

import (
	"encoding/hex"
	"time"

	"github.com/cockroachdb/apd/v3"
	"github.com/cockroachdb/cockroach/pkg/sql/appstatspb"
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
	_ jsonMarshaler = (*int32Array)(nil)
	_ jsonMarshaler = &latencyInfo{}
)

type txnStats appstatspb.TransactionStatistics

func (t *txnStats) decodeJSON(js json.JSON) error {
	// Decode "statistics" field
	if valJSON, err := js.FetchValKey("statistics"); err != nil {
		return err
	} else if valJSON != nil {
		if err := (*innerTxnStats)(t).decodeJSON(valJSON); err != nil {
			return err
		}
	}

	// Decode "execution_statistics" field
	if valJSON, err := js.FetchValKey("execution_statistics"); err != nil {
		return err
	} else if valJSON != nil {
		if err := (*execStats)(&t.ExecStats).decodeJSON(valJSON); err != nil {
			return err
		}
	}

	return nil
}

func (t *txnStats) encodeJSON() (json.JSON, error) {
	builder := json.NewObjectBuilder(2)
	val, err := (*innerTxnStats)(t).encodeJSON()
	if err != nil {
		return nil, err
	}
	builder.Add("statistics", val)
	val, err = (*execStats)(&t.ExecStats).encodeJSON()
	if err != nil {
		return nil, err
	}
	builder.Add("execution_statistics", val)
	return builder.Build(), nil
}

type stmtStats appstatspb.StatementStatistics

func (s *stmtStats) decodeJSON(js json.JSON) error {
	// Decode "statistics" field
	if valJSON, err := js.FetchValKey("statistics"); err != nil {
		return err
	} else if valJSON != nil {
		if err := (*innerStmtStats)(s).decodeJSON(valJSON); err != nil {
			return err
		}
	}

	// Decode "execution_statistics" field
	if valJSON, err := js.FetchValKey("execution_statistics"); err != nil {
		return err
	} else if valJSON != nil {
		if err := (*execStats)(&s.ExecStats).decodeJSON(valJSON); err != nil {
			return err
		}
	}

	// Decode "index_recommendations" field
	if valJSON, err := js.FetchValKey("index_recommendations"); err != nil {
		return err
	} else if valJSON != nil {
		if err := (*stringArray)(&s.IndexRecommendations).decodeJSON(valJSON); err != nil {
			return err
		}
	}

	return nil
}

func (s *stmtStats) encodeJSON() (json.JSON, error) {
	builder := json.NewObjectBuilder(3)
	val, err := (*innerStmtStats)(s).encodeJSON()
	if err != nil {
		return nil, err
	}
	builder.Add("statistics", val)
	val, err = (*execStats)(&s.ExecStats).encodeJSON()
	if err != nil {
		return nil, err
	}
	builder.Add("execution_statistics", val)
	val, err = (*stringArray)(&s.IndexRecommendations).encodeJSON()
	if err != nil {
		return nil, err
	}
	builder.Add("index_recommendations", val)
	return builder.Build(), nil
}

type stmtStatsMetadata appstatspb.CollectedStatementStatistics

type aggregatedMetadata appstatspb.AggregatedStatementMetadata

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

type int32Array []int32

func (a *int32Array) decodeJSON(js json.JSON) error {
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
		*a = append(*a, int32(value))
	}
	return nil
}

func (a *int32Array) encodeJSON() (json.JSON, error) {
	builder := json.NewArrayBuilder(len(*a))
	for _, value := range *a {
		builder.Add(json.FromInt64(int64(value)))
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

type stmtFingerprintIDArray []appstatspb.StmtFingerprintID

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
		*s = append(*s, appstatspb.StmtFingerprintID(fingerprintID))
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

type stmtFingerprintID appstatspb.StmtFingerprintID

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
		encodeStmtFingerprintIDToString((appstatspb.StmtFingerprintID)(*s))), nil
}

type innerTxnStats appstatspb.TransactionStatistics

func (t *innerTxnStats) decodeJSON(js json.JSON) error {
	// Decode "cnt" field
	if valJSON, err := js.FetchValKey("cnt"); err != nil {
		return err
	} else if valJSON != nil {
		if err := (*jsonInt)(&t.Count).decodeJSON(valJSON); err != nil {
			return err
		}
	}

	// Decode "maxRetries" field
	if valJSON, err := js.FetchValKey("maxRetries"); err != nil {
		return err
	} else if valJSON != nil {
		if err := (*jsonInt)(&t.MaxRetries).decodeJSON(valJSON); err != nil {
			return err
		}
	}

	// Decode "numRows" field
	if valJSON, err := js.FetchValKey("numRows"); err != nil {
		return err
	} else if valJSON != nil {
		if err := (*numericStats)(&t.NumRows).decodeJSON(valJSON); err != nil {
			return err
		}
	}

	// Decode "svcLat" field
	if valJSON, err := js.FetchValKey("svcLat"); err != nil {
		return err
	} else if valJSON != nil {
		if err := (*numericStats)(&t.ServiceLat).decodeJSON(valJSON); err != nil {
			return err
		}
	}

	// Decode "retryLat" field
	if valJSON, err := js.FetchValKey("retryLat"); err != nil {
		return err
	} else if valJSON != nil {
		if err := (*numericStats)(&t.RetryLat).decodeJSON(valJSON); err != nil {
			return err
		}
	}

	// Decode "commitLat" field
	if valJSON, err := js.FetchValKey("commitLat"); err != nil {
		return err
	} else if valJSON != nil {
		if err := (*numericStats)(&t.CommitLat).decodeJSON(valJSON); err != nil {
			return err
		}
	}

	// Decode "idleLat" field
	if valJSON, err := js.FetchValKey("idleLat"); err != nil {
		return err
	} else if valJSON != nil {
		if err := (*numericStats)(&t.IdleLat).decodeJSON(valJSON); err != nil {
			return err
		}
	}

	// Decode "bytesRead" field
	if valJSON, err := js.FetchValKey("bytesRead"); err != nil {
		return err
	} else if valJSON != nil {
		if err := (*numericStats)(&t.BytesRead).decodeJSON(valJSON); err != nil {
			return err
		}
	}

	// Decode "rowsRead" field
	if valJSON, err := js.FetchValKey("rowsRead"); err != nil {
		return err
	} else if valJSON != nil {
		if err := (*numericStats)(&t.RowsRead).decodeJSON(valJSON); err != nil {
			return err
		}
	}

	// Decode "rowsWritten" field
	if valJSON, err := js.FetchValKey("rowsWritten"); err != nil {
		return err
	} else if valJSON != nil {
		if err := (*numericStats)(&t.RowsWritten).decodeJSON(valJSON); err != nil {
			return err
		}
	}

	return nil
}

func (t *innerTxnStats) encodeJSON() (json.JSON, error) {
	builder := json.NewObjectBuilder(10)

	// Create a shared FixedKeysObjectBuilder for numericStats objects
	numericStatsBuilder, err := json.NewFixedKeysObjectBuilder([]string{"mean", "sqDiff"})
	if err != nil {
		return nil, err
	}

	val, err := (*jsonInt)(&t.Count).encodeJSON()
	if err != nil {
		return nil, err
	}
	builder.Add("cnt", val)
	val, err = (*jsonInt)(&t.MaxRetries).encodeJSON()
	if err != nil {
		return nil, err
	}
	builder.Add("maxRetries", val)
	val, err = (*numericStats)(&t.NumRows).encodeJSONWithBuilder(numericStatsBuilder)
	if err != nil {
		return nil, err
	}
	builder.Add("numRows", val)
	val, err = (*numericStats)(&t.ServiceLat).encodeJSONWithBuilder(numericStatsBuilder)
	if err != nil {
		return nil, err
	}
	builder.Add("svcLat", val)
	val, err = (*numericStats)(&t.RetryLat).encodeJSONWithBuilder(numericStatsBuilder)
	if err != nil {
		return nil, err
	}
	builder.Add("retryLat", val)
	val, err = (*numericStats)(&t.CommitLat).encodeJSONWithBuilder(numericStatsBuilder)
	if err != nil {
		return nil, err
	}
	builder.Add("commitLat", val)
	val, err = (*numericStats)(&t.IdleLat).encodeJSONWithBuilder(numericStatsBuilder)
	if err != nil {
		return nil, err
	}
	builder.Add("idleLat", val)
	val, err = (*numericStats)(&t.BytesRead).encodeJSONWithBuilder(numericStatsBuilder)
	if err != nil {
		return nil, err
	}
	builder.Add("bytesRead", val)
	val, err = (*numericStats)(&t.RowsRead).encodeJSONWithBuilder(numericStatsBuilder)
	if err != nil {
		return nil, err
	}
	builder.Add("rowsRead", val)
	val, err = (*numericStats)(&t.RowsWritten).encodeJSONWithBuilder(numericStatsBuilder)
	if err != nil {
		return nil, err
	}
	builder.Add("rowsWritten", val)
	return builder.Build(), nil
}

type innerStmtStats appstatspb.StatementStatistics

func (s *innerStmtStats) decodeJSON(js json.JSON) (err error) {
	var fieldName string
	defer func() {
		if err != nil {
			err = errors.Wrapf(err, "decoding field %s", fieldName)
		}
	}()

	iter, err := js.ObjectIter()
	if err != nil {
		return err
	}
	for ok := iter.Next(); ok; ok = iter.Next() {
		switch iter.Key() {
		case "cnt":
			err := (*jsonInt)(&s.Count).decodeJSON(iter.Value())
			if err != nil {
				return err
			}
		case "firstAttemptCnt":
			err := (*jsonInt)(&s.FirstAttemptCount).decodeJSON(iter.Value())
			if err != nil {
				return err
			}
		case "maxRetries":
			err := (*jsonInt)(&s.MaxRetries).decodeJSON(iter.Value())
			if err != nil {
				return err
			}
		case "lastExecAt":
			err := (*jsonTime)(&s.LastExecTimestamp).decodeJSON(iter.Value())
			if err != nil {
				return err
			}
		case "numRows":
			err := (*numericStats)(&s.NumRows).decodeJSON(iter.Value())
			if err != nil {
				return err
			}
		case "idleLat":
			err := (*numericStats)(&s.IdleLat).decodeJSON(iter.Value())
			if err != nil {
				return err
			}
		case "parseLat":
			err := (*numericStats)(&s.ParseLat).decodeJSON(iter.Value())
			if err != nil {
				return err
			}
		case "planLat":
			err := (*numericStats)(&s.PlanLat).decodeJSON(iter.Value())
			if err != nil {
				return err
			}
		case "runLat":
			err := (*numericStats)(&s.RunLat).decodeJSON(iter.Value())
			if err != nil {
				return err
			}
		case "svcLat":
			err := (*numericStats)(&s.ServiceLat).decodeJSON(iter.Value())
			if err != nil {
				return err
			}
		case "ovhLat":
			err := (*numericStats)(&s.OverheadLat).decodeJSON(iter.Value())
			if err != nil {
				return err
			}
		case "bytesRead":
			err := (*numericStats)(&s.BytesRead).decodeJSON(iter.Value())
			if err != nil {
				return err
			}
		case "rowsRead":
			err := (*numericStats)(&s.RowsRead).decodeJSON(iter.Value())
			if err != nil {
				return err
			}
		case "rowsWritten":
			err := (*numericStats)(&s.RowsWritten).decodeJSON(iter.Value())
			if err != nil {
				return err
			}
		case "nodes":
			err := (*int64Array)(&s.Nodes).decodeJSON(iter.Value())
			if err != nil {
				return err
			}
		case "kvNodeIds":
			err := (*int32Array)(&s.KVNodeIDs).decodeJSON(iter.Value())
			if err != nil {
				return err
			}
		case "regions":
			err := (*stringArray)(&s.Regions).decodeJSON(iter.Value())
			if err != nil {
				return err
			}
		case "usedFollowerRead":
			err := (*jsonBool)(&s.UsedFollowerRead).decodeJSON(iter.Value())
			if err != nil {
				return err
			}
		case "planGists":
			err := (*stringArray)(&s.PlanGists).decodeJSON(iter.Value())
			if err != nil {
				return err
			}
		case "indexes":
			err := (*stringArray)(&s.Indexes).decodeJSON(iter.Value())
			if err != nil {
				return err
			}
		case "latencyInfo":
			err := (*latencyInfo)(&s.LatencyInfo).decodeJSON(iter.Value())
			if err != nil {
				return err
			}
		case "lastErrorCode":
			err := (*jsonString)(&s.LastErrorCode).decodeJSON(iter.Value())
			if err != nil {
				return err
			}
		case "failureCount":
			err := (*jsonInt)(&s.FailureCount).decodeJSON(iter.Value())
			if err != nil {
				return err
			}
		case "genericCount":
			err := (*jsonInt)(&s.GenericCount).decodeJSON(iter.Value())
			if err != nil {
				return err
			}
		case "sqlType":
			err := (*jsonString)(&s.SQLType).decodeJSON(iter.Value())
			if err != nil {
				return err
			}
		}
	}

	return nil
}

func (s *innerStmtStats) encodeJSON() (json.JSON, error) {
	builder := json.NewObjectBuilder(24)

	// Create a shared FixedKeysObjectBuilder for numericStats objects
	numericStatsBuilder, err := json.NewFixedKeysObjectBuilder([]string{"mean", "sqDiff"})
	if err != nil {
		return nil, err
	}
	val, err := (*jsonInt)(&s.Count).encodeJSON()
	if err != nil {
		return nil, err
	}
	builder.Add("cnt", val)
	val, err = (*jsonInt)(&s.FirstAttemptCount).encodeJSON()
	if err != nil {
		return nil, err
	}
	builder.Add("firstAttemptCnt", val)
	val, err = (*jsonInt)(&s.MaxRetries).encodeJSON()
	if err != nil {
		return nil, err
	}
	builder.Add("maxRetries", val)
	val, err = (*jsonTime)(&s.LastExecTimestamp).encodeJSON()
	if err != nil {
		return nil, err
	}
	builder.Add("lastExecAt", val)
	val, err = (*numericStats)(&s.NumRows).encodeJSONWithBuilder(numericStatsBuilder)
	if err != nil {
		return nil, err
	}
	builder.Add("numRows", val)
	val, err = (*numericStats)(&s.IdleLat).encodeJSONWithBuilder(numericStatsBuilder)
	if err != nil {
		return nil, err
	}
	builder.Add("idleLat", val)
	val, err = (*numericStats)(&s.ParseLat).encodeJSONWithBuilder(numericStatsBuilder)
	if err != nil {
		return nil, err
	}
	builder.Add("parseLat", val)
	val, err = (*numericStats)(&s.PlanLat).encodeJSONWithBuilder(numericStatsBuilder)
	if err != nil {
		return nil, err
	}
	builder.Add("planLat", val)
	val, err = (*numericStats)(&s.RunLat).encodeJSONWithBuilder(numericStatsBuilder)
	if err != nil {
		return nil, err
	}
	builder.Add("runLat", val)
	val, err = (*numericStats)(&s.ServiceLat).encodeJSONWithBuilder(numericStatsBuilder)
	if err != nil {
		return nil, err
	}
	builder.Add("svcLat", val)
	val, err = (*numericStats)(&s.OverheadLat).encodeJSONWithBuilder(numericStatsBuilder)
	if err != nil {
		return nil, err
	}
	builder.Add("ovhLat", val)
	val, err = (*numericStats)(&s.BytesRead).encodeJSONWithBuilder(numericStatsBuilder)
	if err != nil {
		return nil, err
	}
	builder.Add("bytesRead", val)
	val, err = (*numericStats)(&s.RowsRead).encodeJSONWithBuilder(numericStatsBuilder)
	if err != nil {
		return nil, err
	}
	builder.Add("rowsRead", val)
	val, err = (*numericStats)(&s.RowsWritten).encodeJSONWithBuilder(numericStatsBuilder)
	if err != nil {
		return nil, err
	}
	builder.Add("rowsWritten", val)
	val, err = (*int64Array)(&s.Nodes).encodeJSON()
	if err != nil {
		return nil, err
	}
	builder.Add("nodes", val)
	val, err = (*int32Array)(&s.KVNodeIDs).encodeJSON()
	if err != nil {
		return nil, err
	}
	builder.Add("kvNodeIds", val)
	val, err = (*stringArray)(&s.Regions).encodeJSON()
	if err != nil {
		return nil, err
	}
	builder.Add("regions", val)
	val, err = (*jsonBool)(&s.UsedFollowerRead).encodeJSON()
	if err != nil {
		return nil, err
	}
	builder.Add("usedFollowerRead", val)
	val, err = (*stringArray)(&s.PlanGists).encodeJSON()
	if err != nil {
		return nil, err
	}
	builder.Add("planGists", val)
	val, err = (*stringArray)(&s.Indexes).encodeJSON()
	if err != nil {
		return nil, err
	}
	builder.Add("indexes", val)
	val, err = (*latencyInfo)(&s.LatencyInfo).encodeJSON()
	if err != nil {
		return nil, err
	}
	builder.Add("latencyInfo", val)
	val, err = (*jsonString)(&s.LastErrorCode).encodeJSON()
	if err != nil {
		return nil, err
	}
	builder.Add("lastErrorCode", val)
	val, err = (*jsonInt)(&s.FailureCount).encodeJSON()
	if err != nil {
		return nil, err
	}
	builder.Add("failureCount", val)
	val, err = (*jsonInt)(&s.GenericCount).encodeJSON()
	if err != nil {
		return nil, err
	}
	builder.Add("genericCount", val)
	val, err = (*jsonString)(&s.SQLType).encodeJSON()
	if err != nil {
		return nil, err
	}
	builder.Add("sqlType", val)
	return builder.Build(), nil
}

type execStats appstatspb.ExecStats

func (e *execStats) decodeJSON(js json.JSON) (err error) {
	var fieldName string
	defer func() {
		if err != nil {
			err = errors.Wrapf(err, "decoding field %s", fieldName)
		}
	}()

	iter, err := js.ObjectIter()
	if err != nil {
		return err
	}
	for ok := iter.Next(); ok; ok = iter.Next() {
		switch iter.Key() {
		case "cnt":
			err := (*jsonInt)(&e.Count).decodeJSON(iter.Value())
			if err != nil {
				return err
			}
		case "networkBytes":
			err := (*numericStats)(&e.NetworkBytes).decodeJSON(iter.Value())
			if err != nil {
				return err
			}
		case "maxMemUsage":
			err := (*numericStats)(&e.MaxMemUsage).decodeJSON(iter.Value())
			if err != nil {
				return err
			}
		case "contentionTime":
			err := (*numericStats)(&e.ContentionTime).decodeJSON(iter.Value())
			if err != nil {
				return err
			}
		case "networkMsgs":
			err := (*numericStats)(&e.NetworkMessages).decodeJSON(iter.Value())
			if err != nil {
				return err
			}
		case "maxDiskUsage":
			err := (*numericStats)(&e.MaxDiskUsage).decodeJSON(iter.Value())
			if err != nil {
				return err
			}
		case "cpuSQLNanos":
			err := (*numericStats)(&e.CPUSQLNanos).decodeJSON(iter.Value())
			if err != nil {
				return err
			}
		case "mvccIteratorStats":
			err := (*iteratorStats)(&e.MVCCIteratorStats).decodeJSON(iter.Value())
			if err != nil {
				return err
			}
		}
	}

	return nil
}

func (e *execStats) encodeJSON() (json.JSON, error) {
	builder := json.NewObjectBuilder(8)

	// Create a shared FixedKeysObjectBuilder for numericStats objects
	numericStatsBuilder, err := json.NewFixedKeysObjectBuilder([]string{"mean", "sqDiff"})
	if err != nil {
		return nil, err
	}

	val, err := (*jsonInt)(&e.Count).encodeJSON()
	if err != nil {
		return nil, err
	}
	builder.Add("cnt", val)
	val, err = (*numericStats)(&e.NetworkBytes).encodeJSONWithBuilder(numericStatsBuilder)
	if err != nil {
		return nil, err
	}
	builder.Add("networkBytes", val)
	val, err = (*numericStats)(&e.MaxMemUsage).encodeJSONWithBuilder(numericStatsBuilder)
	if err != nil {
		return nil, err
	}
	builder.Add("maxMemUsage", val)
	val, err = (*numericStats)(&e.ContentionTime).encodeJSONWithBuilder(numericStatsBuilder)
	if err != nil {
		return nil, err
	}
	builder.Add("contentionTime", val)
	val, err = (*numericStats)(&e.NetworkMessages).encodeJSONWithBuilder(numericStatsBuilder)
	if err != nil {
		return nil, err
	}
	builder.Add("networkMsgs", val)
	val, err = (*numericStats)(&e.MaxDiskUsage).encodeJSONWithBuilder(numericStatsBuilder)
	if err != nil {
		return nil, err
	}
	builder.Add("maxDiskUsage", val)
	val, err = (*numericStats)(&e.CPUSQLNanos).encodeJSONWithBuilder(numericStatsBuilder)
	if err != nil {
		return nil, err
	}
	builder.Add("cpuSQLNanos", val)
	val, err = (*iteratorStats)(&e.MVCCIteratorStats).encodeJSON()
	if err != nil {
		return nil, err
	}
	builder.Add("mvccIteratorStats", val)
	return builder.Build(), nil
}

type iteratorStats appstatspb.MVCCIteratorStats

func (e *iteratorStats) decodeJSON(js json.JSON) (err error) {
	var fieldName string
	defer func() {
		if err != nil {
			err = errors.Wrapf(err, "decoding field %s", fieldName)
		}
	}()

	iter, err := js.ObjectIter()
	if err != nil {
		return err
	}
	for ok := iter.Next(); ok; ok = iter.Next() {
		switch iter.Key() {
		case "stepCount":
			err := (*numericStats)(&e.StepCount).decodeJSON(iter.Value())
			if err != nil {
				return err
			}
		case "stepCountInternal":
			err := (*numericStats)(&e.StepCountInternal).decodeJSON(iter.Value())
			if err != nil {
				return err
			}
		case "seekCount":
			err := (*numericStats)(&e.SeekCount).decodeJSON(iter.Value())
			if err != nil {
				return err
			}
		case "seekCountInternal":
			err := (*numericStats)(&e.SeekCountInternal).decodeJSON(iter.Value())
			if err != nil {
				return err
			}
		case "blockBytes":
			err := (*numericStats)(&e.BlockBytes).decodeJSON(iter.Value())
			if err != nil {
				return err
			}
		case "blockBytesInCache":
			err := (*numericStats)(&e.BlockBytesInCache).decodeJSON(iter.Value())
			if err != nil {
				return err
			}
		case "keyBytes":
			err := (*numericStats)(&e.KeyBytes).decodeJSON(iter.Value())
			if err != nil {
				return err
			}
		case "valueBytes":
			err := (*numericStats)(&e.ValueBytes).decodeJSON(iter.Value())
			if err != nil {
				return err
			}
		case "pointCount":
			err := (*numericStats)(&e.PointCount).decodeJSON(iter.Value())
			if err != nil {
				return err
			}
		case "pointsCoveredByRangeTombstones":
			err := (*numericStats)(&e.PointsCoveredByRangeTombstones).decodeJSON(iter.Value())
			if err != nil {
				return err
			}
		case "rangeKeyCount":
			err := (*numericStats)(&e.RangeKeyCount).decodeJSON(iter.Value())
			if err != nil {
				return err
			}
		case "rangeKeyContainedPoints":
			err := (*numericStats)(&e.RangeKeyContainedPoints).decodeJSON(iter.Value())
			if err != nil {
				return err
			}
		case "rangeKeySkippedPoints":
			err := (*numericStats)(&e.RangeKeySkippedPoints).decodeJSON(iter.Value())
			if err != nil {
				return err
			}
		}
	}

	return nil
}

func (e *iteratorStats) encodeJSON() (json.JSON, error) {
	builder := json.NewObjectBuilder(13)

	// Create a shared FixedKeysObjectBuilder for numericStats objects
	numericStatsBuilder, err := json.NewFixedKeysObjectBuilder([]string{"mean", "sqDiff"})
	if err != nil {
		return nil, err
	}

	val, err := (*numericStats)(&e.StepCount).encodeJSONWithBuilder(numericStatsBuilder)
	if err != nil {
		return nil, err
	}
	builder.Add("stepCount", val)
	val, err = (*numericStats)(&e.StepCountInternal).encodeJSONWithBuilder(numericStatsBuilder)
	if err != nil {
		return nil, err
	}
	builder.Add("stepCountInternal", val)
	val, err = (*numericStats)(&e.SeekCount).encodeJSONWithBuilder(numericStatsBuilder)
	if err != nil {
		return nil, err
	}
	builder.Add("seekCount", val)
	val, err = (*numericStats)(&e.SeekCountInternal).encodeJSONWithBuilder(numericStatsBuilder)
	if err != nil {
		return nil, err
	}
	builder.Add("seekCountInternal", val)
	val, err = (*numericStats)(&e.BlockBytes).encodeJSONWithBuilder(numericStatsBuilder)
	if err != nil {
		return nil, err
	}
	builder.Add("blockBytes", val)
	val, err = (*numericStats)(&e.BlockBytesInCache).encodeJSONWithBuilder(numericStatsBuilder)
	if err != nil {
		return nil, err
	}
	builder.Add("blockBytesInCache", val)
	val, err = (*numericStats)(&e.KeyBytes).encodeJSONWithBuilder(numericStatsBuilder)
	if err != nil {
		return nil, err
	}
	builder.Add("keyBytes", val)
	val, err = (*numericStats)(&e.ValueBytes).encodeJSONWithBuilder(numericStatsBuilder)
	if err != nil {
		return nil, err
	}
	builder.Add("valueBytes", val)
	val, err = (*numericStats)(&e.PointCount).encodeJSONWithBuilder(numericStatsBuilder)
	if err != nil {
		return nil, err
	}
	builder.Add("pointCount", val)
	val, err = (*numericStats)(&e.PointsCoveredByRangeTombstones).encodeJSONWithBuilder(numericStatsBuilder)
	if err != nil {
		return nil, err
	}
	builder.Add("pointsCoveredByRangeTombstones", val)
	val, err = (*numericStats)(&e.RangeKeyCount).encodeJSONWithBuilder(numericStatsBuilder)
	if err != nil {
		return nil, err
	}
	builder.Add("rangeKeyCount", val)
	val, err = (*numericStats)(&e.RangeKeyContainedPoints).encodeJSONWithBuilder(numericStatsBuilder)
	if err != nil {
		return nil, err
	}
	builder.Add("rangeKeyContainedPoints", val)
	val, err = (*numericStats)(&e.RangeKeySkippedPoints).encodeJSONWithBuilder(numericStatsBuilder)
	if err != nil {
		return nil, err
	}
	builder.Add("rangeKeySkippedPoints", val)
	return builder.Build(), nil
}

type numericStats appstatspb.NumericStat

func (n *numericStats) decodeJSON(js json.JSON) error {
	// Decode "mean" field
	if valJSON, err := js.FetchValKey("mean"); err != nil {
		return err
	} else if valJSON != nil {
		if err := (*jsonFloat)(&n.Mean).decodeJSON(valJSON); err != nil {
			return err
		}
	}

	// Decode "sqDiff" field
	if valJSON, err := js.FetchValKey("sqDiff"); err != nil {
		return err
	} else if valJSON != nil {
		if err := (*jsonFloat)(&n.SquaredDiffs).decodeJSON(valJSON); err != nil {
			return err
		}
	}

	return nil
}

func (n *numericStats) encodeJSON() (json.JSON, error) {
	builder := json.NewObjectBuilder(2)
	val, err := (*jsonFloat)(&n.Mean).encodeJSON()
	if err != nil {
		return nil, err
	}
	builder.Add("mean", val)
	val, err = (*jsonFloat)(&n.SquaredDiffs).encodeJSON()
	if err != nil {
		return nil, err
	}
	builder.Add("sqDiff", val)
	return builder.Build(), nil
}

func (n *numericStats) encodeJSONWithBuilder(
	builder *json.FixedKeysObjectBuilder,
) (json.JSON, error) {
	meanVal, err := (*jsonFloat)(&n.Mean).encodeJSON()
	if err != nil {
		return nil, err
	}
	if err := builder.Set("mean", meanVal); err != nil {
		return nil, err
	}

	sqDiffVal, err := (*jsonFloat)(&n.SquaredDiffs).encodeJSON()
	if err != nil {
		return nil, err
	}
	if err := builder.Set("sqDiff", sqDiffVal); err != nil {
		return nil, err
	}

	return builder.Build()
}

type latencyInfo appstatspb.LatencyInfo

func (l *latencyInfo) decodeJSON(js json.JSON) error {
	// Decode "min" field
	if valJSON, err := js.FetchValKey("min"); err != nil {
		return err
	} else if valJSON != nil {
		if err := (*jsonFloat)(&l.Min).decodeJSON(valJSON); err != nil {
			return err
		}
	}

	// Decode "max" field
	if valJSON, err := js.FetchValKey("max"); err != nil {
		return err
	} else if valJSON != nil {
		if err := (*jsonFloat)(&l.Max).decodeJSON(valJSON); err != nil {
			return err
		}
	}

	return nil
}

func (l *latencyInfo) encodeJSON() (json.JSON, error) {
	builder := json.NewObjectBuilder(2)
	val, err := (*jsonFloat)(&l.Min).encodeJSON()
	if err != nil {
		return nil, err
	}
	builder.Add("min", val)
	val, err = (*jsonFloat)(&l.Max).encodeJSON()
	if err != nil {
		return nil, err
	}
	builder.Add("max", val)
	return builder.Build(), nil
}

type jsonFields []jsonField

func (jf jsonFields) decodeJSON(js json.JSON) (err error) {
	var fieldName string
	defer func() {
		if err != nil {
			err = errors.Wrapf(err, "decoding field %s", fieldName)
		}
	}()

	iter, err := js.ObjectIter()
	if err != nil {
		return err
	}
	for ok := iter.Next(); ok; ok = iter.Next() {
		for i := range jf {
			if jf[i].field == iter.Key() {
				err := jf[i].val.decodeJSON(iter.Value())
				if err != nil {
					return err
				}
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
	// Tolerate provided nil JSON value as valid case and interpret
	// it as null result.
	if js == nil {
		*s = "<null>"
		return nil
	}
	text, err := js.AsText()
	if err != nil {
		return err
	}
	if text != nil {
		*s = (jsonString)(*text)
	} else {
		*s = "<null>"
	}
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
