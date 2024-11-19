// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package rangelogtestpb

import (
	"bytes"
	"encoding/json"
	"strconv"
	"time"

	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvserverpb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/encoding/csv"
	"github.com/cockroachdb/errors"
)

// These constants describe the structure of the rows passed to
// ParseRow and ParseCSV. This row structure corresponds to the
// structure of system.rangelog.
const (
	TimestampIdx int = iota
	RangeIDIdx
	StoreIDIdx
	EventTypeIdx
	OtherRangeIDIdx
	InfoIdx
	UniqueIDIdx
	expLen
)

// ParseCSV parses csv data of a rangelog into the protobuf struct.
func ParseCSV(data []byte) (*RangeLogData, error) {
	r := csv.NewReader(bytes.NewReader(data))
	records, err := r.ReadAll()
	if err != nil {
		return nil, err
	}
	// Maybe trim the header row.
	if len(records) > 0 &&
		len(records[0]) > 0 &&
		records[0][0].Val == "timestamp" {
		records = records[1:]
	}
	return ParseRows(csvRecordsToStrings(records))
}

func csvRecordsToStrings(records [][]csv.Record) [][]string {
	rows := make([][]string, 0, len(records))
	for _, record := range records {
		row := make([]string, 0, len(record))
		for i := range record {
			row = append(row, record[i].Val)
		}
		rows = append(rows, row)
	}
	return rows
}

// ParseRows parses row data into RangeLogData. Note that the parsing
// should be done using the same version which produced the data as
// json field name stability across versions is not guaranteed.
func ParseRows(rows [][]string) (*RangeLogData, error) {
	var ret RangeLogData
	for i, row := range rows {
		ev, uniqueID, err := parseRangeLogRow(row)
		if err != nil {
			return nil, errors.Wrapf(err, "failed to parse row %d", i)
		}
		ret.Events = append(ret.Events, ev)
		ret.UniqueIds = append(ret.UniqueIds, uniqueID)
	}
	return &ret, nil
}

func parseRangeLogRow(data []string) (*kvserverpb.RangeLogEvent, int64, error) {
	if len(data) != expLen {
		return nil, 0, errors.Errorf("parseEventLogRow: expected %d entries, got %d", expLen, len(data))
	}
	var (
		parseTimestamp = func(s string) (time.Time, error) {
			const (
				timeFormat         = "2006-01-02 15:04:05.999999"
				timeFormatWithZone = timeFormat + " -0700 MST"
			)
			ts, err := time.Parse(timeFormat, s)
			if err != nil {
				ts, err = time.Parse(timeFormatWithZone, s)
			}
			if err == nil {
				ts = ts.UTC()
			}
			return ts, err
		}
		parseInt = func(s string) (int64, error) {
			switch s {
			case "", "NULL":
				return 0, nil
			default:
				return strconv.ParseInt(s, 10, 64)
			}
		}
		parseRangeID = func(s string) (roachpb.RangeID, error) {
			id, err := parseInt(s)
			return roachpb.RangeID(id), err
		}
		parseStoreID = func(s string) (roachpb.StoreID, error) {
			id, err := parseInt(s)
			return roachpb.StoreID(id), err
		}
		parseEventType = func(s string) (kvserverpb.RangeLogEventType, error) {
			if v, ok := kvserverpb.RangeLogEventType_value[s]; ok {
				return kvserverpb.RangeLogEventType(v), nil
			}
			return 0, errors.Errorf("unknown event type %q", s)
		}
	)
	var ev kvserverpb.RangeLogEvent
	var err error
	if ev.Timestamp, err = parseTimestamp(data[TimestampIdx]); err != nil {
		return nil, 0, errors.Wrap(err, "parseEventLogRow: parsing timestamp")
	}
	if ev.RangeID, err = parseRangeID(data[RangeIDIdx]); err != nil {
		return nil, 0, errors.Wrap(err, "parseEventLogRow: parsing rangeID")
	}
	if ev.StoreID, err = parseStoreID(data[StoreIDIdx]); err != nil {
		return nil, 0, errors.Wrap(err, "parseEventLogRow: parsing storeID")
	}
	if ev.OtherRangeID, err = parseRangeID(data[OtherRangeIDIdx]); err != nil {
		return nil, 0, errors.Wrap(err, "parseEventLogRow: parsing otherRangeID")
	}
	if ev.EventType, err = parseEventType(data[EventTypeIdx]); err != nil {
		return nil, 0, errors.Wrap(err, "parseEventLogRow: parsing eventType")
	}
	ev.Info = new(kvserverpb.RangeLogEvent_Info)
	if err := json.Unmarshal([]byte(data[InfoIdx]), ev.Info); err != nil {
		return nil, 0, errors.Wrap(err, "parseEventLogRow: parsing info")

	}
	rowID, err := parseInt(data[UniqueIDIdx])
	if err != nil {
		return nil, 0, errors.Wrap(err, "parseEventLogRow: parsing uniqueID")
	}
	return &ev, rowID, nil
}
