package replicationslot

import (
	"fmt"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/errors"
)

type Item struct {
	LSN        uint64
	XID        int64
	Key, Value []byte
	MVCC       hlc.Timestamp
}

var ReplicationSlots = make(map[string]replicationSlotLooker)

type replicationSlotLooker interface {
	NextTxn(consume bool) []Item
	LSN() uint64
	SetDatabase(s string)
	Database() string
}

func FormatLSN(lsn uint64) string {
	return fmt.Sprintf("%08X/%08X", lsn>>32, lsn&0xFFFFFFFF)
}

// Parse the given XXX/XXX text format LSN used by PostgreSQL.
func ParseLSN(s string) uint64 {
	var upperHalf uint64
	var lowerHalf uint64
	var nparsed int
	nparsed, err := fmt.Sscanf(s, "%X/%X", &upperHalf, &lowerHalf)
	if err != nil {
		panic(err)
	}

	if nparsed != 2 {
		panic(errors.AssertionFailedf("need 2 args"))
	}

	return (upperHalf << 32) + lowerHalf
}

// Hacking for now. This should probably be changed to use avroDataRecord
// NOTE: untested
func ParseJSONValueForPGLogicalPayload(s string) string {
	// First value found is the record type
	recordType, colValues, found := strings.Cut(s, ":")
	if !found {
		return ""
	}
	recordType = strings.Trim(recordType, "{\"")
	var recordString string
	if recordType == "resolved" {
		// This record can be ignored for pg logical replication
		return ""
	}
	if recordType == "after" {
		// Peek ahead to see if this is an INSERT or a DELETE
		if strings.Contains(recordType, "after:null") {
			recordString = "DELETE"
		} else {
			recordString = "INSERT"
		}
	} else {
		// FIXME: still have to handle UPDATE
		panic("Unhandled log record type")
	}

	// Because hard coding things is so en vogue these days
	tableName := "test_table"
	payload := fmt.Sprintf("table %s: %s:", tableName, recordString)

	// Loop through all key values and populate a string based on the trimming/replacing used for keys below
	colValue, colValues, found := strings.Cut(colValues, ",")
	for ; found; colValue, colValues, found = strings.Cut(colValues, ",") {
		// This is ugly and very inefficient.  Fix it!
		colValue = strings.ReplaceAll(colValue, "{", "")
		colValue = strings.ReplaceAll(colValue, "}", "")
		colValue = strings.ReplaceAll(colValue, "\"", "")
		colValue = strings.ReplaceAll(colValue, " ", "")
		// And while we're hard coding, hard code the type too
		// This part is especially bad, and will prevent DMS from working
		colValue = strings.Replace(colValue, ":", "[int]:", 1)
		payload = fmt.Sprintf("%s %s", payload, colValue)
	}
	// Handle last column
	// This is ugly and very inefficient.  Fix it!
	colValue = strings.ReplaceAll(colValue, "{", "")
	colValue = strings.ReplaceAll(colValue, "}", "")
	colValue = strings.ReplaceAll(colValue, "\"", "")
	colValue = strings.ReplaceAll(colValue, " ", "")
	// And while we're hard coding, hard code the type too
	colValue = strings.Replace(colValue, ":", "[int]:", 1)
	payload = fmt.Sprintf("%s %s", payload, colValue)

	// FIXME: need to figure out how to do null handling for DELETE after
	//  debugging.
	return payload
}
