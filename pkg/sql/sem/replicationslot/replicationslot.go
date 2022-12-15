package replicationslot

import (
	"context"
	gojson "encoding/json"
	"fmt"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descs"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/errors"
)

type Item struct {
	FullSlotName string
	LSN          uint64
	XID          int64
	Key, Value   []byte
	MVCC         hlc.Timestamp
}

var ReplicationSlots = make(map[string]replicationSlotLooker)
var ReplicationSlotsMu syncutil.Mutex

type replicationSlotLooker interface {
	NextTxn(consume bool) []Item
	LSN() uint64
	SetDatabase(s string)
	Database() string
	SetTableID(id descpb.ID)
	TableID() descpb.ID
	SetBaseSlotName(s string)
	GetBaseSlotName() string
	SetFullSlotName(s string)
	GetFullSlotName() string
}

const slotPrefix = "pg_logical"
const SlotNameSeparator = "."

func BuildFullSlotName(slotName string, tableName string) string {
	return slotPrefix + SlotNameSeparator + slotName + SlotNameSeparator + tableName
}

func tableNameFromFullSlotName(fullSlotName string) string {
	_, sn, f := strings.Cut(fullSlotName, slotPrefix+SlotNameSeparator)
	if !f {
		panic(fmt.Sprintf("misformed fullSlotName %s", fullSlotName))
	}
	_, tableName, f := strings.Cut(sn, SlotNameSeparator)
	if !f {
		panic(fmt.Sprintf("misformed fullSlotName %s", fullSlotName))
	}
	return tableName
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

//type SubEntry struct {
//	ColName string
//	Value   string
//}
//
//type Entry struct {
//	MessageType string
//	ColValues   map[string]SubEntry
//}

// Hacking for now. This should probably be changed to use avroDataRecord
// NOTE: untested
func ParseJSONValueForPGLogicalPayload(item Item, txn *kv.Txn, coll *descs.Collection) string {
	slot, ok := ReplicationSlots[item.FullSlotName]
	if !ok {
		panic("unable to find replication slot")
	}
	it, err := coll.GetImmutableTableByID(context.Background(), txn, slot.TableID(), tree.ObjectLookupFlagsWithRequired())
	if err != nil {
		panic("unable to retrieve table descriptor")
	}

	var message map[string]interface{}
	if err := gojson.Unmarshal(item.Value, &message); err != nil {
		panic("unable to marshal CF entry")
	}

	var payload string
	tableName := tableNameFromFullSlotName(item.FullSlotName)
	var recordString string
	_, ok = message["after"]
	// Explicitly ignore everything that's not an "after" message
	if !ok {
		return ""
	}

	b, ok := message["after"].(map[string]interface{})
	if !ok {
		recordString = "DELETE"
		idx := it.GetPrimaryIndex()
		payload = fmt.Sprintf("table %s: %s:", tableName, recordString)

		keyString := string(item.Key)
		keyString = strings.Trim(keyString, "[]")

		// This won't work for compound primary keys.
		for i := 0; i < idx.NumKeyColumns(); i++ {
			for _, col := range it.AllColumns() {
				if col.GetID() == idx.GetKeyColumnID(i) {
					payload = fmt.Sprintf("%s %s[%s]:%s", payload, col.GetName(), col.GetType().SQLStandardName(), keyString)
				}
			}
		}
		return payload
	}

	recordString = "INSERT"
	payload = fmt.Sprintf("table %s: %s:", tableName, recordString)

	// FIXME: Using slotName below is still problematic, because on a table rename,
	//  we may not be able to find the table descriptor.
	for colName, colValue := range b {
		var colType string
		for _, col := range it.AccessibleColumns() {
			if col.ColName() != tree.Name(colName) {
				continue
			}
			colType = col.GetType().String()
			break
		}
		payload = fmt.Sprintf("%s %s[%s]:%v", payload, colName, colType, colValue)
	}

	return payload
}
