package replicationslot

import "fmt"

type Item struct {
	LSN        int64
	XID        int64
	Key, Value []byte
}

var ReplicationSlots = make(map[string]replicationSlotLooker)

type replicationSlotLooker interface {
	NextTxn(consume bool) []Item
}

func FormatLSN(lsn int64) string {
	// TOTAL HACK ALERT! Putting A in front of everything so
	// it can be recognised as tree.Name.
	return fmt.Sprintf("A0/A%06x", lsn)
}
