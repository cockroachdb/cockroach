package replicationslot

type Item struct {
	LSN        int64
	XID        int64
	Key, Value []byte
}

var ReplicationSlots = make(map[string]replicationSlotLooker)

type replicationSlotLooker interface {
	NextTxn(consume bool) []Item
}
