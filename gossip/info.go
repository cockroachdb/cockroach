package gossip

import "strings"

type Value interface {
	Less(b Value) bool
}

type Float64Value float64

func (a Float64Value) Less(b Value) bool {
	return a < b.(Float64Value)
}

// Info objects are the basic unit of information traded over the
// gossip network.
type Info struct {
	Key       string // Info key
	Val       Value  // Info value
	Timestamp int64  // Wall time at origination (Unix-nanos)
	TTLStamp  int64  // Wall time before info is discarded (Unix-nanos)
	Seq       int64  // Sequence number for incremental updates
	Node      string // Originating node name
	Hops      uint32 // Number of hops from originator
}

func InfoPrefix(key string) string {
	if index := strings.LastIndex(key, "."); index != -1 {
		return key[:index]
	}
	return ""
}

type InfoMap map[string]*Info
type InfoArray []*Info

// Implement sort.Interface for InfoArray.
func (a InfoArray) Len() int           { return len(a) }
func (a InfoArray) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a InfoArray) Less(i, j int) bool { return a[i].Val.Less(a[j].Val) }
