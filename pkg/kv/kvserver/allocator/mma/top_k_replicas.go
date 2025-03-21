package mma

import (
	"container/heap"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
)

type topKReplicas struct {
	k   int
	dim LoadDimension
	// Decreasing load.
	replicas    []replicaLoad
	replicaHeap replicaHeap
}

type replicaLoad struct {
	roachpb.RangeID
	load LoadValue
}

// Reset when using a StoreLeaseholderMsg to reevaluate from scratch.
func (t *topKReplicas) startInit() {
	t.replicaHeap = t.replicaHeap[:0]
}

func (t *topKReplicas) addReplica(rangeID roachpb.RangeID, loadValue LoadValue) {
	rl := replicaLoad{
		RangeID: rangeID,
		load:    loadValue,
	}
	if t.replicaHeap.Len() >= t.k {
		if less(t.replicaHeap[0], rl) {
			heap.Pop(&t.replicaHeap)
		} else {
			return
		}
	}
	heap.Push(&t.replicaHeap, rl)
}

func (t *topKReplicas) doneInit() {
	n := t.replicaHeap.Len()
	if cap(t.replicas) < n {
		t.replicas = make([]replicaLoad, n)
	} else {
		t.replicas = t.replicas[:n]
	}
	for i := n - 1; i >= 0; i-- {
		t.replicas[i] = heap.Pop(&t.replicaHeap).(replicaLoad)
	}
}

func (t *topKReplicas) len() int {
	if t == nil {
		return 0
	}
	return len(t.replicas)
}

func (t *topKReplicas) index(i int) roachpb.RangeID {
	return t.replicas[i].RangeID
}

const numTopKReplicas = 128

type replicaHeap []replicaLoad

var _ heap.Interface = (*replicaHeap)(nil)

func (h *replicaHeap) Len() int {
	return len(*h)
}

func (h *replicaHeap) Less(i, j int) bool {
	return less((*h)[i], (*h)[j])
}

func less(a, b replicaLoad) bool {
	if a.load == b.load {
		return a.RangeID < b.RangeID
	}
	return a.load < b.load
}

func (h *replicaHeap) Swap(i, j int) {
	(*h)[i], (*h)[j] = (*h)[j], (*h)[i]
}

func (h *replicaHeap) Push(x interface{}) {
	item := x.(replicaLoad)
	*h = append(*h, item)
}

func (h *replicaHeap) Pop() interface{} {
	old := *h
	n := len(old)
	item := old[n-1]
	old[n-1] = replicaLoad{}
	*h = old[0 : n-1]
	return item
}
