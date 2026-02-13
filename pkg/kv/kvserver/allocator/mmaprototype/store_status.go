// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package mmaprototype

import (
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/redact"
)

type (
	// Health represents whether the store "is around", i.e. able to participate
	// in replication. What this means exactly is left to the outside world. MMA
	// relies on the Disposition as much as possible without consulting the health
	// status, aided by the invariant described on Status (only healthy nodes can
	// have "green" dispositions). Its only (envisioned) use for explicit health
	// tracking are sanity checks and preferring unhealthy candidates when picking
	// sources (for example, when rebalancing, it may make sense to rebalance off
	// an unhealthy or dead node over a healthy one as this actually improves
	// availability).
	Health             byte
	LeaseDisposition   byte
	ReplicaDisposition byte
)

const (
	// HealthUnknown is the initial state for health. It's treated as an unhealthy
	// state, distinguished from HealthUnhealthy to tailor logging and metrics.
	HealthUnknown Health = iota
	// HealthOK describes a store that has been determined to be alive and well.
	HealthOK
	// HealthUnhealthy describes a store that has recently become unhealthy, but
	// which has not been unhealthy for "extended periods of time" and is expected
	// to return (as determined by a higher layer)
	HealthUnhealthy
	// HealthDead describes a store that is unhealthy and has been in that state
	// for what is considered a "long time" or which may not return.
	HealthDead
	healthCount // sentinel
)

const (
	// LeaseDispositionOK denotes that the store is in principle willing to
	// receive leases.
	LeaseDispositionOK LeaseDisposition = iota
	// LeaseDispositionRefusing denotes that the store should not be considered
	// as a target for leases.
	LeaseDispositionRefusing
	// LeaseDispositionShedding denotes that the store should actively be
	// considered for removal of leases.
	LeaseDispositionShedding
	leaseDispositionCount // sentinel
)

const (
	// ReplicaDispositionRefusing denotes that the store can receive replicas.
	ReplicaDispositionOK ReplicaDisposition = iota
	// ReplicaDispositionRefusing denotes that the store should not be considered as
	// a target for replica placement.
	ReplicaDispositionRefusing
	// ReplicaDispositionShedding denotes that the store should actively be
	// considered for removal of replicas.
	ReplicaDispositionShedding
	replicaDispositionCount // sentinel
)

// Disposition represents the interest a store has in accepting or
// shedding leases or replicas.
type Disposition struct {
	Lease   LeaseDisposition
	Replica ReplicaDisposition
}

// Status represents the health and disposition of a store. It serves as a
// translation layer between the "messy" outside world in which we may have
// multiple possibly conflicting health signals, and various lifecycle states.
// The Status is never derived internally in mma, but is always passed in and
// kept up to date through the Allocator interface.
//
// The contained Health primarily plays a role when looking for (lease or
// replicas) sources: all other things being equal, we want to be able to
// identify suspect or dead stores as more beneficial sources for, say, a
// replica removal. We also want to be able to tell when a range has lost quorum
// so that we don't even try to make changes in that case. On the flip side, due
// to the INVARIANT below, when looking for targets, health does not typically
// need to be considered as we prohibit "non-healthy" stores from claiming to
// accept replicas or leases.
//
// The wrapped Disposition is important when looking for both targets and
// sources. In principle, stores can reject leases but accept replicas and vice
// versa, though not all combinations may be used in practice (because they do
// not all make sense).
//
// The multi-metric allocator is not currently responsible for establishing
// constraint conformance or for shedding, so not all aspects discussed above
// are necessarily used by it yet.
//
// INVARIANT: if Health != HealthOK, Disposition.{Lease,Replica} = Shedding.
type Status struct {
	Health      Health
	Disposition Disposition
}

func MakeStatus(h Health, l LeaseDisposition, r ReplicaDisposition) Status {
	s := Status{
		Health:      h,
		Disposition: Disposition{Lease: l, Replica: r},
	}
	s.assertOrPanic()
	return s
}

func (ss *Status) assert() error {
	// NB: byte values can't be negative, so we don't have to check.

	if ss.Health >= healthCount {
		return errors.AssertionFailedf("invalid health status %d", ss.Health)
	}
	d := ss.Disposition
	if d.Lease >= leaseDispositionCount {
		return errors.AssertionFailedf("invalid lease disposition %d", d.Lease)
	}
	if d.Replica >= replicaDispositionCount {
		return errors.AssertionFailedf("invalid replica disposition %d", d.Replica)
	}
	if ss.Health != HealthOK && (ss.Disposition.Replica == ReplicaDispositionOK || ss.Disposition.Lease == LeaseDispositionOK) {
		return errors.AssertionFailedf("non-healthy status must not accept leases or replicas: %+v", ss)
	}
	return nil
}

func (ss *Status) assertOrPanic() {
	if err := ss.assert(); err != nil {
		panic(err)
	}
}

func (h Health) String() string {
	return redact.StringWithoutMarkers(h)
}

// SafeFormat implements the redact.SafeFormatter interface.
func (h Health) SafeFormat(w redact.SafePrinter, _ rune) {
	switch h {
	case HealthUnknown:
		w.SafeString("unknown")
	case HealthOK:
		w.SafeString("ok")
	case HealthUnhealthy:
		w.SafeString("unhealthy")
	case HealthDead:
		w.SafeString("dead")
	default:
		w.SafeString("unknown")
	}
}

func (l LeaseDisposition) String() string {
	return redact.StringWithoutMarkers(l)
}

// SafeFormat implements the redact.SafeFormatter interface.
func (l LeaseDisposition) SafeFormat(w redact.SafePrinter, _ rune) {
	switch l {
	case LeaseDispositionOK:
		w.SafeString("ok")
	case LeaseDispositionRefusing:
		w.SafeString("refusing")
	case LeaseDispositionShedding:
		w.SafeString("shedding")
	default:
		w.SafeString("unknown")
	}
}

func (r ReplicaDisposition) String() string {
	return redact.StringWithoutMarkers(r)
}

// SafeFormat implements the redact.SafeFormatter interface.
func (r ReplicaDisposition) SafeFormat(w redact.SafePrinter, _ rune) {
	switch r {
	case ReplicaDispositionOK:
		w.SafeString("ok")
	case ReplicaDispositionRefusing:
		w.SafeString("refusing")
	case ReplicaDispositionShedding:
		w.SafeString("shedding")
	default:
		w.SafeString("unknown")
	}
}

func (d Disposition) String() string {
	return redact.StringWithoutMarkers(d)
}

// SafeFormat implements the redact.SafeFormatter interface.
func (d Disposition) SafeFormat(w redact.SafePrinter, _ rune) {
	if d.Lease == LeaseDispositionOK && d.Replica == ReplicaDispositionOK {
		w.SafeString("accepting all")
		return
	}

	var refusing []redact.SafeString
	var shedding []redact.SafeString

	if d.Lease == LeaseDispositionRefusing {
		refusing = append(refusing, redact.SafeString("leases"))
	} else if d.Lease == LeaseDispositionShedding {
		shedding = append(shedding, redact.SafeString("leases"))
	}

	if d.Replica == ReplicaDispositionRefusing {
		refusing = append(refusing, redact.SafeString("replicas"))
	} else if d.Replica == ReplicaDispositionShedding {
		shedding = append(shedding, redact.SafeString("replicas"))
	}

	first := true
	if len(refusing) > 0 {
		w.SafeString("refusing=")
		for i, s := range refusing {
			if i > 0 {
				w.SafeRune(',')
			}
			w.Print(s)
		}
		first = false
	}
	if len(shedding) > 0 {
		if !first {
			w.SafeRune(' ')
		}
		w.SafeString("shedding=")
		for i, s := range shedding {
			if i > 0 {
				w.SafeRune(',')
			}
			w.Print(s)
		}
	}
}

func (ss Status) String() string {
	return redact.StringWithoutMarkers(ss)
}

// SafeFormat implements the redact.SafeFormatter interface.
func (ss Status) SafeFormat(w redact.SafePrinter, _ rune) {
	ss.Health.SafeFormat(w, 's')
	w.SafeRune(' ')
	ss.Disposition.SafeFormat(w, 's')
}
