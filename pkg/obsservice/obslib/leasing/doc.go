// Copyright 2022 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0

package leasing

// This package implements a distributed leasing library. It is used in the
// Observability Service for assigning the responsibility of monitoring
// individual targets (i.e. CRDB nodes) to Obs Service nodes: an Obs Service
// node takes a time-limited lease on a target, giving it the exclusive right to
// pull data from the respective target. The leasing state is stored in the sink
// CRDB cluster.
//
// This library introduces two concepts: Sessions and Leases. A session
// represents an actor that can take leases. Each Obs Service worker creates a
// session and heartbeats it throughout the worker's lifetime. A session is used
// to acquire leases on specific monitoring targets; a Lease is valid for as
// long as its parent session is valid. Once created, a session is valid for a
// few seconds. A background task periodically heartbeats the session, with each
// heartbeat extending its life further. Once a session expires, the session's
// leases cannot be used anymore. Invalid leases can be stolen by other
// Sessions; each Obs Service worker periodically watches for other nodes'
// expired leases and attempts to steal them.
//
// === API ===
//
// s := NewSession(id, connPool, clock, stop)
// for {
//   _, err := s.Lease(targetID)
//   if errors.Is(err, ErrLeasedByAnother) {
//     // Another session stole the lease. Periodically try to re-acquire the
//     // lease.
//   }
//   // Do something knowing that we have exclusive access to the target
//   // identified by targetID for a while.
// }
//
// === Semantics ===
//
// If Session.Lease() returns success at time t1, then we have a lease that no
// other Session will try to acquire until their local clock is at least
// (t1+1s). If there is no clock skew, it means that we're guaranteed to have
// exclusive access to the resource for 1s. If the maximum possible clock skew
// is, say, 0.4s, then we're guaranteed to have exclusive access to the resource
// for (1s - 0.4s = 0.6s).
// In other words, if `l = s.Lease(targetID)` returns success, then:
//   time.Now().Before(l.ValidityEnd) == true
//   time.Now().Before(l.Expiration.Sub(time.Second)) == true
// Of course, these conditions can't actually be tested because execution might
// be arbitrarily slow and the clock might have advanced arbitrarily by the time
// the condition is evaluated. In particular, a returned lease can be
// arbitrarily close to its ValidityEnd.
//
// In some cases, a loose guarantee that "right now" I have exclusive access to
// a resource is good enough. Other times, when operations on the resource have
// to be guaranteed to not overlap in time, the client needs to use techniques
// like deadlines for the respective operations. The result of Session.Lease() aims
// to help more involved uses cases by exposing extra information:
//
// // LeaseResult represents a lease - i.e the successful result of
// // Session.Lease(). If none of the fields are of interest, the caller can ignore
// // the LeaseResult completely.
// type LeaseResult struct {
//   // ValidityEnd specifies when the current lease should not be used anymore.
//   // This is equal to Expiration-1s. The 1s accounts for clock skew between
//   // nodes, and also aims to leave some gap between when a leaseholder does
//   // something assuming it has the lease and when another Session takes a
//   // conflicting lease.
//   ValidityEnd time.Time
//   // Expiration represents the moment when another Session can take a
//   // conflicting lease, assuming this leaseholder's session record is not
//   // heartbeated anymore.
//   Expiration time.Time
//   // Epoch represents the session record's epoch that this lease is tied to. It
//   // can be compared to the epoch of a prior lease to determine whether it's
//   // certain that no other node held a lease in the meantime.
//   //
//   // The Epoch can only be compared across leases obtained from a single Session
//   // instance (i.e. it cannot be compared across nodes, or even across process
//   // restarts on a single node).
//   Epoch int
// }
//
// === Database state ===
//
// Sessions and leases live in two database tables: `sessions` and
// `monitoring_leases`. Each Session object has a "session record" row, and each
// lease has a "lease record". The database is used as the central source of
// truth, with each node having in-memory state that's sufficiently synchronized
// with the database to provide non-overlapping leases. Stealing a lease from a
// session to another requires communicating between the respective two nodes
// through the database, as described in the "Lease stealing" section.
//
// === Lease stealing ===
//
// In order for session A to steal a lease previously owned by session B,
// session A needs to delete session B's record. This deletion can only happen at a
// time when session B's record is not valid (i.e. it is "expired"). The point of the
// deletion is to ensure that the in-memory representation of leases held by a
// session that expired and then heartbeated again do not appear to be valid if
// another session might have stolen them. Consider the following scenario:
//
// 1. l1, err := s1.Lease(ctx, 42 /* targetID */)
// 2. s1 becomes unable to heartbeat, for whatever reason
// 3.	l1.Expiration passes
// 4. l2, err := s2.lease(ctx, 42 /* targetID */)
// 5. s1 is able to heartbeat again
// 6. s1.Lease() returns ???
//
// We want s1.Lease to return ErrLeasedByAnother in step 6 (since there's
// another conflicting lease - l2). The deletion of s1 that happens in step 4
// causes the heartbeat in step 5 to notice that the session record is missing,
// and to recreate it with an incremented epoch.
//
// Note that, if there was no step 4 and nobody stole any of s1's leases while
// s1 is invalid, then, once s1 becomes valid again, all of its leases are
// again valid. That's a good thing: if a session expired briefly but nobody
// takes a conflicting lease, then we can allow all of the session's leases to
// be used again (in fact, it's possible that nobody ever realized that the
// session was temporarily expired). However, if another session steals *any*
// leases, then the expired session will be forced to renew *all* its leases
// when it becomes valid again.
//
// === Clock sync and lease stasis ===
//
// In order to provide leases that don't overlap in their validity window, this
// library assumes that the system clocks on the nodes creating sessions are
// synchronized within some bounds. If this assumption doesn't hold, it is
// possible for two nodes to have Valid() leases on the same target, at the same
// time.
//
// Note that this library allows the user to check whether a lease they have is
// currently valid, but it doesn't help the user bind any particular operation
// with a given lease: there's no direct way to say, for example, "commit this
// transaction only while this lease is valid". Users must do this kind of
// binding on top of the library, for example through the use of timeouts. For
// the purposes of the Obs Service, such stringent requirements are not
// necessary. Leases are used to make sure that multiple workers don't attempt
// to pull data from the same target at the same time; if one node pulls data
// from a target after its leases expires, that's OK; as soon as another node
// connects to that target, the original node will be disconnected.
