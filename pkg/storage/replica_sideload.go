// Copyright 2017 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package storage

import (
	"runtime/debug"
	"time"

	"golang.org/x/net/context"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/storage/storagebase"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/coreos/etcd/raft/raftpb"
	"github.com/gogo/protobuf/proto"
	"github.com/pkg/errors"
)

const bulkIOWriteLimiterLongWait = 500 * time.Millisecond

func limitBulkIOWrite(ctx context.Context, st *cluster.Settings, cost int) {
	// The limiter disallows anything greater than its burst (set to
	// BulkIOWriteLimiterBurst), so cap the batch size if it would overflow.
	//
	// TODO(dan): This obviously means the limiter is no longer accounting for
	// the full cost. I've tried calling WaitN in a loop to fully cover the
	// cost, but that doesn't seem to be as smooth in practice (TPCH-10 restores
	// on azure local disks), I think because the file is written all at once at
	// the end. This could be fixed by writing the file in chunks, which also
	// would likely help the overall smoothness, too.
	if cost > cluster.BulkIOWriteLimiterBurst {
		cost = cluster.BulkIOWriteLimiterBurst
	}

	begin := timeutil.Now()
	if err := st.BulkIOWriteLimiter.WaitN(ctx, cost); err != nil {
		log.Errorf(ctx, "error rate limiting bulk io write: %+v", err)
	}

	if d := timeutil.Since(begin); d > bulkIOWriteLimiterLongWait {
		log.Warningf(ctx, "bulk io write limiter took %s (>%s):\n%s",
			d, bulkIOWriteLimiterLongWait, debug.Stack())
	}
}

var errSideloadedFileNotFound = errors.New("sideloaded file not found")

// sideloadStorage is the interface used for Raft SSTable sideloading.
// Implementations do not need to be thread safe.
type sideloadStorage interface {
	// Writes the given contents to the file specified by the given index and
	// term. Does not perform the write if the file exists.
	PutIfNotExists(_ context.Context, index, term uint64, contents []byte) error
	// Load the file at the given index and term. Return errSideloadedFileNotFound when no
	// such file is present.
	Get(_ context.Context, index, term uint64) ([]byte, error)
	// Purge removes the file at the given index and term. It may also
	// remove any leftover files at the same index and earlier terms, but
	// is not required to do so. When no file at the given index and term
	// exists, returns errSideloadedFileNotFound.
	Purge(_ context.Context, index, term uint64) error
	// Clear files that may have been written by this sideloadStorage.
	Clear(context.Context) error
	// TruncateTo removes all files belonging to an index strictly smaller than
	// the given one.
	TruncateTo(_ context.Context, index uint64) error
	// Returns an absolute path to the file that Get() would return the contents
	// of. Does not check whether the file actually exists.
	Filename(_ context.Context, index, term uint64) (string, error)
}

// maybeSideloadEntriesRaftMuLocked should be called with a slice of "fat"
// entries before appending them to the Raft log. For those entries which are
// sideloadable, this is where the actual sideloading happens: in come fat
// proposals, out go thin proposals. Note that this method is to be called
// before modifications are persisted to the log. The other way around is
// incorrect since an ill-timed crash gives you thin proposals and no files.
//
// The passed-in slice is not mutated.
func (r *Replica) maybeSideloadEntriesRaftMuLocked(
	ctx context.Context, entriesToAppend []raftpb.Entry,
) (_ []raftpb.Entry, sideloadedEntriesSize int64, _ error) {
	// TODO(tschottdorf): allocating this closure could be expensive. If so make
	// it a method on Replica.
	maybeRaftCommand := func(cmdID storagebase.CmdIDKey) (storagebase.RaftCommand, bool) {
		r.mu.Lock()
		defer r.mu.Unlock()
		cmd, ok := r.mu.proposals[cmdID]
		if ok {
			return cmd.command, true
		}
		return storagebase.RaftCommand{}, false
	}
	return maybeSideloadEntriesImpl(ctx, entriesToAppend, r.raftMu.sideloaded, maybeRaftCommand)
}

// maybeSideloadEntriesImpl iterates through the provided slice of entries. If
// no sideloadable entries are found, it returns the same slice. Otherwise, it
// returns a new slice in which all applicable entries have been sideloaded to
// the specified sideloadStorage. maybeRaftCommand is called when sideloading is
// necessary and can optionally supply a pre-Unmarshaled RaftCommand (which
// usually is provided by the Replica in-flight proposal map.
func maybeSideloadEntriesImpl(
	ctx context.Context,
	entriesToAppend []raftpb.Entry,
	sideloaded sideloadStorage,
	maybeRaftCommand func(storagebase.CmdIDKey) (storagebase.RaftCommand, bool),
) (_ []raftpb.Entry, sideloadedEntriesSize int64, _ error) {

	cow := false
	for i := range entriesToAppend {
		var err error
		if sniffSideloadedRaftCommand(entriesToAppend[i].Data) {
			log.Event(ctx, "sideloading command in append")
			if !cow {
				// Avoid mutating the passed-in entries directly. The caller
				// wants them to remain "fat".
				log.Eventf(ctx, "copying entries slice of length %d", len(entriesToAppend))
				cow = true
				entriesToAppend = append([]raftpb.Entry(nil), entriesToAppend...)
			}

			ent := &entriesToAppend[i]
			cmdID, data := DecodeRaftCommand(ent.Data) // cheap
			strippedCmd, ok := maybeRaftCommand(cmdID)
			if ok {
				// Happy case: we have this proposal locally (i.e. we proposed
				// it). In this case, we can save unmarshalling the fat proposal
				// because it's already in-memory.
				if strippedCmd.ReplicatedEvalResult.AddSSTable == nil {
					log.Fatalf(ctx, "encountered sideloaded non-AddSSTable command: %+v", strippedCmd)
				}
				log.Eventf(ctx, "command already in memory")
				// The raft proposal is immutable. To respect that, shallow-copy
				// the (nullable) AddSSTable struct which we intend to modify.
				addSSTableCopy := *strippedCmd.ReplicatedEvalResult.AddSSTable
				strippedCmd.ReplicatedEvalResult.AddSSTable = &addSSTableCopy
			} else {
				// Bad luck: we didn't have the proposal in-memory, so we'll
				// have to unmarshal it.
				log.Event(ctx, "proposal not already in memory; unmarshaling")
				if err := proto.Unmarshal(data, &strippedCmd); err != nil {
					return nil, 0, err
				}
			}

			if strippedCmd.ReplicatedEvalResult.AddSSTable == nil {
				// Still no AddSSTable; someone must've proposed a v2 command
				// but not becaused it contains an inlined SSTable. Strange, but
				// let's be future proof.
				log.Warning(ctx, "encountered sideloaded Raft command without inlined payload")
				continue
			}

			// Actually strip the command.
			dataToSideload := strippedCmd.ReplicatedEvalResult.AddSSTable.Data
			strippedCmd.ReplicatedEvalResult.AddSSTable.Data = nil

			{
				var err error
				data, err = strippedCmd.Marshal()
				if err != nil {
					return nil, 0, errors.Wrap(err, "while marshalling stripped sideloaded command")
				}
			}

			ent.Data = encodeRaftCommandV2(cmdID, data)
			log.Eventf(ctx, "writing payload at index=%d term=%d", ent.Index, ent.Term)
			if err = sideloaded.PutIfNotExists(ctx, ent.Index, ent.Term, dataToSideload); err != nil {
				return nil, 0, err
			}
			sideloadedEntriesSize += int64(len(dataToSideload))
		}
	}
	return entriesToAppend, sideloadedEntriesSize, nil
}

func sniffSideloadedRaftCommand(data []byte) (sideloaded bool) {
	return len(data) > 0 && data[0] == byte(raftVersionSideloaded)
}

// maybeInlineSideloadedRaftCommand takes an entry and inspects it. If its
// command encoding version indicates a sideloaded entry, it uses the entryCache
// or sideloadStorage to inline the payload, returning a new entry (which must
// be treated as immutable by the caller) or nil (if inlining does not apply)
//
// If a payload is missing, returns an error whose Cause() is
// errSideloadedFileNotFound.
func maybeInlineSideloadedRaftCommand(
	ctx context.Context,
	rangeID roachpb.RangeID,
	ent raftpb.Entry,
	sideloaded sideloadStorage,
	entryCache *raftEntryCache,
) (*raftpb.Entry, error) {
	if !sniffSideloadedRaftCommand(ent.Data) {
		return nil, nil
	}
	log.Event(ctx, "inlining sideloaded SSTable")
	// We could unmarshal this yet again, but if it's committed we
	// are very likely to have appended it recently, in which case
	// we can save work.
	cachedSingleton, _, _ := entryCache.getEntries(
		nil, rangeID, ent.Index, ent.Index+1, 1<<20,
	)

	if len(cachedSingleton) > 0 {
		log.Event(ctx, "using cache hit")
		return &cachedSingleton[0], nil
	}

	// Make a shallow copy.
	entCpy := ent
	ent = entCpy

	log.Event(ctx, "inlined entry not cached")
	// Out of luck, for whatever reason the inlined proposal isn't in the cache.
	cmdID, data := DecodeRaftCommand(ent.Data)

	var command storagebase.RaftCommand
	if err := proto.Unmarshal(data, &command); err != nil {
		return nil, err
	}

	if len(command.ReplicatedEvalResult.AddSSTable.Data) > 0 {
		// The entry we started out with was already "fat". This happens when
		// the entry reached us through a preemptive snapshot (when we didn't
		// have a ReplicaID yet).
		log.Event(ctx, "entry already inlined")
		return &ent, nil
	}

	sideloadedData, err := sideloaded.Get(ctx, ent.Index, ent.Term)
	if err != nil {
		return nil, errors.Wrap(err, "loading sideloaded data")
	}
	command.ReplicatedEvalResult.AddSSTable.Data = sideloadedData
	{
		data, err := command.Marshal()
		if err != nil {
			return nil, err
		}
		ent.Data = encodeRaftCommandV2(cmdID, data)
	}
	return &ent, nil
}
