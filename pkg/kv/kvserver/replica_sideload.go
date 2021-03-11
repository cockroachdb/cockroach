// Copyright 2017 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package kvserver

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvserverpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/raftentry"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/errors"
	"go.etcd.io/etcd/raft/v3/raftpb"
)

var errSideloadedFileNotFound = errors.New("sideloaded file not found")

// SideloadStorage is the interface used for Raft SSTable sideloading.
// Implementations do not need to be thread safe.
type SideloadStorage interface {
	// The directory in which the sideloaded files are stored. May or may not
	// exist.
	Dir() string
	// Writes the given contents to the file specified by the given index and
	// term. Overwrites the file if it already exists.
	Put(_ context.Context, index, term uint64, contents []byte) error
	// Load the file at the given index and term. Return errSideloadedFileNotFound when no
	// such file is present.
	Get(_ context.Context, index, term uint64) ([]byte, error)
	// Purge removes the file at the given index and term. It may also
	// remove any leftover files at the same index and earlier terms, but
	// is not required to do so. When no file at the given index and term
	// exists, returns errSideloadedFileNotFound.
	//
	// Returns the total size of the purged payloads.
	Purge(_ context.Context, index, term uint64) (int64, error)
	// Clear files that may have been written by this SideloadStorage.
	Clear(context.Context) error
	// TruncateTo removes all files belonging to an index strictly smaller than
	// the given one. Returns the number of bytes freed, the number of bytes in
	// files that remain, or an error.
	TruncateTo(_ context.Context, index uint64) (freed, retained int64, _ error)
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
	return maybeSideloadEntriesImpl(ctx, entriesToAppend, r.raftMu.sideloaded)
}

// maybeSideloadEntriesImpl iterates through the provided slice of entries. If
// no sideloadable entries are found, it returns the same slice. Otherwise, it
// returns a new slice in which all applicable entries have been sideloaded to
// the specified SideloadStorage.
func maybeSideloadEntriesImpl(
	ctx context.Context, entriesToAppend []raftpb.Entry, sideloaded SideloadStorage,
) (_ []raftpb.Entry, sideloadedEntriesSize int64, _ error) {

	cow := false
	for i := range entriesToAppend {
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

			// Unmarshal the command into an object that we can mutate.
			var strippedCmd kvserverpb.RaftCommand
			if err := protoutil.Unmarshal(data, &strippedCmd); err != nil {
				return nil, 0, err
			}

			if strippedCmd.ReplicatedEvalResult.AddSSTable == nil {
				// Still no AddSSTable; someone must've proposed a v2 command
				// but not because it contains an inlined SSTable. Strange, but
				// let's be future proof.
				log.Warning(ctx, "encountered sideloaded Raft command without inlined payload")
				continue
			}

			// Actually strip the command.
			dataToSideload := strippedCmd.ReplicatedEvalResult.AddSSTable.Data
			strippedCmd.ReplicatedEvalResult.AddSSTable.Data = nil

			// Marshal the command and attach to the Raft entry.
			{
				data := make([]byte, raftCommandPrefixLen+strippedCmd.Size())
				encodeRaftCommandPrefix(data[:raftCommandPrefixLen], raftVersionSideloaded, cmdID)
				_, err := protoutil.MarshalTo(&strippedCmd, data[raftCommandPrefixLen:])
				if err != nil {
					return nil, 0, errors.Wrap(err, "while marshaling stripped sideloaded command")
				}
				ent.Data = data
			}

			log.Eventf(ctx, "writing payload at index=%d term=%d", ent.Index, ent.Term)
			if err := sideloaded.Put(ctx, ent.Index, ent.Term, dataToSideload); err != nil {
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
// or SideloadStorage to inline the payload, returning a new entry (which must
// be treated as immutable by the caller) or nil (if inlining does not apply)
//
// If a payload is missing, returns an error whose Cause() is
// errSideloadedFileNotFound.
func maybeInlineSideloadedRaftCommand(
	ctx context.Context,
	rangeID roachpb.RangeID,
	ent raftpb.Entry,
	sideloaded SideloadStorage,
	entryCache *raftentry.Cache,
) (*raftpb.Entry, error) {
	if !sniffSideloadedRaftCommand(ent.Data) {
		return nil, nil
	}
	log.Event(ctx, "inlining sideloaded SSTable")
	// We could unmarshal this yet again, but if it's committed we
	// are very likely to have appended it recently, in which case
	// we can save work.
	cachedSingleton, _, _, _ := entryCache.Scan(
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

	var command kvserverpb.RaftCommand
	if err := protoutil.Unmarshal(data, &command); err != nil {
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
		data := make([]byte, raftCommandPrefixLen+command.Size())
		encodeRaftCommandPrefix(data[:raftCommandPrefixLen], raftVersionSideloaded, cmdID)
		_, err := protoutil.MarshalTo(&command, data[raftCommandPrefixLen:])
		if err != nil {
			return nil, err
		}
		ent.Data = data
	}
	return &ent, nil
}

// assertSideloadedRaftCommandInlined asserts that if the provided entry is a
// sideloaded entry, then its payload has already been inlined. Doing so
// requires unmarshalling the raft command, so this assertion should be kept out
// of performance critical paths.
func assertSideloadedRaftCommandInlined(ctx context.Context, ent *raftpb.Entry) {
	if !sniffSideloadedRaftCommand(ent.Data) {
		return
	}

	var command kvserverpb.RaftCommand
	_, data := DecodeRaftCommand(ent.Data)
	if err := protoutil.Unmarshal(data, &command); err != nil {
		log.Fatalf(ctx, "%v", err)
	}

	if len(command.ReplicatedEvalResult.AddSSTable.Data) == 0 {
		// The entry is "thin", which is what this assertion is checking for.
		log.Fatalf(ctx, "found thin sideloaded raft command: %+v", command)
	}
}

// maybePurgeSideloaded removes [firstIndex, ..., lastIndex] at the given term
// and returns the total number of bytes removed. Nonexistent entries are
// silently skipped over.
func maybePurgeSideloaded(
	ctx context.Context, ss SideloadStorage, firstIndex, lastIndex uint64, term uint64,
) (int64, error) {
	var totalSize int64
	for i := firstIndex; i <= lastIndex; i++ {
		size, err := ss.Purge(ctx, i, term)
		if err != nil && !errors.Is(err, errSideloadedFileNotFound) {
			return totalSize, err
		}
		totalSize += size
	}
	return totalSize, nil
}
