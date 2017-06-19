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
	"fmt"

	"golang.org/x/net/context"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage/storagebase"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/coreos/etcd/raft/raftpb"
	"github.com/pkg/errors"
)

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
	// Clear removes all on-disk files.
	Clear(context.Context) error
	// TruncateTo removes all files belonging to an index strictly smaller than
	// the given one.
	TruncateTo(_ context.Context, index uint64) error
}

type slKey struct {
	index, term uint64
}

type inMemSideloadStorage struct {
	m      map[slKey][]byte
	prefix string
}

func newInMemSideloadStorage(rangeID roachpb.RangeID, replicaID roachpb.ReplicaID) sideloadStorage {
	return &inMemSideloadStorage{
		prefix: fmt.Sprintf("%d.%d/", rangeID, replicaID),
		m:      make(map[slKey][]byte),
	}
}

func (imss *inMemSideloadStorage) String() string {
	return imss.prefix
}

func (imss *inMemSideloadStorage) key(index, term uint64) slKey {
	return slKey{index: index, term: term}
}

func (imss *inMemSideloadStorage) PutIfNotExists(
	_ context.Context, index, term uint64, contents []byte,
) error {
	key := imss.key(index, term)
	if _, ok := imss.m[key]; ok {
		return nil
	}
	imss.m[key] = contents
	return nil
}

func (imss *inMemSideloadStorage) Get(_ context.Context, index, term uint64) ([]byte, error) {
	key := imss.key(index, term)
	data, ok := imss.m[key]
	if !ok {
		return nil, errSideloadedFileNotFound
	}
	return data, nil
}

func (imss *inMemSideloadStorage) Purge(_ context.Context, index, term uint64) error {
	k := imss.key(index, term)
	if _, ok := imss.m[k]; !ok {
		return errSideloadedFileNotFound
	}
	delete(imss.m, k)
	return nil
}

func (imss *inMemSideloadStorage) Clear(_ context.Context) error {
	imss.m = make(map[slKey][]byte)
	return nil
}

func (imss *inMemSideloadStorage) TruncateTo(_ context.Context, index uint64) error {
	// Not efficient, but this storage is for testing purposes only anyway.
	for k := range imss.m {
		if k.index < index {
			delete(imss.m, k)
		}
	}
	return nil
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
) ([]raftpb.Entry, error) {
	// TODO(tschottdorf): allocating this closure could be expensive. If so make
	// it a method on Replica.
	maybeRaftCommand := func(cmdID storagebase.CmdIDKey) storagebase.RaftCommand {
		r.mu.Lock()
		defer r.mu.Unlock()
		cmd, ok := r.mu.proposals[cmdID]
		if !ok {
			return storagebase.RaftCommand{}
		}
		// Happy case: we have this proposal locally (i.e. we proposed
		// it). In this case, we can save unmarshalling the fat proposal
		// because it's already in-memory.
		if cmd.command.ReplicatedEvalResult.AddSSTable == nil {
			panic("encountered sideloaded non-AddSSTable command")
		}
		log.Eventf(ctx, "sideloaded proposal was proposed locally")
		// The raft proposal is technically protected by the lock, but
		// the contained command is immutable. Since we respect that, we
		// shallow-copy the (nullable) AddSSTable struct which we intend
		// to modify.
		rCmd := cmd.command
		addSSTableCopy := *cmd.command.ReplicatedEvalResult.AddSSTable
		rCmd.ReplicatedEvalResult.AddSSTable = &addSSTableCopy
		return rCmd
	}
	return maybeSideloadEntriesImpl(ctx, entriesToAppend, r.raftMu.sideloaded, maybeRaftCommand)
}

func sniffSideloadedRaftCommand(data []byte) (sideloaded bool) {
	return len(data) > 0 && data[0] == byte(raftCommandEncodingVersionV2)
}

func maybeInlineSideloadedRaftCommand(
	ctx context.Context,
	rangeID roachpb.RangeID,
	ent *raftpb.Entry,
	sideloaded sideloadStorage,
	entryCache *raftEntryCache,
) error {
	if !sniffSideloadedRaftCommand(ent.Data) {
		return nil
	}
	log.Event(ctx, "inlining sideloaded SSTable")
	// We could unmarshal this yet again, but if it's committed we
	// are very likely to have appended it recently, in which case
	// we can save work.
	cachedSingleton, _, _ := entryCache.getEntries(
		nil, rangeID, ent.Index, ent.Index+1, 1<<20,
	)

	if len(cachedSingleton) > 0 {
		*ent = cachedSingleton[0]
		log.Event(ctx, "using cache hit")
		return nil
	}

	log.Event(ctx, "inlined entry not cached")
	// Out of luck, for whatever reason the inlined proposal isn't in the cache.
	cmdID, data := DecodeRaftCommand(ent.Data)

	var command storagebase.RaftCommand
	if err := command.Unmarshal(data); err != nil {
		return err
	}
	sideloadedData, err := sideloaded.Get(ctx, ent.Index, ent.Term)
	if err != nil {
		return errors.Wrap(err, "loading sideloaded data")
	}
	command.ReplicatedEvalResult.AddSSTable.Data = sideloadedData
	{
		data, err := command.Marshal()
		if err != nil {
			return err
		}
		ent.Data = encodeRaftCommandV2(cmdID, data)
	}
	return nil
}

// maybeSideloadEntriesImpl iterates through the provided slice of entries. If
// no sideloadable entries are found, it returns the same slice. Otherwise, it
// returns a new slice in which all applicable entries have been sideloaded to
// the specified sideloadStorage. Whenever maybeRaftCommand returns a Raft
// command, it is assumed that its AddSSTable field may be mutated.
func maybeSideloadEntriesImpl(
	ctx context.Context,
	entriesToAppend []raftpb.Entry,
	sideloaded sideloadStorage,
	maybeRaftCommand func(storagebase.CmdIDKey) storagebase.RaftCommand,
) ([]raftpb.Entry, error) {

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
			strippedCmd := maybeRaftCommand(cmdID)

			if strippedCmd.ReplicatedEvalResult.AddSSTable == nil {
				// Bad luck: we didn't have the proposal in-memory, so we'll
				// have to unmarshal it.
				log.Event(ctx, "proposal not already in memory; unmarshaling")
				if err := strippedCmd.Unmarshal(data); err != nil {
					return nil, err
				}
			} else {
				log.Event(ctx, "(copy of) command already in memory; using it")
			}

			if strippedCmd.ReplicatedEvalResult.AddSSTable == nil {
				// Still no AddSSTable; someone must've proposed a v2 command
				// but not becaused it contains an inlined SSTable. Strange, but
				// let's be future proof.
				log.Warning(ctx, "encountered v2 Raft command without inlined payload")
				continue
			}

			// Actually strip the command.
			dataToSideload := strippedCmd.ReplicatedEvalResult.AddSSTable.Data
			strippedCmd.ReplicatedEvalResult.AddSSTable.Data = nil

			{
				var err error
				data, err = strippedCmd.Marshal()
				if err != nil {
					return nil, errors.Wrap(err, "while marshalling stripped sideloaded command")
				}
			}

			ent.Data = encodeRaftCommandV2(cmdID, data)
			log.Eventf(ctx, "writing payload at index=%d term=%d", ent.Index, ent.Term)
			if err = sideloaded.PutIfNotExists(ctx, ent.Index, ent.Term, dataToSideload); err != nil {
				return nil, err
			}
		}
	}
	return entriesToAppend, nil
}
