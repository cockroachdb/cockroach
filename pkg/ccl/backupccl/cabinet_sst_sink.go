// Copyright 2022 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package backupccl

import (
	"context"
	"fmt"
	"path"
	"sort"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/cloud"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/builtins"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/mon"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/errors"
)

type cabinetSSTSinkConf struct {
	enc      *jobspb.BackupEncryptionOptions
	id       base.SQLInstanceID
	settings *settings.Values
}

type cabinetSSTSink struct {
	dest cloud.ExternalStorage
	conf cabinetSSTSinkConf

	queue queue

	ctx context.Context

	memAcc struct {
		ba            *mon.BoundAccount
		reservedBytes int64
	}
}

type queue struct {
	// The information in keys[i] will be contained in values[i].
	// But we want to marshall values early to save memory, so store keys
	// separately to sort the list without unmarshalling.
	keys   []*roachpb.Key
	values []*[]byte

	// The current byte size of the queue.
	size int

	sort.Interface
}

func (q queue) Len() int {
	return len(q.keys)
}

func (q queue) Swap(i int, j int) {
	q.keys[i], q.keys[j] = q.keys[j], q.keys[i]
	q.values[i], q.values[j] = q.values[j], q.values[i]
}

func (q queue) Less(i, j int) bool {
	return q.keys[i].Compare(*q.keys[j]) < 0
}

func (q *queue) add(key *roachpb.Key, value *[]byte) {
	q.keys = append(q.keys, key)
	q.values = append(q.values, value)
	q.size += len(*key) + len(*value)
}

func (q *queue) reset() {
	q.keys = make([]*roachpb.Key, 0)
	q.values = make([]*[]byte, 0)
	q.size = 0
}

func makeCabinetSSTSink(
	ctx context.Context,
	conf cabinetSSTSinkConf,
	dest cloud.ExternalStorage,
	backupMem *mon.BoundAccount,
) (*cabinetSSTSink, error) {
	s := &cabinetSSTSink{conf: conf, dest: dest, ctx: ctx}
	s.memAcc.ba = backupMem

	// Reserve memory for the file buffer. Incrementally reserve memory in chunks
	// upto a maximum of the `smallFileBuffer` cluster setting value. If we fail
	// to grow the bound account at any stage, use the buffer size we arrived at
	// prior to the error.
	incrementSize := int64(32 << 20)
	maxSize := smallFileBuffer.Get(s.conf.settings)
	for {
		if s.memAcc.reservedBytes >= maxSize {
			break
		}

		if incrementSize > maxSize-s.memAcc.reservedBytes {
			incrementSize = maxSize - s.memAcc.reservedBytes
		}

		if err := s.memAcc.ba.Grow(ctx, incrementSize); err != nil {
			log.Infof(ctx,
				"failed to grow file queue by %d bytes, running backup with queue size %d bytes: %+v",
				incrementSize, s.memAcc.reservedBytes, err)
			break
		}
		s.memAcc.reservedBytes += incrementSize
	}
	if s.memAcc.reservedBytes == 0 {
		return nil, errors.New("failed to reserve memory for cabinetSSTSink queue")
	}

	return s, nil
}

func (s *cabinetSSTSink) close() {
	if s.queue.size > 0 {
		s.queue.reset()
		log.Errorf(s.ctx, "Cabinet queue non-empty on close.")
	}
	// Release the memory reserved for the file buffer.
	s.memAcc.ba.Shrink(s.ctx, s.memAcc.reservedBytes)
	s.memAcc.reservedBytes = 0
}

func (s *cabinetSSTSink) push(file BackupManifest_File) (*int64, error) {
	encodedFile, err := protoutil.Marshal(&file)
	if err != nil {
		return nil, err
	}
	key := encodeFileSSTKey(file.Span.Key, file.Path)
	s.queue.add(&key, &encodedFile)

	if s.queue.size < int(s.memAcc.reservedBytes) {
		return nil, nil
	}

	cabinetID, err := s.writeQueue()
	if err != nil {
		return nil, err
	}

	return &cabinetID, nil
}

func (s *cabinetSSTSink) writeQueue() (int64, error) {
	sort.Sort(s.queue)

	cabinetID := int64(builtins.GenerateUniqueInt(s.conf.id))

	cabinetPath := cabinetPathFromID(cabinetID)

	w, err := makeWriter(s.ctx, s.dest, cabinetPath, s.conf.enc)
	if err != nil {
		return 0, err
	}
	defer w.Close()
	fileSST := storage.MakeBackupSSTWriter(s.ctx, s.dest.Settings(), w)
	defer fileSST.Close()

	for i, key := range s.queue.keys {
		value := s.queue.values[i]
		if err := fileSST.PutUnversioned(*key, *value); err != nil {
			return 0, err
		}
	}
	err = fileSST.Finish()
	if err != nil {
		return 0, err
	}
	err = w.Close()
	if err != nil {
		return 0, err
	}

	s.queue.reset()

	return cabinetID, nil
}

func cabinetPathFromID(ID int64) string {
	return path.Join(cabinetDirectory, fmt.Sprintf("%d.sst", ID))
}

func encodeFileSSTKey(spanStart roachpb.Key, filename string) roachpb.Key {
	buf := make([]byte, 0)
	buf = encoding.EncodeBytesAscending(buf, spanStart)
	return roachpb.Key(encoding.EncodeStringAscending(buf, filename))
}
