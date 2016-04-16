package engine

import (
	"errors"

	"github.com/bmatsuo/lmdb-go/lmdb"
	"github.com/cockroachdb/cockroach/roachpb"
	"github.com/cockroachdb/cockroach/util"
	"github.com/gogo/protobuf/proto"
)

type lmdbBatch struct {
	onClose  func()
	readonly bool
	l        *LMDB
	db       lmdb.DBI
	txn      *lmdb.Txn
}

var _ Engine = &lmdbBatch{}

func (l *lmdbBatch) Open() error {
	panic("unimplemented")
}

func (l *lmdbBatch) Close() {
	defer l.onClose()
	l.txn.Abort()
	l.txn = nil
}

func (l *lmdbBatch) Closed() bool {
	return l.txn == nil
}

func (l *lmdbBatch) Attrs() roachpb.Attributes {
	return l.l.Attrs()
}

func (l *lmdbBatch) Put(key MVCCKey, value []byte) error {
	return lmdbPut(l.txn, l.db, lmdbEncode(key), value)
}

func (l *lmdbBatch) Get(key MVCCKey) ([]byte, error) {
	return lmdbGet(l.txn, l.db, lmdbEncode(key))
}

func (l *lmdbBatch) GetProto(key MVCCKey, msg proto.Message) (ok bool, keyBytes, valBytes int64, err error) {
	var data []byte
	if data, err = l.txn.Get(l.db, lmdbEncode(key)); err != nil {
		return
	}
	return lmdbGetProto(key, data, msg)
}

func (l *lmdbBatch) Iterate(start, end MVCCKey, f func(MVCCKeyValue) (bool, error)) error {
	iter := l.NewIterator(nil)
	defer iter.Close()
	return iterFromTo(iter, start, end, f)
}

func (l *lmdbBatch) NewIterator(prefix roachpb.Key) Iterator {
	if prefix != nil {
		// Not using the prefix isn't incorrect, just means
		// we're going to not use information given to us by the user.
	}
	cursor, err := l.txn.OpenCursor(l.db)
	if err != nil {
		panic(err)
	}

	return &lmdbIterator{
		onClose: cursor.Close,
		cursor:  cursor,
	}
}

func (l *lmdbBatch) Clear(key MVCCKey) error {
	return lmdbClear(l.txn, l.db, lmdbEncode(key))
}

func (l *lmdbBatch) Merge(key MVCCKey, value []byte) error {
	if l.readonly {
		return errors.New("cannot merge to a snapshot")
	}
	return nil // unimplemented
}

func (l *lmdbBatch) Capacity() (roachpb.StoreCapacity, error) {
	return l.l.Capacity()
}

func (l *lmdbBatch) ApproximateSize(start, end MVCCKey) (uint64, error) {
	return l.l.ApproximateSize(start, end)
}

func (l *lmdbBatch) Flush() error {
	return nil
}

func (l *lmdbBatch) NewSnapshot() Engine {
	panic("already a snapshot or batch")
}

func (l *lmdbBatch) NewBatch() Engine {
	panic("already a snapshot or batch")
}

func (l *lmdbBatch) Commit() error {
	if l.readonly {
		return errors.New("cannot commit a snapshot")
	}

	defer l.onClose()
	return l.txn.Commit()
}

func (l *lmdbBatch) Defer(fn func()) {
	panic("only implemented for batches")
}

func (l *lmdbBatch) GetStats() (*Stats, error) {
	return nil, util.Errorf("GetStats is not implemented for %T", l)
}
