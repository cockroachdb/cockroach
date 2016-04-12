package engine

import (
	"github.com/bmatsuo/lmdb-go/lmdb"
	"github.com/cockroachdb/cockroach/roachpb"
	"github.com/gogo/protobuf/proto"
)

type lmdbIterator struct {
	onClose    func()
	cursor     *lmdb.Cursor
	scratchKey []byte
	curKV      MVCCKeyValue
	exhausted  bool
	err        error
}

var _ Iterator = &lmdbIterator{}

// The following methods implement the Iterator interface.
func (l *lmdbIterator) Close() {
	l.onClose()
}

func (l *lmdbIterator) Seek(key MVCCKey) {
	if len(key.Key) == 0 {
		l.setState(l.cursor.Get(key.Key, nil, lmdb.First))
	} else {
		if key.Equal(l.unsafeKey()) {
			return
		}
		enc := encode(key)
		l.setState(l.cursor.Get(enc, nil, lmdb.SetRange))
	}
}

func (l *lmdbIterator) Valid() bool {
	return l.err == nil && !l.exhausted
}

func (l *lmdbIterator) Next() {
	l.setState(l.cursor.Get(nil, nil, lmdb.Next))
}

func (l *lmdbIterator) SeekReverse(key MVCCKey) {
	if len(key.Key) == 0 {
		// Dubious convention taken from the RocksDB impl.
		l.setState(l.cursor.Get(key.Key, nil, lmdb.Last))
	} else {
		if key.Equal(l.unsafeKey()) {
			return
		}
		l.setState(l.cursor.Get(key.Key, nil, lmdb.SetRange))
		if !l.Valid() {
			l.setState(l.cursor.Get(key.Key, nil, lmdb.Last)) // seek to last
		}
		if !l.Valid() {
			return
		}
		if key.Less(l.Key()) {
			l.Prev()
		}
	}
}

func (l *lmdbIterator) setState(k, v []byte, err error) {
	if lmdb.IsNotFound(err) {
		l.exhausted = true
	} else {
		l.err = err
		if err == nil {
			decode(k, v, &l.curKV)
		}
	}
}

func (l *lmdbIterator) Prev() {
	l.setState(l.cursor.Get(l.scratchKey, l.curKV.Value, lmdb.Prev))
}

func (l *lmdbIterator) Key() MVCCKey {
	k := l.curKV.Key
	k.Key = append(roachpb.Key(nil), k.Key...)
	return k
}

func (l *lmdbIterator) Value() []byte {
	return append([]byte(nil), l.curKV.Value...)
}

func (l *lmdbIterator) ValueProto(msg proto.Message) error {
	if v := l.unsafeValue(); len(v) > 0 {
		return proto.Unmarshal(v, msg)
	}
	return nil
}

func (l *lmdbIterator) unsafeKey() MVCCKey {
	return l.curKV.Key
}

func (l *lmdbIterator) unsafeValue() []byte {
	return l.curKV.Value
}

func (l *lmdbIterator) Error() error {
	return l.err
}

func (l *lmdbIterator) ComputeStats(start, end MVCCKey, nowNanos int64) (MVCCStats, error) {
	panic("unimplemented")
}
