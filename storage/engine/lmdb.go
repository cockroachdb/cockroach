package engine

import (
	"os"
	"runtime"

	"github.com/bmatsuo/lmdb-go/lmdb"
	"github.com/cockroachdb/cockroach/roachpb"
	"github.com/cockroachdb/cockroach/util/encoding"
	"github.com/cockroachdb/cockroach/util/log"
	"github.com/dustin/go-humanize"
	"github.com/gogo/protobuf/proto"
)

// LMDB wraps a Lightning Memory-Mapped Database.
// On linux, should build with the 'pwritev' build tag for optimal performance,
// see https://github.com/bmatsuo/lmdb-go.
type LMDB struct {
	size int64
	path string
	env  *lmdb.Env
}

// NewLMDB creates a new LMDB handle of the given size in the given directory.
func NewLMDB(size int64, path string) *LMDB {
	return &LMDB{
		size: size,
		path: path,
	}
}

var _ Engine = &LMDB{}

// Open implements Engine.
func (l *LMDB) Open() error {
	env, err := lmdb.NewEnv()
	if err != nil {
		return err
	}
	if err := env.SetMapSize(l.size); err != nil {
		return err
	}
	if err := os.MkdirAll(l.path, 0770); err != nil {
		return err
	}
	err = env.Open(l.path, 0, 0664)
	if err != nil {
		_ = env.Close()
	}
	log.Infof("opening lmdb instance at %q (%s)", l.path, humanize.Bytes(uint64(l.size)))
	l.env = env
	return err
}

// Close implements Engine.
func (l *LMDB) Close() {
	if err := l.env.Close(); err != nil {
		panic(err)
	}
	l.env = nil
}

// Closed implements Engine.
func (l *LMDB) Closed() bool {
	return l.env == nil
}

// Attrs implements Engine.
func (l *LMDB) Attrs() roachpb.Attributes {
	var attr roachpb.Attributes
	return attr
}

func lmdbEncode(key MVCCKey) []byte {
	hasTS := key.Timestamp != roachpb.ZeroTimestamp
	var enc []byte
	if hasTS {
		enc = make([]byte, 0, len(key.Key)+2)
	} else {
		enc = make([]byte, 0, len(key.Key)+1)
	}
	enc = append(enc, key.Key...)
	if hasTS {
		enc = append(enc, 0)
		enc = encoding.EncodeUint64Descending(enc, uint64(key.Timestamp.WallTime))
		enc = encoding.EncodeUint32Descending(enc, uint32(key.Timestamp.Logical))
	}
	return append(enc, byte(len(enc)-len(key.Key)))
}

func lmdbDecode(key, value []byte, dest *MVCCKeyValue) {
	dest.Value = value
	dest.Key.Timestamp = roachpb.ZeroTimestamp

	tsLen, key := int(key[len(key)-1]), key[:len(key)-1]
	keyPart, tsPart := key[:len(key)-tsLen], key[len(key)-tsLen:]
	dest.Key.Key = keyPart
	if len(tsPart) > 0 {
		tsPart = tsPart[1:] // remove the null byte

		var wt uint64
		var err error
		tsPart, wt, err = encoding.DecodeUint64Descending(tsPart)
		if err != nil {
			panic(err)
		}
		var lg uint32
		tsPart, lg, err = encoding.DecodeUint32Descending(tsPart)
		if err != nil {
			panic(err)
		}
		if len(tsPart) > 0 {
			panic(string(tsPart))
		}
		dest.Key.Timestamp.WallTime, dest.Key.Timestamp.Logical = int64(wt), int32(lg)
	}
}

func lmdbPut(txn *lmdb.Txn, db lmdb.DBI, key, value []byte) error {
	return txn.Put(db, key, value, 0)
}

// Put implements Engine.
func (l *LMDB) Put(key MVCCKey, value []byte) error {
	if len(key.Key) == 0 {
		return emptyKeyError()
	}
	return l.env.Update(func(txn *lmdb.Txn) error {
		db, err := txn.OpenRoot(0)
		if err != nil {
			return err
		}

		return lmdbPut(txn, db, lmdbEncode(key), value)
	})
}

func lmdbGet(txn *lmdb.Txn, db lmdb.DBI, key []byte) ([]byte, error) {
	v, err := txn.Get(db, key)
	if lmdb.IsNotFound(err) {
		return nil, nil
	}
	return v, err
}

// Get implements Engine.
func (l *LMDB) Get(key MVCCKey) (r []byte, err error) {
	if len(key.Key) == 0 {
		return nil, emptyKeyError()
	}

	_ = l.env.View(func(txn *lmdb.Txn) error {
		var db lmdb.DBI
		if db, err = txn.OpenRoot(0); err != nil {
			return err
		}
		r, err = lmdbGet(txn, db, lmdbEncode(key))
		return nil
	})
	return
}

func lmdbGetProto(key MVCCKey, data []byte, msg proto.Message) (ok bool, keyBytes, valBytes int64, err error) {
	if data == nil {
		msg.Reset()
		return
	}
	ok = true
	if msg != nil {
		// Make a byte slice that is backed by result.data. This slice
		// cannot live past the lifetime of this method, but we're only
		// using it to unmarshal the roachpb.
		err = proto.Unmarshal(data, msg)
	}
	keyBytes = int64(key.EncodedSize())
	valBytes = int64(len(data))
	return
}

// GetProto implements Engine.
func (l *LMDB) GetProto(key MVCCKey, msg proto.Message) (ok bool, keyBytes int64, valBytes int64, err error) {
	if len(key.Key) == 0 {
		err = emptyKeyError()
		return
	}
	var data []byte
	if data, err = l.Get(key); err != nil {
		return
	}
	return lmdbGetProto(key, data, msg)
}

// Iterate implements Engine.
func (l *LMDB) Iterate(start, end MVCCKey, f func(MVCCKeyValue) (bool, error)) error {
	iter := l.NewIterator(nil)
	defer iter.Close()
	return iterFromTo(iter, start, end, f)
}

func lmdbClear(txn *lmdb.Txn, db lmdb.DBI, key roachpb.Key) error {
	err := txn.Del(db, key, nil)
	if lmdb.IsNotFound(err) {
		return nil
	}
	return err
}

// Clear implements Engine.
func (l *LMDB) Clear(key MVCCKey) error {
	if len(key.Key) == 0 {
		return emptyKeyError()
	}
	return l.env.Update(func(txn *lmdb.Txn) error {
		db, err := txn.OpenRoot(0)
		if err != nil {
			return err
		}
		return lmdbClear(txn, db, lmdbEncode(key))
	})
}

// Merge implements Engine.
func (l *LMDB) Merge(key MVCCKey, value []byte) error {
	return nil // unimplemented
}

// Capacity implements Engine.
func (l *LMDB) Capacity() (roachpb.StoreCapacity, error) {
	cap := roachpb.StoreCapacity{}
	return cap, nil
}

// ApproximateSize implements Engine.
func (l *LMDB) ApproximateSize(start, end MVCCKey) (uint64, error) {
	return 0, nil
}

// Flush implements Engine.
func (l *LMDB) Flush() error {
	return nil
}

// NewIterator implements Engine.
func (l *LMDB) NewIterator(prefix roachpb.Key) Iterator {
	txn := readOnlyTxn(l.env)
	db, err := txn.OpenRoot(0)
	if err != nil {
		panic(err)
	}
	cursor, err := txn.OpenCursor(db)
	if err != nil {
		panic(err)
	}
	return &lmdbIterator{
		onClose: func() {
			cursor.Close()
			txn.Abort()
		},
		cursor: cursor,
	}
}

func readOnlyTxn(env *lmdb.Env) *lmdb.Txn {
	txn, err := env.BeginTxn(nil, lmdb.Readonly)
	if err != nil {
		panic(err)
	}
	return txn
}

// NewSnapshot implements Engine.
func (l *LMDB) NewSnapshot() Engine {
	txn := readOnlyTxn(l.env)
	db, err := txn.OpenRoot(0)
	if err != nil {
		panic(err)
	}
	return &lmdbBatch{
		readonly: true,
		l:        l,
		onClose: func() {
			txn.Abort()
		},
		txn: txn,
		db:  db,
	}
}

// NewBatch implements Engine.
func (l *LMDB) NewBatch() Engine {
	runtime.LockOSThread() // unlocked on Close or Commit
	txn, err := l.env.BeginTxn(nil, 0)
	if err != nil {
		panic(err)
	}
	db, err := txn.OpenRoot(0)
	if err != nil {
		panic(err)
	}
	return &lmdbBatch{
		l:       l,
		onClose: runtime.UnlockOSThread,
		txn:     txn,
		db:      db,
	}
}

// Commit implements Engine.
func (l *LMDB) Commit() error {
	return nil
}

// Defer implements Engine.
func (l *LMDB) Defer(fn func()) {
	panic("only implemented for batches")
}

// GetStats implements Engine.
func (l *LMDB) GetStats() (*Stats, error) {
	return &Stats{}, nil // TODO
}
