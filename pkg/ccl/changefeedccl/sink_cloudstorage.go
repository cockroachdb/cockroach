// Copyright 2019 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package changefeedccl

import (
	"bytes"
	"compress/gzip"
	"context"
	"fmt"
	"io"
	"net/url"
	"path/filepath"
	"strings"
	"sync/atomic"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl/changefeedbase"
	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/storage/cloud"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/humanizeutil"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/mon"
	"github.com/cockroachdb/errors"
	"github.com/google/btree"
)

func isCloudStorageSink(u *url.URL) bool {
	switch u.Scheme {
	case changefeedbase.SinkSchemeCloudStorageS3, changefeedbase.SinkSchemeCloudStorageGCS,
		changefeedbase.SinkSchemeCloudStorageNodelocal, changefeedbase.SinkSchemeCloudStorageHTTP,
		changefeedbase.SinkSchemeCloudStorageHTTPS, changefeedbase.SinkSchemeCloudStorageAzure:
		return true
	default:
		return false
	}
}

// cloudStorageFormatTime formats times as YYYYMMDDHHMMSSNNNNNNNNNLLLLLLLLLL.
func cloudStorageFormatTime(ts hlc.Timestamp) string {
	// TODO(dan): This is an absurdly long way to print out this timestamp, but
	// I kept hitting bugs while trying to do something clever to make it
	// shorter. Revisit.
	const f = `20060102150405`
	t := ts.GoTime()
	return fmt.Sprintf(`%s%09d%010d`, t.Format(f), t.Nanosecond(), ts.Logical)
}

type cloudStorageSinkFile struct {
	cloudStorageSinkKey
	codec   io.WriteCloser
	rawSize int
	buf     bytes.Buffer
}

var _ io.Writer = &cloudStorageSinkFile{}

func (f *cloudStorageSinkFile) Write(p []byte) (int, error) {
	f.rawSize += len(p)
	if f.codec != nil {
		return f.codec.Write(p)
	}
	return f.buf.Write(p)
}

// cloudStorageSink writes changefeed output to files in a cloud storage bucket
// (S3/GCS/HTTP) maintaining CDC's ordering guarantees (see below) for each
// row through lexicographical filename ordering.
//
// Changefeeds offer the following two ordering guarantees to external clients:
//
// 1. Rows are emitted with a timestamp. Individual rows are emitted in
// timestamp order. There may be duplicates, but once a row is seen at a given
// timestamp no previously unseen version of that row will be emitted at a less
// (or equal) timestamp. For example, you may see 1 2 1 2, or even 1 2 1, but
// never simply 2 1.
// 2. Periodically, a resolved timestamp is emitted. This is a changefeed-wide
// guarantee that no previously unseen row will later be seen with a timestamp
// less (or equal) to the resolved one. The cloud storage sink is structured as
// a number of distsql processors that each emit some part of the total changefeed.
// These processors only write files containing row data (initially only in ndjson
// format in this cloudStorageSink). This mapping is stable for a given distsql
// flow of a changefeed (meaning any given row is always emitted by the same
// processor), but it's not stable across restarts (pause/unpause). Each of these
// processors report partial progress information to a central coordinator
// (changeFrontier), which is responsible for writing the resolved timestamp files.
//
// In addition to the guarantees required of any changefeed, the cloud storage
// sink adds some quality of life guarantees of its own.
// 3. All rows in a file are from the same table. Further, all rows in a file are
// from the same schema version of that table, and so all have the same schema.
// 4. All files are partitioned into folders by the date part of the filename.
//
// Two methods of the cloudStorageSink on each data emitting processor are
// called. EmitRow is called with each row change and Flush is called before
// sending partial progress information to the coordinator. This happens with no
// concurrency, all EmitRow and Flush calls for a sink are serialized.
// EmitResolvedTimestamp is only called by the `changeFrontier`.
//
// The rows handed to EmitRow by the changefeed are guaranteed to satisfy
// condition (1). Further, as long as the sink has written every EmitRow it's
// gotten before returning from Flush, condition (2) is upheld.
//
// The cloudStorageSink uses lexicographic filename ordering to provide a total
// ordering for the output of this sink. Guarantees (1) and (2) depend on this
// ordering. Specifically, at any given time, the order of the data written by
// the sink is by lexicographic filename and then by order within the file.
//
// Batching these row updates into files is complicated because:
// a) We need to pick a representative timestamp for the file. This is required
// for comparison with resolved timestamp filenames as part of guarantee (2).
// b) For each topic, the row ordering guarantees must be preserved.
// One intuitive way of solving (b) is to ensure that filenames are emitted in
// strictly lexically increasing order (see assertion in `flushFile()`). This
// guarantees correctness as long as the underlying system is correct.
//
// Before the local progress is sent to the coordinator (called the "local frontier" as
// opposed to the resolved timestamp which is exactly a "global frontier" or
// "changefeed-level frontier"), all buffered data which preceded that update is flushed.
// To accomplish (a), we need two invariants. (a1) is that once Flush is called, we can
// never write a file with a timestamp that is less than or equal to the local frontier.
// This is because the local progress update could indeed cause a resolved timestamp file
// to be written with that timestamp. We cannot break this invariant because the client is
// free to ignore any files with a lexically lesser filename. Additionally, because we
// picked the resolved timestamp filename to sort after a data file with the same
// timestamp, a data file can't even be emitted at the same timestamp, it must be emitted
// at a timestamp that is strictly greater than the last globally resolved timestamp. Note
// that the local frontier is a guarantee that the sink will never get an EmitRow with
// that timestamp or lower. (a2) is that whenever Flush is called, all files written by
// the sink must be named using timestamps less than or equal to the one for the local
// frontier at the time Flush is called. This is again because our local progress update
// could cause the global progress to be updated and we need everything written so far to
// lexically compare as less than the new resolved timestamp.
//
// The data files written by this sink are named according to the pattern
// `<timestamp>-<uniquer>-<topic_id>-<schema_id>.<ext>`, each component of which is as
// follows:
//
// `<timestamp>` is the smallest resolved timestamp being tracked by this sink's
// `changeAggregator`, as of the time the last `Flush()` call was made (or `StatementTime`
// if `Flush()` hasn't been called yet). Intuitively, this can be thought of as an
// inclusive lower bound on the timestamps of updates that can be seen in a given file.
//
// `<topic>` corresponds to one SQL table.
//
// `<schema_id>` changes whenever the SQL table schema changes, which allows us
// to guarantee to users that _all entries in a given file have the same
// schema_.
//
// `<uniquer>` is used to keep nodes in a cluster from overwriting each other's data and
// should be ignored by external users. It also keeps a single node from overwriting its
// own data if there are multiple changefeeds, or if a changefeed gets
// canceled/restarted/zombied. Internally, it's generated by
// `<session_id>-<node_id>-<sink_id>-<file_id>` where `<sink_id>` is a unique id for each
// cloudStorageSink in a running process, `<file_id>` is a unique id for each file written
// by a given `<sink_id>` and <session_id> is a unique identifying string for the job
// session running the `changeAggregator` that owns this sink.
//
// `<ext>` implies the format of the file: currently the only option is
// `ndjson`, which means a text file conforming to the "Newline Delimited JSON"
// spec.
//
// This naming convention of data files is carefully chosen in order to preserve
// the external ordering guarantees of CDC. Naming output files in this fashion
// provides monotonicity among files emitted by a given sink for a given table
// name, table schema version pair within a given job session. This ensures that
// all row updates for a given span are read in an order that preserves the CDC
// ordering guarantees, even in the presence of job restarts (see proof below).
// Each record in the data files is a value, keys are not included, so the
// `envelope` option must be set to `value_only`. Within a file, records are not
// guaranteed to be sorted by timestamp. A duplicate of some records might exist
// in a different file or even in the same file.
//
//
// The resolved timestamp files are named `<timestamp>.RESOLVED`. This is
// carefully done so that we can offer the following external guarantee: At any
// given time, if the files are iterated in lexicographic filename order,
// then encountering any filename containing `RESOLVED` means that everything
// before it is finalized (and thus can be ingested into some other system and
// deleted, included in hive queries, etc). A typical user of cloudStorageSink
// would periodically do exactly this.
//
// Still TODO is writing out data schemas, Avro support, bounding memory usage.
//
// Now what follows is a proof of why the above is correct even in the presence
// of multiple job restarts. We begin by establishing some terminology and by
// formally (re)stating some invariants about the underlying system.
//
// Terminology
// 1. Characters A,B...Z refer to job sessions.
// 2. Ai, for i in Nat, refers to `the filename of the i'th data file
// emitted by session A`. Note that because of the invariants we will state,
// this can also be taken to mean "the filename of lexically the i'th data
// file emitted by session A". This is a notation simply used for convenience.
// 3. Ae > Bf refers to a lexical comparison of Ae and Bf.
// 4. ts(Xi) refers to the <timestamp> part of Xi.
//
// Invariants
// 1. We assume that the ordering guarantee (1) stated at the beginning of this
// comment blob is upheld by the underlying system. More specifically, this proof
// only proves correctness of the cloudStorageSink, not the entire system.
// To re-state, if the rows are read in the order they are emitted by the underlying
// system, it is impossible to see a previously unseen timestamp that is lower
// than some timestamp we've seen before.
// 2. Data files emitted by a single session of a changefeed job are lexically
// ordered exactly as they were emitted. Xi lexically precedes X(i-1), for i in
// Nat, for all job sessions X. The naming convention described above guarantees
// this.
// 3. Data files are named using the successor of the "local frontier" timestamp as of the
// time the last `Flush()` call was made (or StatementTime in case `Flush()` hasn't been
// called yet). Since all EmitRow calls are guaranteed to be for rows that equal or
// succeed this timestamp, ts(Xi) is an inclusive lower bound for the rows contained
// inside Xi.
// 4. When a job restarts, the new job session starts with a catch-up scan
// from the last globally resolved timestamp of the changefeed. This catch-up
// scan replays all rows since this resolved timestamp preserving invariant 1.
//
// Corollary 1: It is impossible to see a previously unseen timestamp that is
// lower than any timestamp seen thus far, in a lexical ordering of files if the
// files satisfy invariant 2 and the underlying system satisfies invariant 1.
//
// Note that correctness does not necessarily imply invariants 1 and 2.
//
// Lemma 1: Given two totally ordered sets of files X and Y that preserve CDC's ordering
// guarantee along with invariants 3 and 4, their union produces a totally ordered set of
// files that preserves this guarantee.
// Proof of lemma: Lets refer to the data filenames emitted by these sessions as X1,X2....
// and similarly for session Y. Additionally, lets refer to the last file ever emitted by
// session X as Xn, for some n in Nat. Now lexically speaking there are 2 cases here: 1.
// Y1 < Xn: For the sake of contradiction, let's assume there is a violation here. Since
// there is a total lexical ordering among files in each set individually, we must have
// read Y(e-1) before Ye, for all e in Nat. Similarly for X. Without loss of generality,
// lets say there are 2 files Ye and Xf such that (1.1) Ye < Xf and Xf contains an unseen
// timestamp that is lower than at least one timestamp seen in Ye. call this timestamp t.
// More explicitly, it must be the case that this timestamp t does not exist in any files
// Y1...Ye. This must mean that timestamp t lies before the starting point of session Y's
// catch-up scan (again from invariant 4). Thus it must be the case that (1.2) ts(Y1) > t.
// Now, due to invariant 3, we know that we won't see any rows in a file Xi with a
// timestamp that is lower than ts(Xi). This must mean that (1.3) ts(Xf) <= t. Statements
// 1.1, 1.2 and 1.3 together give us a contradiction. 2. Y1 > Xn. This case means that all
// data files of session Y lexically succeed all the data files of session X. This means
// that all data files are ordered monotonically relative to when they were emitted, this
// gives us invariant 2 (but for 2 sessions). Correctness follows from this and invariant
// 1. Note that Y1 == Xn is not possible because sessions are assigned unique session IDs.
// QED.
//
// Proof of correctness: It is impossible to see a previously unseen timestamp that is
// lower than any timestamp seen thus far, across n job sessions for all n, n in Nat. We
// do this by induction, let k be the number of job sessions a changefeed job goes
// through:
// Case k = 1: Correctness for this case follows from corollary 1.
// Case k = 2: This follows from lemma 1 stated above.
// Case k > 2 (induction case): Assume that the statement of the proof is true for the
// output of a changefeed job with k sessions. We will show that it must also be true for
// the output of a changefeed job with k+1 sessions. Let's refer to the first k jobs as
// P1,P2,..PK, and the k+1st job as Q. Now, since we assumed the statement is true for
// P1,P2...PK, it must produce a totally ordered (lexical ordering) set of files that
// satisfies requirements of lemma 1. So we can consider these k jobs conceptually as one
// job (call it P). Now, we're back to the case where k = 2 with jobs P and Q. Thus, by
// induction we have the required proof.
//
type cloudStorageSink struct {
	srcID             base.SQLInstanceID
	sinkID            int64
	targetMaxFileSize int64
	settings          *cluster.Settings
	partitionFormat   string

	ext          string
	rowDelimiter []byte

	compression string

	es cloud.ExternalStorage

	// These are fields to track information needed to output files based on the naming
	// convention described above. See comment on cloudStorageSink above for more details.
	fileID int64
	files  *btree.BTree // of *cloudStorageSinkFile

	timestampOracle timestampLowerBoundOracle
	jobSessionID    string
	// We keep track of the successor of the least resolved timestamp in the local
	// frontier as of the time of the last `Flush()` call. If `Flush()` hasn't been
	// called, these fields are based on the statement time of the changefeed.
	dataFileTs        string
	dataFilePartition string
	prevFilename      string

	// Memory used by this sink
	mem mon.BoundAccount
}

const sinkCompressionGzip = "gzip"

var cloudStorageSinkIDAtomic int64

func makeCloudStorageSink(
	ctx context.Context,
	u sinkURL,
	srcID base.SQLInstanceID,
	settings *cluster.Settings,
	opts map[string]string,
	timestampOracle timestampLowerBoundOracle,
	makeExternalStorageFromURI cloud.ExternalStorageFromURIFactory,
	user security.SQLUsername,
	acc mon.BoundAccount,
) (Sink, error) {
	var targetMaxFileSize int64 = 16 << 20 // 16MB
	if fileSizeParam := u.consumeParam(changefeedbase.SinkParamFileSize); fileSizeParam != `` {
		var err error
		if targetMaxFileSize, err = humanizeutil.ParseBytes(fileSizeParam); err != nil {
			return nil, pgerror.Wrapf(err, pgcode.Syntax, `parsing %s`, fileSizeParam)
		}
	}
	u.Scheme = strings.TrimPrefix(u.Scheme, `experimental-`)

	// Date partitioning is pretty standard, so no override for now, but we could
	// plumb one down if someone needs it.
	const defaultPartitionFormat = `2006-01-02`

	sinkID := atomic.AddInt64(&cloudStorageSinkIDAtomic, 1)
	s := &cloudStorageSink{
		srcID:             srcID,
		sinkID:            sinkID,
		settings:          settings,
		targetMaxFileSize: targetMaxFileSize,
		files:             btree.New(8),
		partitionFormat:   defaultPartitionFormat,
		timestampOracle:   timestampOracle,
		// TODO(dan,ajwerner): Use the jobs framework's session ID once that's available.
		jobSessionID: generateChangefeedSessionID(),
		mem:          acc,
	}
	if timestampOracle != nil {
		s.dataFileTs = cloudStorageFormatTime(timestampOracle.inclusiveLowerBoundTS())
		s.dataFilePartition = timestampOracle.inclusiveLowerBoundTS().GoTime().Format(s.partitionFormat)
	}

	switch changefeedbase.FormatType(opts[changefeedbase.OptFormat]) {
	case changefeedbase.OptFormatJSON:
		// TODO(dan): It seems like these should be on the encoder, but that
		// would require a bit of refactoring.
		s.ext = `.ndjson`
		s.rowDelimiter = []byte{'\n'}
	default:
		return nil, errors.Errorf(`this sink is incompatible with %s=%s`,
			changefeedbase.OptFormat, opts[changefeedbase.OptFormat])
	}

	switch changefeedbase.EnvelopeType(opts[changefeedbase.OptEnvelope]) {
	case changefeedbase.OptEnvelopeWrapped:
	default:
		return nil, errors.Errorf(`this sink is incompatible with %s=%s`,
			changefeedbase.OptEnvelope, opts[changefeedbase.OptEnvelope])
	}

	if _, ok := opts[changefeedbase.OptKeyInValue]; !ok {
		return nil, errors.Errorf(`this sink requires the WITH %s option`, changefeedbase.OptKeyInValue)
	}

	if codec, ok := opts[changefeedbase.OptCompression]; ok && codec != "" {
		if strings.EqualFold(codec, "gzip") {
			s.compression = sinkCompressionGzip
			s.ext = s.ext + ".gz"
		} else {
			return nil, errors.Errorf(`unsupported compression codec %q`, codec)
		}
	}

	var err error
	if s.es, err = makeExternalStorageFromURI(ctx, u.String(), user); err != nil {
		return nil, err
	}

	return s, nil
}

func (s *cloudStorageSink) getOrCreateFile(topic TopicDescriptor) *cloudStorageSinkFile {
	key := cloudStorageSinkKey{topic.GetName(), int64(topic.GetVersion())}
	if item := s.files.Get(key); item != nil {
		return item.(*cloudStorageSinkFile)
	}
	f := &cloudStorageSinkFile{
		cloudStorageSinkKey: key,
	}
	switch s.compression {
	case sinkCompressionGzip:
		f.codec = gzip.NewWriter(&f.buf)
	}
	s.files.ReplaceOrInsert(f)
	return f
}

// EmitRow implements the Sink interface.
func (s *cloudStorageSink) EmitRow(
	ctx context.Context, topic TopicDescriptor, key, value []byte, updated hlc.Timestamp,
) error {
	if s.files == nil {
		return errors.New(`cannot EmitRow on a closed sink`)
	}

	file := s.getOrCreateFile(topic)

	oldCap := file.buf.Cap()
	if _, err := file.Write(value); err != nil {
		return err
	}
	if _, err := file.Write(s.rowDelimiter); err != nil {
		return err
	}

	// Grow buffered memory.  It's okay that we do it after the fact
	// (and if not, we're in a deeper problem and probably OOMed by now).
	if err := s.mem.Grow(ctx, int64(file.buf.Cap()-oldCap)); err != nil {
		return err
	}

	if int64(file.buf.Len()) > s.targetMaxFileSize {
		if err := s.flushTopicVersions(ctx, file.topic, file.schemaID); err != nil {
			return err
		}
	}
	return nil
}

// EmitResolvedTimestamp implements the Sink interface.
func (s *cloudStorageSink) EmitResolvedTimestamp(
	ctx context.Context, encoder Encoder, resolved hlc.Timestamp,
) error {
	if s.files == nil {
		return errors.New(`cannot EmitRow on a closed sink`)
	}

	var noTopic string
	payload, err := encoder.EncodeResolvedTimestamp(ctx, noTopic, resolved)
	if err != nil {
		return err
	}
	// Don't need to copy payload because we never buffer it anywhere.

	part := resolved.GoTime().Format(s.partitionFormat)
	filename := fmt.Sprintf(`%s.RESOLVED`, cloudStorageFormatTime(resolved))
	if log.V(1) {
		log.Infof(ctx, "writing file %s %s", filename, resolved.AsOfSystemTime())
	}
	return cloud.WriteFile(ctx, s.es, filepath.Join(part, filename), bytes.NewReader(payload))
}

// flushTopicVersions flushes all open files for the provided topic up to and
// including maxVersionToFlush.
//
// To understand why we need to do this, consider the following example in case
// we didn't have this logic:
//
//  1. The sink starts buffering a file for schema 1.
//  2. It then starts buffering a file for schema 2.
//  3. The newer, schema 2 file exceeds the file size threshold and thus gets
//     flushed at timestamp x with fileID 0.
//  4. The older, schema 1 file is also flushed at timestamp x and thus is
//     assigned a fileID greater than 0.
//
// This would lead to the older file being lexically ordered after the newer,
// schema 2 file, leading to a violation of our ordering guarantees (see comment
// on cloudStorageSink)
func (s *cloudStorageSink) flushTopicVersions(
	ctx context.Context, topic string, maxVersionToFlush int64,
) (err error) {
	var toRemoveAlloc [2]int64    // generally avoid allocating
	toRemove := toRemoveAlloc[:0] // schemaIDs of flushed files
	gte := cloudStorageSinkKey{topic: topic}
	lt := cloudStorageSinkKey{topic: topic, schemaID: maxVersionToFlush + 1}
	s.files.AscendRange(gte, lt, func(i btree.Item) (wantMore bool) {
		f := i.(*cloudStorageSinkFile)
		if err = s.flushFile(ctx, f); err == nil {
			toRemove = append(toRemove, f.schemaID)
		}
		return err == nil
	})
	for _, v := range toRemove {
		s.files.Delete(cloudStorageSinkKey{topic: topic, schemaID: v})
	}
	return err
}

// Flush implements the Sink interface.
func (s *cloudStorageSink) Flush(ctx context.Context) error {
	if s.files == nil {
		return errors.New(`cannot Flush on a closed sink`)
	}

	var err error
	s.files.Ascend(func(i btree.Item) (wantMore bool) {
		err = s.flushFile(ctx, i.(*cloudStorageSinkFile))
		return err == nil
	})
	if err != nil {
		return err
	}
	s.files.Clear(true /* addNodesToFreeList */)

	// Record the least resolved timestamp being tracked in the frontier as of this point,
	// to use for naming files until the next `Flush()`. See comment on cloudStorageSink
	// for an overview of the naming convention and proof of correctness.
	s.dataFileTs = cloudStorageFormatTime(s.timestampOracle.inclusiveLowerBoundTS())
	s.dataFilePartition = s.timestampOracle.inclusiveLowerBoundTS().GoTime().Format(s.partitionFormat)
	return nil
}

// file should not be used after flushing.
func (s *cloudStorageSink) flushFile(ctx context.Context, file *cloudStorageSinkFile) error {
	if file.rawSize == 0 {
		// This method shouldn't be called with an empty file, but be defensive
		// about not writing empty files anyway.
		return nil
	}

	// Release memory allocated for this file.  Note, closing codec
	// below may as well write more data to our buffer (and that may cause buffer
	// to grow due to reallocation).  But we don't account for that additional memory
	// because a) we don't know if buffer will be resized (nor by how much), and
	// b) if we're out of memory we'd OOMed when trying to close codec anyway.
	defer func(delta int) {
		s.mem.Shrink(ctx, int64(delta))
	}(file.buf.Cap())

	if file.codec != nil {
		if err := file.codec.Close(); err != nil {
			return err
		}
	}

	// We use this monotonically increasing fileID to ensure correct ordering
	// among files emitted at the same timestamp during the same job session.
	fileID := s.fileID
	s.fileID++
	// Pad file ID to maintain lexical ordering among files from the same sink.
	// Note that we use `-` here to delimit the filename because we want
	// `%d.RESOLVED` files to lexicographically succeed data files that have the
	// same timestamp. This works because ascii `-` < ascii '.'.
	filename := fmt.Sprintf(`%s-%s-%d-%d-%08x-%s-%x%s`, s.dataFileTs,
		s.jobSessionID, s.srcID, s.sinkID, fileID, file.topic, file.schemaID, s.ext)
	if s.prevFilename != "" && filename < s.prevFilename {
		return errors.AssertionFailedf("error: detected a filename %s that lexically "+
			"precedes a file emitted before: %s", filename, s.prevFilename)
	}
	s.prevFilename = filename
	if err := cloud.WriteFile(ctx, s.es, filepath.Join(s.dataFilePartition, filename), bytes.NewReader(file.buf.Bytes())); err != nil {
		return err
	}
	return nil
}

// Close implements the Sink interface.
func (s *cloudStorageSink) Close() error {
	s.files = nil
	s.mem.Close(context.Background())
	return s.es.Close()
}

// Dial implements the Sink interface.
func (s *cloudStorageSink) Dial() error {
	return nil
}

type cloudStorageSinkKey struct {
	topic    string
	schemaID int64
}

func (k cloudStorageSinkKey) Less(other btree.Item) bool {
	switch other := other.(type) {
	case *cloudStorageSinkFile:
		return keyLess(k, other.cloudStorageSinkKey)
	case cloudStorageSinkKey:
		return keyLess(k, other)
	default:
		panic(errors.Errorf("unexpected item type %T", other))
	}
}

func keyLess(a, b cloudStorageSinkKey) bool {
	if a.topic == b.topic {
		return a.schemaID < b.schemaID
	}
	return a.topic < b.topic
}
