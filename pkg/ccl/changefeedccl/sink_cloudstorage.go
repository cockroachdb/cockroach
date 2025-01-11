// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package changefeedccl

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"io"
	"net/url"
	"path/filepath"
	"strings"
	"sync/atomic"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl/changefeedbase"
	"github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl/kvevent"
	"github.com/cockroachdb/cockroach/pkg/cloud"
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/util/ctxgroup"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/humanizeutil"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/log/logcrash"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
	"github.com/google/btree"
	// Placeholder for pgzip and zdstd.
	_ "github.com/klauspost/compress/zstd"
	_ "github.com/klauspost/pgzip"
)

func isCloudStorageSink(u *url.URL) bool {
	switch u.Scheme {
	case changefeedbase.SinkSchemeCloudStorageS3, changefeedbase.SinkSchemeCloudStorageGCS,
		changefeedbase.SinkSchemeCloudStorageNodelocal, changefeedbase.SinkSchemeCloudStorageHTTP,
		changefeedbase.SinkSchemeCloudStorageHTTPS, changefeedbase.SinkSchemeCloudStorageAzure:
		return true
	// During the deprecation period, we need to keep parsing these as cloudstorage for backwards
	// compatibility. Afterwards we'll either remove them or move them to webhook.
	case changefeedbase.DeprecatedSinkSchemeHTTP, changefeedbase.DeprecatedSinkSchemeHTTPS:
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
	created       time.Time
	codec         io.WriteCloser
	rawSize       int
	numMessages   int
	buf           bytes.Buffer
	alloc         kvevent.Alloc
	oldestMVCC    hlc.Timestamp
	parquetCodec  *parquetWriter
	allocCallback func(delta int64)
}

func (f *cloudStorageSinkFile) mergeAlloc(other *kvevent.Alloc) {
	prev := f.alloc.Bytes()
	f.alloc.Merge(other)
	f.allocCallback(f.alloc.Bytes() - prev)
}

func (f *cloudStorageSinkFile) releaseAlloc(ctx context.Context) {
	prev := f.alloc.Bytes()
	f.alloc.Release(ctx)
	f.allocCallback(-prev)
}

func (f *cloudStorageSinkFile) adjustBytesToTarget(ctx context.Context, targetBytes int64) {
	prev := f.alloc.Bytes()
	f.alloc.AdjustBytesToTarget(ctx, targetBytes)
	f.allocCallback(f.alloc.Bytes() - prev)
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
type cloudStorageSink struct {
	srcID  base.SQLInstanceID
	sinkID int64

	// targetMaxFileSize is the max target file size in bytes.
	targetMaxFileSize int64
	settings          *cluster.Settings
	partitionFormat   string
	topicNamer        *TopicNamer

	ext          string
	rowDelimiter []byte

	compression compressionAlgo

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
	metrics           metricsRecorder

	asyncFlushActive bool
	flushGroup       ctxgroup.Group
	asyncFlushCh     chan flushRequest // channel for submitting flush requests.
	asyncFlushTermCh chan struct{}     // channel closed by async flusher to indicate an error
	asyncFlushErr    error             // set by async flusher, prior to closing asyncFlushTermCh

	// testingKnobs may be nil if no knobs are set.
	testingKnobs *TestingKnobs
}

type flushRequest struct {
	file  *cloudStorageSinkFile
	dest  string
	flush chan struct{}
}

func (s *cloudStorageSink) getConcreteType() sinkType {
	return sinkTypeCloudstorage
}

var cloudStorageSinkIDAtomic int64

// Files that are emitted can be partitioned by their earliest event time,
// for example being emitted to topic/date/file.ndjson, or further split by hour.
// Note that a file may contain events with timestamps that would normally
// fall under a different partition had they been flushed later.
var partitionDateFormats = map[string]string{
	"flat":   "/",
	"daily":  "2006-01-02/",
	"hourly": "2006-01-02/15/",
}
var defaultPartitionFormat = partitionDateFormats["daily"]

// flushQueueDepth puts a limit on how many flush requests
// may be outstanding, before we block.
// In reality, we will block much sooner than this limit due
// to blocking buffer memory limits (in its default configuration);
// We just want this setting to be sufficiently large, but not
// so large as to have extremely large flush queues.
// The default of 256, with the default file size of 16MB, gives us
// a queue of 2.5GB of outstanding flush data.
const flushQueueDepth = 256

func makeCloudStorageSink(
	ctx context.Context,
	u *changefeedbase.SinkURL,
	srcID base.SQLInstanceID,
	settings *cluster.Settings,
	encodingOpts changefeedbase.EncodingOptions,
	timestampOracle timestampLowerBoundOracle,
	makeExternalStorageFromURI cloud.ExternalStorageFromURIFactory,
	user username.SQLUsername,
	mb metricsRecorderBuilder,
	testingKnobs *TestingKnobs,
) (Sink, error) {
	var targetMaxFileSize int64 = 16 << 20 // 16MB
	if fileSizeParam := u.ConsumeParam(changefeedbase.SinkParamFileSize); fileSizeParam != `` {
		var err error
		if targetMaxFileSize, err = humanizeutil.ParseBytes(fileSizeParam); err != nil {
			return nil, pgerror.Wrapf(err, pgcode.Syntax, `parsing %s`, fileSizeParam)
		}
	}
	u.Scheme = strings.TrimPrefix(u.Scheme, `experimental-`)
	u.Scheme = strings.TrimPrefix(u.Scheme, `file-`)

	sinkID := atomic.AddInt64(&cloudStorageSinkIDAtomic, 1)
	sessID, err := generateChangefeedSessionID()
	if err != nil {
		return nil, err
	}

	// Using + rather than . here because some consumers may be relying on there being exactly
	// one '.' in the filepath, and '+' shares with '-' the useful property of being
	// lexicographically earlier than '.'.
	tn, err := MakeTopicNamer(changefeedbase.Targets{}, WithJoinByte('+'))
	if err != nil {
		return nil, err
	}

	s := &cloudStorageSink{
		srcID:             srcID,
		sinkID:            sinkID,
		settings:          settings,
		targetMaxFileSize: targetMaxFileSize,
		files:             btree.New(8),
		partitionFormat:   defaultPartitionFormat,
		timestampOracle:   timestampOracle,
		// TODO(dan,ajwerner): Use the jobs framework's session ID once that's available.
		jobSessionID:     sessID,
		topicNamer:       tn,
		asyncFlushActive: enableAsyncFlush.Get(&settings.SV),
		// TODO (yevgeniy): Consider adding ctx to Dial method instead.
		flushGroup:       ctxgroup.WithContext(ctx),
		asyncFlushCh:     make(chan flushRequest, flushQueueDepth),
		asyncFlushTermCh: make(chan struct{}),
		testingKnobs:     testingKnobs,
	}
	s.flushGroup.GoCtx(s.asyncFlusher)

	if partitionFormat := u.ConsumeParam(changefeedbase.SinkParamPartitionFormat); partitionFormat != "" {
		dateFormat, ok := partitionDateFormats[partitionFormat]
		if !ok {
			return nil, errors.Errorf("invalid partition_format of %s", partitionFormat)
		}

		s.partitionFormat = dateFormat
	}

	if s.timestampOracle != nil {
		s.setDataFileTimestamp()
	}

	switch encodingOpts.Format {
	case changefeedbase.OptFormatJSON:
		// TODO(dan): It seems like these should be on the encoder, but that
		// would require a bit of refactoring.
		s.ext = `.ndjson`
		s.rowDelimiter = []byte{'\n'}
	case changefeedbase.OptFormatCSV:
		// TODO(dan): It seems like these should be on the encoder, but that
		// would require a bit of refactoring.
		s.ext = `.csv`
		s.rowDelimiter = []byte{'\n'}
	case changefeedbase.OptFormatParquet:
		s.ext = `.parquet`
		s.rowDelimiter = nil
	default:
		return nil, errors.Errorf(`this sink is incompatible with %s=%s`,
			changefeedbase.OptFormat, encodingOpts.Format)
	}

	switch encodingOpts.Envelope {
	case changefeedbase.OptEnvelopeWrapped, changefeedbase.OptEnvelopeBare:
	default:
		return nil, errors.Errorf(`this sink is incompatible with %s=%s`,
			changefeedbase.OptEnvelope, encodingOpts.Envelope)
	}

	if encodingOpts.Envelope != changefeedbase.OptEnvelopeBare {
		encodingOpts.KeyInValue = true
	}

	if codec := encodingOpts.Compression; codec != "" {
		algo, ext, err := compressionFromString(codec)
		if err != nil {
			return nil, err
		}
		s.compression = algo
		if encodingOpts.Format != changefeedbase.OptFormatParquet {
			s.ext = s.ext + ext
		}
	}

	// We make the external storage with a nil IOAccountingInterceptor since we
	// record usage metrics via s.metrics.
	s.es, err = makeExternalStorageFromURI(ctx, u.String(), user, cloud.WithIOAccountingInterceptor(nil), cloud.WithClientName("cdc"))
	if err != nil {
		return nil, err
	}
	if mb != nil && s.es != nil {
		s.metrics = mb(s.es.RequiresExternalIOAccounting())
	} else {
		s.metrics = (*sliMetrics)(nil)
	}

	if encodingOpts.Format == changefeedbase.OptFormatParquet {
		parquetSinkWithEncoder, err := makeParquetCloudStorageSink(s)
		if err != nil {
			return nil, err
		}
		// For parquet, we will always use the compression internally supported by
		// parquet codec.
		s.compression = ""
		return parquetSinkWithEncoder, nil
	}

	return s, nil
}

func (s *cloudStorageSink) getOrCreateFile(
	topic TopicDescriptor, eventMVCC hlc.Timestamp,
) (*cloudStorageSinkFile, error) {
	name, _ := s.topicNamer.Name(topic)
	key := cloudStorageSinkKey{name, int64(topic.GetVersion())}
	if item := s.files.Get(key); item != nil {
		f := item.(*cloudStorageSinkFile)
		if eventMVCC.Less(f.oldestMVCC) {
			f.oldestMVCC = eventMVCC
		}
		return f, nil
	}
	f := &cloudStorageSinkFile{
		created:             timeutil.Now(),
		cloudStorageSinkKey: key,
		oldestMVCC:          eventMVCC,
		allocCallback:       s.metrics.makeCloudstorageFileAllocCallback(),
	}

	if s.compression.enabled() {
		codec, err := newCompressionCodec(s.compression, &s.settings.SV, &f.buf)
		if err != nil {
			return nil, err
		}
		f.codec = codec
	}
	s.files.ReplaceOrInsert(f)
	return f, nil
}

// EmitRow implements the Sink interface.
func (s *cloudStorageSink) EmitRow(
	ctx context.Context,
	topic TopicDescriptor,
	key, value []byte,
	updated, mvcc hlc.Timestamp,
	alloc kvevent.Alloc,
) (retErr error) {
	if s.files == nil {
		return errors.New(`cannot EmitRow on a closed sink`)
	}

	defer func() {
		if !s.compression.enabled() {
			return
		}
		if retErr == nil {
			retErr = ctx.Err()
		}
		if retErr != nil {
			// If we are returning an error, immediately close all compression
			// codecs to release resources.  This step is also done in the
			// Close() method, but doing this clean-up as soon as we know
			// an error has occurred, ensures that we do not leak resources,
			// even if the Close() method is not called.
			retErr = errors.CombineErrors(retErr, s.closeAllCodecs())
		}
	}()

	s.metrics.recordMessageSize(int64(len(key) + len(value)))
	file, err := s.getOrCreateFile(topic, mvcc)
	if err != nil {
		return err
	}
	file.mergeAlloc(&alloc)

	if _, err := file.Write(value); err != nil {
		return err
	}
	if _, err := file.Write(s.rowDelimiter); err != nil {
		return err
	}
	file.numMessages++

	if int64(file.buf.Len()) > s.targetMaxFileSize {
		s.metrics.recordSizeBasedFlush()
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
	// TODO: There should be a better way to check if the sink is closed.
	if s.files == nil {
		return errors.New(`cannot EmitRow on a closed sink`)
	}

	defer s.metrics.recordResolvedCallback()()

	var noTopic string
	payload, err := encoder.EncodeResolvedTimestamp(ctx, noTopic, resolved)
	if err != nil {
		return err
	}

	// Wait for previously issued async flush requests to complete
	// before we write  resolved time stamp file.
	if err := s.waitAsyncFlush(ctx); err != nil {
		return errors.Wrapf(err, "while emitting resolved timestamp")
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
	if err != nil {
		return err
	}

	// Allow synchronization with the async flusher to happen.
	if s.testingKnobs != nil && s.testingKnobs.AsyncFlushSync != nil {
		s.testingKnobs.AsyncFlushSync()
	}

	// Wait for the async flush to complete before clearing files.
	// Note that if waitAsyncFlush returns an error some successfully
	// flushed files may not be removed from s.files. This is ok, since
	// the error will trigger the sink to be closed, and we will only use
	// s.files to ensure that the codecs are closed before deallocating it.
	err = s.waitAsyncFlush(ctx)
	if err != nil {
		return err
	}

	// Files need to be cleared after the flush completes, otherwise file
	// resources may be leaked.
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

	s.metrics.recordFlushRequestCallback()()

	var err error
	s.files.Ascend(func(i btree.Item) (wantMore bool) {
		err = s.flushFile(ctx, i.(*cloudStorageSinkFile))
		return err == nil
	})
	if err != nil {
		return err
	}
	// Allow synchronization with the async flusher to happen.
	if s.testingKnobs != nil && s.testingKnobs.AsyncFlushSync != nil {
		s.testingKnobs.AsyncFlushSync()
	}
	s.setDataFileTimestamp()

	// Note that if waitAsyncFlush returns an error some successfully
	// flushed files may not be removed from s.files. This is ok, since
	// the error will trigger the sink to be closed, and we will only use
	// s.files to ensure that the codecs are closed before deallocating it.
	err = s.waitAsyncFlush(ctx)
	if err != nil {
		return err
	}
	// Files need to be cleared after the flush completes, otherwise file resources
	// may not be released properly when closing the sink.
	s.files.Clear(true /* addNodesToFreeList */)
	return nil
}

func (s *cloudStorageSink) setDataFileTimestamp() {
	// Record the least resolved timestamp being tracked in the frontier as of this point,
	// to use for naming files until the next `Flush()`. See comment on cloudStorageSink
	// for an overview of the naming convention and proof of correctness.
	ts := s.timestampOracle.inclusiveLowerBoundTS()
	s.dataFileTs = cloudStorageFormatTime(ts)
	s.dataFilePartition = ts.GoTime().Format(s.partitionFormat)
}

// enableAsyncFlush controls async flushing behavior for this sink.
var enableAsyncFlush = settings.RegisterBoolSetting(
	settings.ApplicationLevel,
	"changefeed.cloudstorage.async_flush.enabled",
	"enable async flushing",
	true,
)

// waitAsyncFlush waits until all async flushes complete.
func (s *cloudStorageSink) waitAsyncFlush(ctx context.Context) error {
	done := make(chan struct{})
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-s.asyncFlushTermCh:
		return s.asyncFlushErr
	case s.asyncFlushCh <- flushRequest{flush: done}:
	}

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-s.asyncFlushTermCh:
		return s.asyncFlushErr
	case <-done:
		return nil
	}
}

var logQueueDepth = log.Every(30 * time.Second)

// flushFile flushes file to the cloud storage.
// file should not be used after flushing.
func (s *cloudStorageSink) flushFile(ctx context.Context, file *cloudStorageSinkFile) error {
	asyncFlushEnabled := enableAsyncFlush.Get(&s.settings.SV)
	if s.asyncFlushActive && !asyncFlushEnabled {
		// Async flush behavior was turned off --  drain any active flush requests
		// before flushing this file.
		if err := s.waitAsyncFlush(ctx); err != nil {
			return err
		}
	}
	s.asyncFlushActive = asyncFlushEnabled

	// If using parquet, we need to finish off writing the entire file.
	// Closing the parquet codec will append some metadata to the file.
	if file.parquetCodec != nil {
		if err := file.parquetCodec.close(); err != nil {
			return err
		}
		file.rawSize = file.buf.Len()
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
		err := errors.AssertionFailedf("error: detected a filename %s that lexically "+
			"precedes a file emitted before: %s", filename, s.prevFilename)
		logcrash.ReportOrPanic(ctx, &s.settings.SV, "incorrect filename order: %v", err)
		return err
	}
	s.prevFilename = filename
	dest := filepath.Join(s.dataFilePartition, filename)

	if !asyncFlushEnabled {
		return file.flushToStorage(ctx, s.es, dest, s.metrics)
	}

	// Try to submit flush request, but produce warning message
	// if we can't.
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-s.asyncFlushTermCh:
		return s.asyncFlushErr
	case s.asyncFlushCh <- flushRequest{file: file, dest: dest}:
		return nil
	default:
		if logQueueDepth.ShouldLog() {
			log.Infof(ctx, "changefeed flush queue is full; ~%d bytes to flush",
				flushQueueDepth*s.targetMaxFileSize)
		}
	}

	// Queue was full, block until it's not.
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-s.asyncFlushTermCh:
		return s.asyncFlushErr
	case s.asyncFlushCh <- flushRequest{file: file, dest: dest}:
		return nil
	}
}

func (s *cloudStorageSink) asyncFlusher(ctx context.Context) error {
	defer close(s.asyncFlushTermCh)

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case req, ok := <-s.asyncFlushCh:
			if !ok {
				return nil // we're done
			}

			// handle flush request.
			if req.flush != nil {
				close(req.flush)
				continue
			}

			// Allow synchronization with the flushing routine to happen between getting
			// the flush request from the channel and completing the flush.
			if s.testingKnobs != nil && s.testingKnobs.AsyncFlushSync != nil {
				s.testingKnobs.AsyncFlushSync()
			}

			// flush file to storage.
			flushDone := s.metrics.recordFlushRequestCallback()
			err := req.file.flushToStorage(ctx, s.es, req.dest, s.metrics)
			flushDone()

			if err != nil {
				log.Errorf(ctx, "error flushing file to storage: %s", err)
				s.asyncFlushErr = err
				return err
			}
		}
	}
}

// flushToStorage writes out file into external storage into 'dest'.
func (f *cloudStorageSinkFile) flushToStorage(
	ctx context.Context, es cloud.ExternalStorage, dest string, m metricsRecorder,
) error {
	defer f.releaseAlloc(ctx)
	defer m.timers().DownstreamClientSend.Start()()

	if f.rawSize == 0 {
		// This method shouldn't be called with an empty file, but be defensive
		// about not writing empty files anyway.
		return nil
	}

	if f.codec != nil {
		if err := f.codec.Close(); err != nil {
			return err
		}
		// Reset reference to underlying codec to prevent accidental reuse.
		f.codec = nil
	}

	compressedBytes := f.buf.Len()
	if err := cloud.WriteFile(ctx, es, dest, bytes.NewReader(f.buf.Bytes())); err != nil {
		return err
	}
	m.recordEmittedBatch(f.created, f.numMessages, f.oldestMVCC, f.rawSize, compressedBytes)

	return nil
}

func (s *cloudStorageSink) closeAllCodecs() (err error) {
	// Close any codecs we might have in use and collect the first error if any
	// (other errors are ignored because they are likely going to be the same ones,
	// though based on the current compression implementation, the close method
	// should not return an error).
	// Codecs need to be closed because of the klauspost compression library implementation
	// details where it spins up go routines to perform compression in parallel.
	// Those go routines are cleaned up when the compression codec is closed.
	s.files.Ascend(func(i btree.Item) (wantMore bool) {
		f := i.(*cloudStorageSinkFile)
		if f.codec != nil {
			cErr := f.codec.Close()
			f.codec = nil
			if err == nil {
				err = cErr
			}
		}
		return true
	})
	return err
}

// Close implements the Sink interface.
func (s *cloudStorageSink) Close() error {
	err := s.closeAllCodecs()
	s.files = nil
	err = errors.CombineErrors(err, s.waitAsyncFlush(context.Background()))
	close(s.asyncFlushCh) // signal flusher to exit.
	err = errors.CombineErrors(err, s.flushGroup.Wait())
	return errors.CombineErrors(err, s.es.Close())
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

// generateChangefeedSessionID generates a unique string that is used to
// prevent overwriting of output files by the cloudStorageSink.
func generateChangefeedSessionID() (string, error) {
	// We read exactly 8 random bytes. 8 bytes should be enough because:
	// Consider that each new session for a changefeed job can occur at the
	// same highWater timestamp for its catch up scan. This session ID is
	// used to ensure that a session emitting files with the same timestamp
	// as the session before doesn't clobber existing files. Let's assume that
	// each of these runs for 0 seconds. Our node liveness duration is currently
	// 9 seconds, but let's go with a conservative duration of 1 second.
	// With 8 bytes using the rough approximation for the birthday problem
	// https://en.wikipedia.org/wiki/Birthday_problem#Square_approximation, we
	// will have a 50% chance of a single collision after sqrt(2^64) = 2^32
	// sessions. So if we start a new job every second, we get a coin flip chance of
	// single collision after 136 years. With this same approximation, we get
	// something like 220 days to have a 0.001% chance of a collision. In practice,
	// jobs are likely to run for longer and it's likely to take longer for
	// job adoption, so we should be good with 8 bytes. Similarly, it's clear that
	// 16 would be way overkill. 4 bytes gives us a 50% chance of collision after
	// 65K sessions at the same timestamp.
	const size = 8
	p := make([]byte, size)
	buf := make([]byte, hex.EncodedLen(size))
	if _, err := rand.Read(p); err != nil {
		return "", err
	}
	hex.Encode(buf, p)
	return string(buf), nil
}
