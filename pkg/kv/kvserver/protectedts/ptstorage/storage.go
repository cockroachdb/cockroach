// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

// Package ptstorage implements protectedts.Storage.
package ptstorage

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/protectedts"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/protectedts/ptpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/isql"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/cockroachdb/errors"
)

// TODO(ajwerner): Consider memory accounting.
// TODO(ajwerner): Add metrics.

// TODO(ajwerner): Provide some sort of reconciliation of metadata in the face
// of corruption. Not clear how or why such corruption might happen but if it
// does it might be nice to have an escape hatch. Perhaps another interface
// method which scans the records and updates the counts in the meta row
// accordingly.

// TODO(ajwerner): Hook into the alerts infrastructure and metrics to provide
// visibility into corruption when it is detected.

// Manager interacts with the durable state of the protectedts subsystem.
type Manager struct {
	settings *cluster.Settings
	knobs    *protectedts.TestingKnobs
}

// storage implements protectedts.Storage with a transaction.
type storage struct {
	txn      isql.Txn
	settings *cluster.Settings
	knobs    *protectedts.TestingKnobs
}

func (p *storage) Protect(ctx context.Context, r *ptpb.Record) error {
	if err := validateRecordForProtect(ctx, r, p.settings, p.knobs); err != nil {
		return err
	}

	meta := r.Meta
	if meta == nil {
		// v20.1 crashes in rowToRecord and Manager.Release if it finds a NULL
		// value in system.protected_ts_records.meta. v20.2 and above handle
		// this correctly, but we need to maintain mixed version compatibility
		// for at least one release.
		// TODO(nvanbenschoten): remove this for v21.1.
		meta = []byte{}
	}

	// The `target` column was added to `system.protected_ts_records` as part of
	// the tenant migration `AlterSystemProtectedTimestampAddColumn`. Prior to the
	// migration we should continue write records that protect `spans`.
	//
	// TODO(adityamaru): Delete in 22.2 once we exclusively protect `target`s.
	if useDeprecatedProtectedTSStorage(ctx, p.settings, p.knobs) || writeDeprecatedPTSRecord(p.knobs, r) {
		return p.deprecatedProtect(ctx, r, meta)
	}

	// Clear the `DeprecatedSpans` field even if it has been set by the caller.
	// Once the `AlterSystemProtectedTimestampAddColumn` migration has run, we
	// only want to persist the `target` on which the pts record applies. We have
	// already verified that the record has a valid `target`.
	r.DeprecatedSpans = nil
	s := makeSettings(p.settings)
	encodedTarget, err := protoutil.Marshal(&ptpb.Target{Union: r.Target.GetUnion(),
		IgnoreIfExcludedFromBackup: r.Target.IgnoreIfExcludedFromBackup})
	if err != nil { // how can this possibly fail?
		return errors.Wrap(err, "failed to marshal spans")
	}

	updateMeta := usePTSMetaTable(ctx, p.settings, p.knobs)
	query := protectInsertRecordCTE
	args := []interface{}{
		r.ID, r.Timestamp.AsOfSystemTime(),
		r.MetaType, meta,
		len(r.DeprecatedSpans), encodedTarget, encodedTarget}

	if updateMeta {
		query = protectQuery
		args = []interface{}{s.maxSpans, s.maxBytes, len(r.DeprecatedSpans),
			r.ID, r.Timestamp.AsOfSystemTime(),
			r.MetaType, meta,
			len(r.DeprecatedSpans), encodedTarget, encodedTarget}
	}

	it, err := p.txn.QueryIteratorEx(ctx, "protectedts-protect", p.txn.KV(),
		sessiondata.NodeUserSessionDataOverride,
		query,
		args...)
	if err != nil {
		return errors.Wrapf(err, "failed to write record %v", r.ID)
	}

	ok, err := it.Next(ctx)
	if err != nil {
		return errors.Wrapf(err, "failed to write record %v", r.ID)
	}
	if !ok {
		return errors.Newf("failed to write record %v", r.ID)
	}

	defer func() {
		if err := it.Close(); err != nil {
			log.Infof(ctx, "encountered %v when writing record %v", err, r.ID)
		}
	}()

	row := it.Cur()
	if failed := *row[0].(*tree.DBool); failed {
		if updateMeta {
			curBytes := int64(*row[1].(*tree.DInt))
			recordBytes := int64(len(encodedTarget) + len(r.Meta) + len(r.MetaType))
			if s.maxBytes > 0 && curBytes+recordBytes > s.maxBytes {
				return errors.WithHint(
					errors.Errorf("protectedts: limit exceeded: %d+%d > %d bytes", curBytes, recordBytes,
						s.maxBytes),
					"SET CLUSTER SETTING kv.protectedts.max_bytes to a higher value")
			}
		}
		return protectedts.ErrExists
	}

	return nil
}

func (p *storage) GetRecord(ctx context.Context, id uuid.UUID) (*ptpb.Record, error) {
	if useDeprecatedProtectedTSStorage(ctx, p.settings, p.knobs) {
		return p.deprecatedGetRecord(ctx, id)
	}
	row, err := p.txn.QueryRowEx(ctx, "protectedts-GetRecord", p.txn.KV(),
		sessiondata.NodeUserSessionDataOverride,
		getRecordQuery, id.GetBytesMut())
	if err != nil {
		return nil, errors.Wrapf(err, "failed to read record %v", id)
	}
	if len(row) == 0 {
		return nil, protectedts.ErrNotExists
	}
	var r ptpb.Record
	if err := rowToRecord(row, &r, false /* isDeprecatedRow */); err != nil {
		return nil, err
	}
	return &r, nil
}

func (p storage) MarkVerified(ctx context.Context, id uuid.UUID) error {
	numRows, err := p.txn.ExecEx(ctx, "protectedts-MarkVerified", p.txn.KV(),
		sessiondata.NodeUserSessionDataOverride,
		markVerifiedQuery, id.GetBytesMut())
	if err != nil {
		return errors.Wrapf(err, "failed to mark record %v as verified", id)
	}
	if numRows == 0 {
		return protectedts.ErrNotExists
	}
	return nil
}

func (p storage) Release(ctx context.Context, id uuid.UUID) error {
	query := releaseQueryWithMeta
	if !usePTSMetaTable(ctx, p.settings, p.knobs) {
		query = releaseQuery
	}
	numRows, err := p.txn.ExecEx(ctx, "protectedts-Release", p.txn.KV(),
		sessiondata.NodeUserSessionDataOverride,
		query, id.GetBytesMut())
	if err != nil {
		return errors.Wrapf(err, "failed to release record %v", id)
	}
	if numRows == 0 {
		return protectedts.ErrNotExists
	}
	return nil
}

func (p storage) GetMetadata(ctx context.Context) (ptpb.Metadata, error) {
	row, err := p.txn.QueryRowEx(ctx, "protectedts-GetMetadata", p.txn.KV(),
		sessiondata.NodeUserSessionDataOverride,
		getMetadataQuery)
	if err != nil {
		return ptpb.Metadata{}, errors.Wrap(err, "failed to read metadata")
	}
	if row == nil {
		return ptpb.Metadata{}, errors.New("failed to read metadata")
	}
	return ptpb.Metadata{
		Version:    uint64(*row[0].(*tree.DInt)),
		NumRecords: uint64(*row[1].(*tree.DInt)),
		NumSpans:   uint64(*row[2].(*tree.DInt)),
		TotalBytes: uint64(*row[3].(*tree.DInt)),
	}, nil
}

func (p storage) GetState(ctx context.Context) (ptpb.State, error) {
	md, err := p.GetMetadata(ctx)
	if err != nil {
		return ptpb.State{}, err
	}
	records, err := p.getRecords(ctx)
	if err != nil {
		return ptpb.State{}, err
	}
	return ptpb.State{
		Metadata: md,
		Records:  records,
	}, nil
}

func (p *storage) getRecords(ctx context.Context) ([]ptpb.Record, error) {
	if useDeprecatedProtectedTSStorage(ctx, p.settings, p.knobs) {
		return p.deprecatedGetRecords(ctx)
	}
	it, err := p.txn.QueryIteratorEx(ctx, "protectedts-GetRecords", p.txn.KV(),
		sessiondata.NodeUserSessionDataOverride, getRecordsQuery)
	if err != nil {
		return nil, errors.Wrap(err, "failed to read records")
	}

	var ok bool
	var records []ptpb.Record
	for ok, err = it.Next(ctx); ok; ok, err = it.Next(ctx) {
		var record ptpb.Record
		if err := rowToRecord(it.Cur(), &record, false /* isDeprecatedRow */); err != nil {
			log.Errorf(ctx, "failed to parse row as record: %v", err)
		}
		records = append(records, record)
	}
	if err != nil {
		return nil, errors.Wrap(err, "failed to read records")
	}
	return records, nil
}

func (p storage) UpdateTimestamp(ctx context.Context, id uuid.UUID, timestamp hlc.Timestamp) error {
	query := updateTimestampQuery
	if !usePTSMetaTable(ctx, p.settings, p.knobs) {
		query = updateTimestampUpsertRecordCTE
	}
	row, err := p.txn.QueryRowEx(ctx, "protectedts-update", p.txn.KV(),
		sessiondata.NodeUserSessionDataOverride,
		query, id.GetBytesMut(), timestamp.AsOfSystemTime())
	if err != nil {
		return errors.Wrapf(err, "failed to update record %v", id)
	}
	if len(row) == 0 {
		return protectedts.ErrNotExists
	}
	return nil
}

func (p *Manager) WithTxn(txn isql.Txn) protectedts.Storage {
	return &storage{
		txn:      txn,
		settings: p.settings,
		knobs:    p.knobs,
	}
}

var _ protectedts.Manager = (*Manager)(nil)

// TODO(adityamaru): Delete in 22.2.
func useDeprecatedProtectedTSStorage(
	ctx context.Context, st *cluster.Settings, knobs *protectedts.TestingKnobs,
) bool {
	return knobs.DisableProtectedTimestampForMultiTenant
}

func writeDeprecatedPTSRecord(knobs *protectedts.TestingKnobs, r *ptpb.Record) bool {
	return knobs != nil && knobs.WriteDeprecatedPTSRecords && r.DeprecatedSpans != nil && len(r.DeprecatedSpans) > 0
}

func usePTSMetaTable(
	ctx context.Context, st *cluster.Settings, knobs *protectedts.TestingKnobs,
) bool {
	return knobs.UseMetaTable
}

// New creates a new Storage.
func New(settings *cluster.Settings, knobs *protectedts.TestingKnobs) *Manager {
	if knobs == nil {
		knobs = &protectedts.TestingKnobs{}
	}
	return &Manager{settings: settings, knobs: knobs}
}

// rowToRecord parses a row as returned from the variants of getRecords and
// populates the passed *Record. If any errors are encountered during parsing,
// they are logged but not returned. Returning an error due to malformed data
// in the protected timestamp subsystem would create more problems than it would
// solve. Malformed records can still be removed (and hopefully will be).
//
// isDeprecatedRow indicates if the supplied row was generated by one of the
// deprecated PTS Manager methods, and as such, does not include the target
// column.
func rowToRecord(row tree.Datums, r *ptpb.Record, isDeprecatedRow bool) error {
	r.ID = row[0].(*tree.DUuid).UUID.GetBytes()
	tsDecimal := row[1].(*tree.DDecimal)
	ts, err := hlc.DecimalToHLC(&tsDecimal.Decimal)
	if err != nil {
		return errors.Wrapf(err, "failed to parse timestamp for %v", r.ID)
	}
	r.Timestamp = ts

	r.MetaType = string(*row[2].(*tree.DString))
	if row[3] != tree.DNull {
		if meta := row[3].(*tree.DBytes); len(*meta) > 0 {
			r.Meta = []byte(*meta)
		}
	}
	var spans Spans
	if err := protoutil.Unmarshal([]byte(*row[4].(*tree.DBytes)), &spans); err != nil {
		return errors.Wrapf(err, "failed to unmarshal span for %v", r.ID)
	}
	r.DeprecatedSpans = spans.Spans
	r.Verified = bool(*row[5].(*tree.DBool))

	if !isDeprecatedRow {
		target := &ptpb.Target{}
		targetDBytes, ok := row[6].(*tree.DBytes)
		if !ok {
			// We are reading a pre-22.1 protected timestamp record that has a NULL
			// target column, so there is nothing more to do.
			return nil
		}
		if err := protoutil.Unmarshal([]byte(*targetDBytes), target); err != nil {
			return errors.Wrapf(err, "failed to unmarshal target for %v", r.ID)
		}
		r.Target = target
	}
	return nil
}

func (p *storage) deprecatedProtect(ctx context.Context, r *ptpb.Record, meta []byte) error {
	s := makeSettings(p.settings)
	encodedSpans, err := protoutil.Marshal(&Spans{Spans: r.DeprecatedSpans})
	if err != nil { // how can this possibly fail?
		return errors.Wrap(err, "failed to marshal spans")
	}
	it, err := p.txn.QueryIteratorEx(ctx, "protectedts-deprecated-protect", p.txn.KV(),
		sessiondata.NodeUserSessionDataOverride,
		protectQueryWithoutTarget,
		s.maxSpans, s.maxBytes, len(r.DeprecatedSpans),
		r.ID, r.Timestamp.AsOfSystemTime(),
		r.MetaType, meta,
		len(r.DeprecatedSpans), encodedSpans)
	if err != nil {
		return errors.Wrapf(err, "failed to write record %v", r.ID)
	}
	ok, err := it.Next(ctx)
	if err != nil {
		return errors.Wrapf(err, "failed to write record %v", r.ID)
	}
	if !ok {
		return errors.Newf("failed to write record %v", r.ID)
	}
	row := it.Cur()
	if err := it.Close(); err != nil {
		log.Infof(ctx, "encountered %v when writing record %v", err, r.ID)
	}
	if failed := *row[0].(*tree.DBool); failed {
		curNumSpans := int64(*row[1].(*tree.DInt))
		if s.maxSpans > 0 && curNumSpans+int64(len(r.DeprecatedSpans)) > s.maxSpans {
			return errors.WithHint(
				errors.Errorf("protectedts: limit exceeded: %d+%d > %d spans", curNumSpans,
					len(r.DeprecatedSpans), s.maxSpans),
				"SET CLUSTER SETTING kv.protectedts.max_spans to a higher value")
		}
		curBytes := int64(*row[2].(*tree.DInt))
		recordBytes := int64(len(encodedSpans) + len(r.Meta) + len(r.MetaType))
		if s.maxBytes > 0 && curBytes+recordBytes > s.maxBytes {
			return errors.WithHint(
				errors.Errorf("protectedts: limit exceeded: %d+%d > %d bytes", curBytes, recordBytes,
					s.maxBytes),
				"SET CLUSTER SETTING kv.protectedts.max_bytes to a higher value")
		}
		return protectedts.ErrExists
	}
	return nil
}

func (p *storage) deprecatedGetRecord(ctx context.Context, id uuid.UUID) (*ptpb.Record, error) {
	row, err := p.txn.QueryRowEx(ctx, "protectedts-deprecated-GetRecord", p.txn.KV(),
		sessiondata.NodeUserSessionDataOverride,
		getRecordWithoutTargetQuery, id.GetBytesMut())
	if err != nil {
		return nil, errors.Wrapf(err, "failed to read record %v", id)
	}
	if len(row) == 0 {
		return nil, protectedts.ErrNotExists
	}
	var r ptpb.Record
	if err := rowToRecord(row, &r, true /* isDeprecatedRow */); err != nil {
		return nil, err
	}
	return &r, nil
}

func (p *storage) deprecatedGetRecords(ctx context.Context) ([]ptpb.Record, error) {
	it, err := p.txn.QueryIteratorEx(ctx, "protectedts-deprecated-GetRecords", p.txn.KV(),
		sessiondata.NodeUserSessionDataOverride,
		getRecordsWithoutTargetQuery)
	if err != nil {
		return nil, errors.Wrap(err, "failed to read records")
	}

	var ok bool
	var records []ptpb.Record
	for ok, err = it.Next(ctx); ok; ok, err = it.Next(ctx) {
		var record ptpb.Record
		if err := rowToRecord(it.Cur(), &record, true /* isDeprecatedRow */); err != nil {
			log.Errorf(ctx, "failed to parse row as record: %v", err)
		}
		records = append(records, record)
	}
	if err != nil {
		return nil, errors.Wrap(err, "failed to read records")
	}
	return records, nil
}

type settings struct {
	maxSpans int64
	maxBytes int64
}

func makeSettings(s *cluster.Settings) settings {
	return settings{
		maxSpans: protectedts.MaxSpans.Get(&s.SV),
		maxBytes: protectedts.MaxBytes.Get(&s.SV),
	}
}

var (
	errZeroTimestamp        = errors.New("invalid zero value timestamp")
	errZeroID               = errors.New("invalid zero value ID")
	errEmptySpans           = errors.Errorf("invalid empty set of spans")
	errNilTarget            = errors.Errorf("invalid nil target")
	errInvalidMeta          = errors.Errorf("invalid Meta with empty MetaType")
	errCreateVerifiedRecord = errors.Errorf("cannot create a verified record")
)

func validateRecordForProtect(
	ctx context.Context, r *ptpb.Record, st *cluster.Settings, knobs *protectedts.TestingKnobs,
) error {
	if r.Timestamp.IsEmpty() {
		return errZeroTimestamp
	}
	if r.ID.GetUUID() == uuid.Nil {
		return errZeroID
	}
	useDeprecatedPTSStorage := useDeprecatedProtectedTSStorage(ctx, st, knobs)
	if !useDeprecatedPTSStorage && r.Target == nil {
		return errNilTarget
	}
	if useDeprecatedPTSStorage && len(r.DeprecatedSpans) == 0 {
		return errEmptySpans
	}
	if len(r.Meta) > 0 && len(r.MetaType) == 0 {
		return errInvalidMeta
	}
	if r.Verified {
		return errCreateVerifiedRecord
	}
	return nil
}
