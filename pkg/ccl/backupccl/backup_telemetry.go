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
	"net/url"
	"sort"
	"strings"
	"time"

	"github.com/cockroachdb/cockroach/pkg/ccl/backupccl/backupbase"
	"github.com/cockroachdb/cockroach/pkg/ccl/backupccl/backupdest"
	"github.com/cockroachdb/cockroach/pkg/cloud"
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/scheduledjobs/schedulebase"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/log/eventpb"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/redact"
)

type targetScope int

//go:generate stringer -type=targetScope -linecomment

const (
	unknownScope  targetScope = iota // unknown
	tableScope                       // table
	schemaScope                      // schema
	databaseScope                    // database
	clusterScope                     // cluster
)

const (
	backupEventType             eventpb.RecoveryEventType = "backup"
	scheduledBackupEventType    eventpb.RecoveryEventType = "scheduled_backup"
	createdScheduleEventType    eventpb.RecoveryEventType = "create_schedule"
	restoreEventType            eventpb.RecoveryEventType = "restore"
	backupJobEventType          eventpb.RecoveryEventType = "backup_job"
	scheduledBackupJobEventType eventpb.RecoveryEventType = "scheduled_backup_job"
	restoreJobEventType         eventpb.RecoveryEventType = "restore_job"

	latestSubdirType   = "latest"
	standardSubdirType = "standard"
	customSubdirType   = "custom"

	// Currently the telemetry event payload only contains keys of options. Future
	// changes to telemetry should refrain from adding values to the payload
	// unless they are properly redacted.
	telemetryOptionDetached                  = "detached"
	telemetryOptionIntoDB                    = "into_db"
	telemetryOptionRenameDB                  = "rename_db"
	telemetryOptionSkipMissingFK             = "skip_missing_foreign_keys"
	telemetryOptionSkipMissingSequences      = "skip_missing_sequences"
	telemetryOptionSkipMissingSequenceOwners = "skip_missing_sequence_owners"
	telemetryOptionSkipMissingViews          = "skip_missing_views"
	telemetryOptionSkipLocalitiesCheck       = "skip_localities_check"
	telemetryOptionSchemaOnly                = "schema_only"
)

// logBackupTelemetry publishes an eventpb.RecoveryEvent about a manually
// invoked backup or a scheduled backup.
func logBackupTelemetry(
	ctx context.Context, initialDetails jobspb.BackupDetails, jobID jobspb.JobID,
) {
	event := createBackupRecoveryEvent(ctx, initialDetails, jobID)
	log.StructuredEvent(ctx, &event)
}

func createBackupRecoveryEvent(
	ctx context.Context, initialDetails jobspb.BackupDetails, jobID jobspb.JobID,
) eventpb.RecoveryEvent {
	recoveryType := backupEventType
	if initialDetails.ScheduleID != 0 {
		recoveryType = scheduledBackupEventType
	}

	largestScope := getLargestScope(initialDetails.FullCluster, initialDetails.RequestedTargets)

	targetCount := 1
	if !initialDetails.FullCluster {
		targetCount = len(initialDetails.RequestedTargets)
	}

	multiRegion := false
	for i := range initialDetails.ResolvedTargets {
		_, db, _, _, _ := descpb.GetDescriptors(&initialDetails.ResolvedTargets[i])
		if db != nil {
			if db.RegionConfig != nil {
				multiRegion = true
			}
		}
	}

	timeBaseSubdir := true
	var subdirType string
	if _, err := time.Parse(backupbase.DateBasedIntoFolderName,
		initialDetails.Destination.Subdir); err != nil {
		timeBaseSubdir = false
	}

	if strings.EqualFold(initialDetails.Destination.Subdir, backupbase.LatestFileName) {
		subdirType = latestSubdirType
	} else if !timeBaseSubdir {
		subdirType = customSubdirType
	} else {
		subdirType = standardSubdirType
	}

	authTypes := make(map[string]struct{})
	storageTypes := make(map[string]struct{})
	defaultURI, urisByLocalityKV, err := backupdest.GetURIsByLocalityKV(initialDetails.Destination.To, "")
	if err != nil {
		log.Warningf(ctx, "failed to get URIs by locality: %v", err)
	}

	if defaultURI != "" {
		if storageType, authType, err := parseStorageAndAuth(defaultURI); err != nil {
			log.Warningf(ctx, "failed to parse backup default URI: %v", err)
		} else {
			storageTypes[storageType] = struct{}{}
			authTypes[authType] = struct{}{}
		}
	}

	for _, uri := range urisByLocalityKV {
		if storageType, authType, err := parseStorageAndAuth(uri); err != nil {
			log.Warningf(ctx, "failed to parse locality URI: %v", err)
		} else {
			storageTypes[storageType] = struct{}{}
			authTypes[authType] = struct{}{}
		}
	}

	var isLocalityAware bool
	if len(urisByLocalityKV) > 0 {
		isLocalityAware = true
	}
	passphrase, kms, kmsCount := getPassphraseAndKMS(ctx, initialDetails.EncryptionOptions)

	var options []string
	if initialDetails.Detached {
		options = append(options, telemetryOptionDetached)
	}

	event := eventpb.RecoveryEvent{
		RecoveryType:            recoveryType,
		TargetScope:             largestScope.String(),
		IsMultiregionTarget:     multiRegion,
		TargetCount:             uint32(targetCount),
		DestinationSubdirType:   subdirType,
		IsLocalityAware:         isLocalityAware,
		WithRevisionHistory:     initialDetails.RevisionHistory,
		HasEncryptionPassphrase: passphrase,
		KMSType:                 kms,
		KMSCount:                uint32(kmsCount),
		JobID:                   uint64(jobID),
		AsOfInterval:            initialDetails.AsOfInterval,
		Options:                 options,
		ApplicationName:         initialDetails.ApplicationName,
	}

	event.DestinationAuthTypes = make([]string, 0, len(authTypes))
	for typ := range authTypes {
		event.DestinationAuthTypes = append(event.DestinationAuthTypes, typ)
	}
	sort.Strings(event.DestinationAuthTypes)

	event.DestinationStorageTypes = make([]string, 0, len(storageTypes))
	for typ := range storageTypes {
		event.DestinationStorageTypes = append(event.DestinationStorageTypes, typ)
	}
	sort.Strings(event.DestinationStorageTypes)
	return event
}

func getLargestScope(fullCluster bool, requestedDescriptors []descpb.Descriptor) targetScope {
	if fullCluster {
		return clusterScope
	}

	largestScope := unknownScope
	// Log the largest scope from the targets.
	for i := range requestedDescriptors {
		var scope targetScope
		tbl, db, _, sc, _ := descpb.GetDescriptors(&requestedDescriptors[i])
		if tbl != nil {
			scope = tableScope
		} else if sc != nil {
			scope = schemaScope
		} else if db != nil {
			scope = databaseScope
		}

		if scope > largestScope {
			largestScope = scope
		}
	}
	return largestScope
}

func getPassphraseAndKMS(
	ctx context.Context, enc *jobspb.BackupEncryptionOptions,
) (passphrase bool, kms string, kmsCount int) {
	if enc != nil {
		switch enc.Mode {
		case jobspb.EncryptionMode_Passphrase:
			passphrase = true
		case jobspb.EncryptionMode_KMS:
			if enc.KMSInfo != nil {
				parsedKMSURI, err := url.ParseRequestURI(enc.KMSInfo.Uri)
				if err != nil {
					log.Warningf(ctx, "failed to parse KMS URI %s: %v", enc.KMSInfo.Uri, err)
				} else {
					kms = parsedKMSURI.Scheme
				}
				kmsCount = len(enc.RawKmsUris)
			}
		}
	}

	return passphrase, kms, kmsCount
}

func parseStorageAndAuth(uri string) (string, string, error) {
	var storageType string
	authType := cloud.AuthParamSpecified
	parsedURI, err := url.Parse(uri)
	if err != nil {
		return "", "", errors.Wrapf(err, "failed to parse URI %s for telemetry", uri)
	}
	storageType = parsedURI.Scheme
	auth := parsedURI.Query().Get(cloud.AuthParam)
	if auth != "" {
		authType = auth
	}
	return storageType, authType, nil
}

func loggedSubdirType(subdir string) string {
	timeBaseSubdir := true
	var subdirType string
	if _, err := time.Parse(backupbase.DateBasedIntoFolderName,
		subdir); err != nil {
		timeBaseSubdir = false
	}

	if strings.EqualFold(subdir, backupbase.LatestFileName) {
		subdirType = latestSubdirType
	} else if !timeBaseSubdir {
		subdirType = customSubdirType
	} else {
		subdirType = standardSubdirType
	}

	return subdirType
}

// logCreateScheduleTelemetry publishes an eventpb.RecoveryEvent about a created
// backup schedule.
func logCreateScheduleTelemetry(
	ctx context.Context,
	incRecurrence *schedulebase.ScheduleRecurrence,
	fullRecurrence *schedulebase.ScheduleRecurrence,
	firstRun *time.Time,
	ignoreExisting bool,
	details jobspb.ScheduleDetails,
	backupEvent eventpb.RecoveryEvent,
) {
	var firstRunNanos int64
	if firstRun != nil {
		firstRunNanos = firstRun.UnixNano()
	}

	var recurringCron string
	if incRecurrence != nil {
		recurringCron = incRecurrence.Cron
	}

	var fullCron string
	if fullRecurrence != nil {
		fullCron = fullRecurrence.Cron
	}

	// For events about backup schedule creation, simply append the schedule-only
	// fields to the event generated by the dry-run backup.
	backupEvent.RecoveryType = createdScheduleEventType
	backupEvent.RecurringCron = recurringCron
	backupEvent.FullBackupCron = fullCron
	backupEvent.CustomFirstRunTime = firstRunNanos
	backupEvent.OnPreviousRunning = jobspb.ScheduleDetails_WaitBehavior_name[int32(details.Wait)]
	backupEvent.OnExecutionFailure = jobspb.ScheduleDetails_ErrorHandlingBehavior_name[int32(details.OnError)]
	backupEvent.IgnoreExistingBackup = ignoreExisting

	log.StructuredEvent(ctx, &backupEvent)
}

// logRestoreTelemetry publishes an eventpb.RecoveryEvent about a restore
// invocation.
func logRestoreTelemetry(
	ctx context.Context,
	jobID jobspb.JobID,
	details jobspb.RestoreDetails,
	intoDB string,
	newDBName string,
	subdir string,
	asOfInterval int64,
	opts tree.RestoreOptions,
	descsByTablePattern map[tree.TablePattern]catalog.Descriptor,
	restoreDBs []catalog.DatabaseDescriptor,
	debugPauseOn string,
	applicationName string,
) {
	var requestedTargets []descpb.Descriptor
	for _, desc := range descsByTablePattern {
		requestedTargets = append(requestedTargets, *desc.DescriptorProto())
	}
	for _, desc := range restoreDBs {
		requestedTargets = append(requestedTargets, *desc.DescriptorProto())
	}

	largestScope := getLargestScope(details.DescriptorCoverage == tree.AllDescriptors, requestedTargets)

	targetCount := 1
	if details.DescriptorCoverage != tree.AllDescriptors {
		targetCount = len(requestedTargets)
	}

	multiRegion := false
	for _, db := range details.DatabaseDescs {
		if db != nil {
			if db.RegionConfig != nil {
				multiRegion = true
			}
		}
	}

	authTypes := make(map[string]struct{})
	storageTypes := make(map[string]struct{})
	localityAware := false

	for _, uri := range details.URIs {
		if storage, auth, err := parseStorageAndAuth(uri); err != nil {
			log.Warningf(ctx, "failed to parse URI: %v", err)
		} else {
			authTypes[auth] = struct{}{}
			storageTypes[storage] = struct{}{}
		}
	}

	for _, localityInfo := range details.BackupLocalityInfo {
		if len(localityInfo.URIsByOriginalLocalityKV) > 0 {
			localityAware = true
		}

		for _, uri := range localityInfo.URIsByOriginalLocalityKV {
			if storage, auth, err := parseStorageAndAuth(uri); err != nil {
				log.Warningf(ctx, "failed to parse URI: %v", err)
			} else {
				authTypes[auth] = struct{}{}
				storageTypes[storage] = struct{}{}
			}
		}
	}

	passphrase, kmsType, kmsCount := getPassphraseAndKMS(ctx, details.Encryption)

	var options []string
	if intoDB != "" {
		options = append(options, telemetryOptionIntoDB)
	}
	if newDBName != "" {
		options = append(options, telemetryOptionRenameDB)
	}
	if opts.SkipMissingFKs {
		options = append(options, telemetryOptionSkipMissingFK)
	}
	if opts.SkipMissingViews {
		options = append(options, telemetryOptionSkipMissingViews)
	}
	if opts.SkipLocalitiesCheck {
		options = append(options, telemetryOptionSkipLocalitiesCheck)
	}
	if opts.SkipMissingSequences {
		options = append(options, telemetryOptionSkipMissingSequences)
	}
	if opts.SkipMissingSequenceOwners {
		options = append(options, telemetryOptionSkipMissingSequenceOwners)
	}
	if opts.Detached {
		options = append(options, telemetryOptionDetached)
	}
	if opts.SchemaOnly {
		options = append(options, telemetryOptionSchemaOnly)
	}
	sort.Strings(options)

	event := &eventpb.RecoveryEvent{
		RecoveryType:            restoreEventType,
		TargetScope:             largestScope.String(),
		TargetCount:             uint32(targetCount),
		IsMultiregionTarget:     multiRegion,
		DestinationSubdirType:   loggedSubdirType(subdir),
		IsLocalityAware:         localityAware,
		AsOfInterval:            asOfInterval,
		HasEncryptionPassphrase: passphrase,
		KMSType:                 kmsType,
		KMSCount:                uint32(kmsCount),
		DebugPauseOn:            debugPauseOn,
		JobID:                   uint64(jobID),
		Options:                 options,
		ApplicationName:         applicationName,
	}

	event.DestinationAuthTypes = make([]string, 0, len(authTypes))
	for typ := range authTypes {
		event.DestinationAuthTypes = append(event.DestinationAuthTypes, typ)
	}
	sort.Strings(event.DestinationAuthTypes)

	event.DestinationStorageTypes = make([]string, 0, len(storageTypes))
	for typ := range storageTypes {
		event.DestinationStorageTypes = append(event.DestinationStorageTypes, typ)
	}
	sort.Strings(event.DestinationStorageTypes)

	log.StructuredEvent(ctx, event)
}

// logJobCompletion publishes an eventpb.RecoveryEvent about a successful or
// failed backup or restore job.
func logJobCompletion(
	ctx context.Context,
	eventType eventpb.RecoveryEventType,
	jobID jobspb.JobID,
	success bool,
	jobErr error,
) {
	var redactedErr redact.RedactableString
	if jobErr != nil {
		redactedErr = redact.Sprint(jobErr)
	}
	status := jobs.StatusSucceeded
	if !success {
		if jobs.HasErrJobCanceled(jobErr) {
			status = jobs.StatusCanceled
		} else {
			status = jobs.StatusFailed
		}
	}

	event := &eventpb.RecoveryEvent{
		RecoveryType: eventType,
		JobID:        uint64(jobID),
		ResultStatus: string(status),
		ErrorText:    redactedErr,
	}

	log.StructuredEvent(ctx, event)
}
