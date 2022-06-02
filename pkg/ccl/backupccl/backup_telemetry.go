package backupccl

import (
	"context"
	"net/url"
	"sort"
	"strings"
	"time"

	"github.com/cockroachdb/cockroach/pkg/cloud"
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/log/eventpb"
	"github.com/cockroachdb/errors"
)

type targetScope int

const (
	unknownScope targetScope = iota
	tableScope
	schemaScope
	databaseScope
	clusterScope
)

type resultCode string

func (s targetScope) String() string {
	switch s {
	case clusterScope:
		return "cluster"
	case databaseScope:
		return "database"
	case schemaScope:
		return "schema"
	case tableScope:
		return "table"
	default:
		return "unknown"
	}
}

const (
	backupEventType          = "backup"
	scheduledBackupEventType = "scheduled_backup"
	createdScheduleEventType = "create_schedule"
	restoreEventType         = "restore"

	backupJobEventType          = "backup_job"
	scheduledBackupJobEventType = "scheduled_backup_job"
	restoreJobEventType         = "restore_job"

	latestSubdirType   = "latest"
	standardSubdirType = "standard"
	customSubdirType   = "custom"

	nowAsOf = "now"
)

func logBackupTelemetry(
	ctx context.Context,
	backupManifest BackupManifest,
	initialDetails jobspb.BackupDetails,
	backupDetails jobspb.BackupDetails,
	jobID jobspb.JobID,
) {
	var event eventpb.EventPayload

	recoveryType := backupEventType
	if backupDetails.ScheduleID != 0 {
		recoveryType = scheduledBackupEventType
	}

	largestScope := getLargestScope(backupManifest.DescriptorCoverage, backupDetails.RequestedTargets)

	targetCount := 1
	if backupManifest.DescriptorCoverage != tree.AllDescriptors {
		targetCount = len(backupDetails.RequestedTargets)
	}

	multiRegion := false
	for i := range backupDetails.ResolvedTargets {
		_, db, _, _ := descpb.FromDescriptor(&backupDetails.ResolvedTargets[i])
		if db != nil {
			if db.RegionConfig != nil {
				multiRegion = true
			}
		}
	}

	timeBaseSubdir := true
	var subdirType string
	if _, err := time.Parse(DateBasedIntoFolderName,
		initialDetails.Destination.Subdir); err != nil {
		timeBaseSubdir = false
	}

	if strings.EqualFold(initialDetails.Destination.Subdir, latestFileName) {
		subdirType = latestSubdirType
	} else if !timeBaseSubdir {
		subdirType = customSubdirType
	} else {
		subdirType = standardSubdirType
	}

	authTypes := make(map[string]struct{})
	storageTypes := make(map[string]struct{})
	if backupDetails.CollectionURI != "" {
		if storageType, authType, err := parseStorageAndAuth(backupDetails.CollectionURI); err != nil {
			log.Warningf(ctx, "failed to parse backup collection URI: %v", err)
		} else {
			storageTypes[storageType] = struct{}{}
			authTypes[authType] = struct{}{}
		}
	}

	for _, uri := range backupDetails.URIsByLocalityKV {
		if storageType, authType, err := parseStorageAndAuth(uri); err != nil {
			log.Warningf(ctx, "failed to parse locality URI: %v", err)
		} else {
			storageTypes[storageType] = struct{}{}
			authTypes[authType] = struct{}{}
		}
	}

	var isLocalityAware bool
	if len(backupDetails.URIsByLocalityKV) > 0 {
		isLocalityAware = true
	}
	revisionHistory := backupManifest.MVCCFilter == MVCCFilter_All

	var kms string
	var kmsCount int
	var passphrase bool
	if backupDetails.EncryptionOptions != nil {
		switch backupDetails.EncryptionOptions.Mode {
		case jobspb.EncryptionMode_Passphrase:
			passphrase = true
		case jobspb.EncryptionMode_KMS:
			if backupDetails.EncryptionOptions.KMSInfo != nil {
				parsedKMSURI, err := url.ParseRequestURI(backupDetails.EncryptionOptions.KMSInfo.Uri)
				if err != nil {
					log.Warningf(ctx, "failed to parse KMS URI %s: %v", backupDetails.EncryptionOptions.KMSInfo.Uri, err)
				} else {
					kms = parsedKMSURI.Scheme
				}
				kmsCount = len(backupDetails.EncryptionOptions.RawKmsUris)
			}
		}
	}

	asOf := backupDetails.AsOf
	if asOf == "" {
		asOf = nowAsOf
	}

	event = &eventpb.RecoveryEvent{
		RecoveryType:            recoveryType,
		TargetScope:             largestScope.String(),
		IsMultiregionTarget:     multiRegion,
		TargetCount:             uint32(targetCount),
		DestinationSubdirType:   subdirType,
		IsLocalityAware:         isLocalityAware,
		WithRevisionHistory:     revisionHistory,
		HasEncryptionPassphrase: passphrase,
		KmsType:                 kms,
		KmsCount:                uint32(kmsCount),
		JobID:                   uint64(jobID),
		ScheduleID:              uint64(backupDetails.ScheduleID),
		AsOf:                    asOf,
		IsDetached:              backupDetails.Detached,
	}

	recoveryEvent := event.(*eventpb.RecoveryEvent)
	for typ := range authTypes {
		recoveryEvent.AuthTypes = append(recoveryEvent.AuthTypes, typ)
	}
	sort.Strings(recoveryEvent.AuthTypes)
	for typ := range storageTypes {
		recoveryEvent.DestinationStorageTypes = append(recoveryEvent.DestinationStorageTypes, typ)
	}
	sort.Strings(recoveryEvent.DestinationStorageTypes)

	log.StructuredEvent(ctx, event)
}

func getLargestScope(
	coverage tree.DescriptorCoverage, requestedDescriptors []descpb.Descriptor,
) targetScope {
	largestScope := unknownScope

	if coverage == tree.AllDescriptors {
		largestScope = clusterScope
	} else {
		// Log the largest scope from the targets.
		for i := range requestedDescriptors {
			var scope targetScope
			tbl, db, _, sc := descpb.FromDescriptor(&requestedDescriptors[i])
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
	}
	return largestScope
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

func loggedWaitBehavior(b jobspb.ScheduleDetails_WaitBehavior) string {
	switch b {
	case jobspb.ScheduleDetails_NO_WAIT:
		return "no-wait"
	case jobspb.ScheduleDetails_SKIP:
		return "skip"
	default:
		return "wait"
	}
}

func loggedErrorHandlingBehavior(b jobspb.ScheduleDetails_ErrorHandlingBehavior) string {
	switch b {
	case jobspb.ScheduleDetails_RETRY_SOON:
		return "retry-soon"
	case jobspb.ScheduleDetails_PAUSE_SCHED:
		return "pause-schedule"
	default:
		return "retry-schedule"
	}
}

func loggedSubdirType(subdir string) string {
	timeBaseSubdir := true
	var subdirType string
	if _, err := time.Parse(DateBasedIntoFolderName,
		subdir); err != nil {
		timeBaseSubdir = false
	}

	if strings.EqualFold(subdir, latestFileName) {
		subdirType = latestSubdirType
	} else if !timeBaseSubdir {
		subdirType = customSubdirType
	} else {
		subdirType = standardSubdirType
	}

	return subdirType
}

func logCreateScheduleTelemetry(
	ctx context.Context,
	incRecurrence *scheduleRecurrence,
	fullRecurrence *scheduleRecurrence,
	incScheduledJob *jobs.ScheduledJob,
	fullScheduledJob *jobs.ScheduledJob,
	firstRun *time.Time,
	ignoreExisting bool,
	details jobspb.ScheduleDetails,
) {
	var event eventpb.EventPayload

	var firstRunStr string
	if firstRun != nil {
		firstRunStr = firstRun.String()
	}

	// TODO: add rest of the actual backup logging here.

	var incScheduleID uint64
	if incScheduledJob != nil {
		incScheduleID = uint64(incScheduledJob.ScheduleID())
	}
	var fullScheduleID uint64
	if fullScheduledJob != nil {
		fullScheduleID = uint64(fullScheduledJob.ScheduleID())
	}

	event = &eventpb.RecoveryEvent{
		RecoveryType:          createdScheduleEventType,
		RecurringCron:         incRecurrence.cron,
		FullBackupCron:        fullRecurrence.cron,
		CustomFirstRunTime:    firstRunStr,
		OnPreviousRunning:     loggedWaitBehavior(details.Wait),
		OnExecutionFailure:    loggedErrorHandlingBehavior(details.OnError),
		IgnoreExistingBackup:  ignoreExisting,
		IncrementalScheduleID: incScheduleID,
		FullScheduleID:        fullScheduleID,
	}

	log.StructuredEvent(ctx, event)
}

func logRestoreTelemetry(
	ctx context.Context,
	jobID jobspb.JobID,
	details jobspb.RestoreDetails,
	kms []string,
	intoDB string,
	newDBName string,
	subdir string,
	asOf tree.AsOfClause,
	opts tree.RestoreOptions,
	descsByTablePattern map[tree.TablePattern]catalog.Descriptor,
	restoreDBs []catalog.DatabaseDescriptor,
	debugPauseOn string,
) {
	var event eventpb.EventPayload
	var requestedTargets []descpb.Descriptor
	for _, desc := range descsByTablePattern {
		requestedTargets = append(requestedTargets, *desc.DescriptorProto())
	}
	for _, desc := range restoreDBs {
		requestedTargets = append(requestedTargets, *desc.DescriptorProto())
	}

	largestScope := getLargestScope(details.DescriptorCoverage, requestedTargets)

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

	asOfStr := nowAsOf
	if asOf.Expr != nil {
		asOfStr = asOf.Expr.String()
	}

	var kmsType string
	var kmsCount int
	var passphrase bool
	if details.Encryption != nil {
		switch details.Encryption.Mode {
		case jobspb.EncryptionMode_Passphrase:
			passphrase = true
		case jobspb.EncryptionMode_KMS:
			if details.Encryption.KMSInfo != nil {
				parsedKMSURI, err := url.ParseRequestURI(details.Encryption.KMSInfo.Uri)
				if err != nil {
					log.Warningf(ctx, "failed to parse KMS URI %s: %v", details.Encryption.KMSInfo.Uri, err)
				} else {
					kmsType = parsedKMSURI.Scheme
				}
				kmsCount = len(kms)
			}
		}
	}

	event = &eventpb.RecoveryEvent{
		RecoveryType:            restoreEventType,
		TargetScope:             largestScope.String(),
		TargetCount:             uint32(targetCount),
		IsMultiregionTarget:     multiRegion,
		DestinationSubdirType:   loggedSubdirType(subdir),
		IsLocalityAware:         localityAware,
		AsOf:                    asOfStr,
		HasEncryptionPassphrase: passphrase,
		KmsType:                 kmsType,
		KmsCount:                uint32(kmsCount),
		IsDetached:              opts.Detached,
		IntoDB:                  intoDB,
		RenameDB:                newDBName,
		SkipMissingFK:           opts.SkipMissingFKs,
		SkipMissingSequences:    opts.SkipMissingSequences,
		SkipMissingViews:        opts.SkipMissingViews,
		SkipLocalitiesCheck:     opts.SkipLocalitiesCheck,
		DebugPauseOn:            debugPauseOn,
		JobID:                   uint64(jobID),
	}

	recoveryEvent := event.(*eventpb.RecoveryEvent)
	for typ := range authTypes {
		recoveryEvent.AuthTypes = append(recoveryEvent.AuthTypes, typ)
	}
	sort.Strings(recoveryEvent.AuthTypes)
	for typ := range storageTypes {
		recoveryEvent.DestinationStorageTypes = append(recoveryEvent.DestinationStorageTypes, typ)
	}
	sort.Strings(recoveryEvent.DestinationStorageTypes)

	log.StructuredEvent(ctx, event)
}

func logJobCompletion(
	ctx context.Context, eventType string, jobID jobspb.JobID, status jobs.Status, jobErr error,
) {
	var errorText string
	if jobErr != nil {
		errorText = jobErr.Error()
	}
	event := &eventpb.RecoveryEvent{
		RecoveryType: eventType,
		JobID:        uint64(jobID),
		ResultStatus: string(status),
		ErrorText:    errorText,
	}

	log.StructuredEvent(ctx, event)
}
