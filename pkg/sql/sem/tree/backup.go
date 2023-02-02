// Copyright 2016 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package tree

import (
	"github.com/cockroachdb/errors"
	"github.com/google/go-cmp/cmp"
)

// DescriptorCoverage specifies the subset of descriptors that are requested during a backup
// or a restore.
type DescriptorCoverage int32

const (
	// RequestedDescriptors table coverage means that the backup is not
	// guaranteed to have all of the cluster data. This can be accomplished by
	// backing up a specific subset of tables/databases. Note that even if all
	// of the tables and databases have been included in the backup manually, a
	// backup is not said to have complete table coverage unless it was created
	// by a `BACKUP TO` command.
	RequestedDescriptors DescriptorCoverage = iota

	// AllDescriptors table coverage means that backup is guaranteed to have all the
	// relevant data in the cluster. These can only be created by running a
	// full cluster backup with `BACKUP TO`.
	AllDescriptors

	// SystemUsers coverage indicates that only the system.users
	// table will be restored from the backup.
	SystemUsers
)

// BackupOptions describes options for the BACKUP execution.
type BackupOptions struct {
	CaptureRevisionHistory Expr
	EncryptionPassphrase   Expr
	Detached               *DBool
	EncryptionKMSURI       StringOrPlaceholderOptList
	IncrementalStorage     StringOrPlaceholderOptList
}

var _ NodeFormatter = &BackupOptions{}

// Backup represents a BACKUP statement.
type Backup struct {
	Targets *BackupTargetList

	// To is set to the root directory of the backup (called the <destination> in
	// the docs).
	To StringOrPlaceholderOptList

	// IncrementalFrom is only set for the old 'BACKUP .... TO ...' syntax.
	IncrementalFrom Exprs

	AsOf    AsOfClause
	Options BackupOptions

	// Nested is set to true when the user creates a backup with
	//`BACKUP ... INTO... ` syntax.
	Nested bool

	// AppendToLatest is set to true if the user creates a backup with
	//`BACKUP...INTO LATEST...`
	AppendToLatest bool

	// Subdir may be set by the parser when the SQL query is of the form `BACKUP
	// INTO 'subdir' IN...`. Alternatively, if Nested is true but a subdir was not
	// explicitly specified by the user, then this will be set during BACKUP
	// planning once the destination has been resolved.
	Subdir Expr
}

var _ Statement = &Backup{}

// Format implements the NodeFormatter interface.
func (node *Backup) Format(ctx *FmtCtx) {
	ctx.WriteString("BACKUP ")
	if node.Targets != nil {
		ctx.FormatNode(node.Targets)
		ctx.WriteString(" ")
	}
	if node.Nested {
		ctx.WriteString("INTO ")
		if node.Subdir != nil {
			ctx.FormatNode(node.Subdir)
			ctx.WriteString(" IN ")
		} else if node.AppendToLatest {
			ctx.WriteString("LATEST IN ")
		}
	} else {
		ctx.WriteString("TO ")
	}
	ctx.FormatNode(&node.To)
	if node.AsOf.Expr != nil {
		ctx.WriteString(" ")
		ctx.FormatNode(&node.AsOf)
	}
	if node.IncrementalFrom != nil {
		ctx.WriteString(" INCREMENTAL FROM ")
		ctx.FormatNode(&node.IncrementalFrom)
	}

	if !node.Options.IsDefault() {
		ctx.WriteString(" WITH ")
		ctx.FormatNode(&node.Options)
	}
}

// Coverage return the coverage (all vs requested).
func (node Backup) Coverage() DescriptorCoverage {
	if node.Targets == nil {
		return AllDescriptors
	}
	return RequestedDescriptors
}

// RestoreOptions describes options for the RESTORE execution.
type RestoreOptions struct {
	EncryptionPassphrase      Expr
	DecryptionKMSURI          StringOrPlaceholderOptList
	IntoDB                    Expr
	SkipMissingFKs            bool
	SkipMissingSequences      bool
	SkipMissingSequenceOwners bool
	SkipMissingViews          bool
	Detached                  bool
	SkipLocalitiesCheck       bool
	DebugPauseOn              Expr
	NewDBName                 Expr
	IncrementalStorage        StringOrPlaceholderOptList
	AsTenant                  Expr
	ForceTenantID             Expr
	SchemaOnly                bool
	VerifyData                bool
}

var _ NodeFormatter = &RestoreOptions{}

// Restore represents a RESTORE statement.
type Restore struct {
	Targets            BackupTargetList
	DescriptorCoverage DescriptorCoverage

	// From contains the URIs for the backup(s) we seek to restore.
	//   - len(From)>1 implies the user explicitly passed incremental backup paths,
	//     which is only allowed using the old syntax, `RESTORE <targets> FROM <destination>.
	//     In this case, From[0] contains the URI(s) for the full backup.
	//   - len(From)==1 implies we'll have to look for incremental backups in planning
	//   - len(From[0]) > 1 implies the backups are locality aware
	//   - From[i][0] must be the default locality.
	From    []StringOrPlaceholderOptList
	AsOf    AsOfClause
	Options RestoreOptions

	// Subdir may be set by the parser when the SQL query is of the form `RESTORE
	// ... FROM 'from' IN 'subdir'...`. Alternatively, restore_planning.go will set
	// it for the query `RESTORE ... FROM 'from' IN LATEST...`
	Subdir Expr
}

var _ Statement = &Restore{}

// Format implements the NodeFormatter interface.
func (node *Restore) Format(ctx *FmtCtx) {
	ctx.WriteString("RESTORE ")
	if node.DescriptorCoverage == RequestedDescriptors {
		ctx.FormatNode(&node.Targets)
		ctx.WriteString(" ")
	}
	ctx.WriteString("FROM ")
	if node.Subdir != nil {
		ctx.FormatNode(node.Subdir)
		ctx.WriteString(" IN ")
	}
	for i := range node.From {
		if i > 0 {
			ctx.WriteString(", ")
		}
		ctx.FormatNode(&node.From[i])
	}
	if node.AsOf.Expr != nil {
		ctx.WriteString(" ")
		ctx.FormatNode(&node.AsOf)
	}
	if !node.Options.IsDefault() {
		ctx.WriteString(" WITH ")
		ctx.FormatNode(&node.Options)
	}
}

// KVOption is a key-value option.
type KVOption struct {
	Key   Name
	Value Expr
}

// KVOptions is a list of KVOptions.
type KVOptions []KVOption

// HasKey searches the set of options to discover if it has key.
func (o *KVOptions) HasKey(key Name) bool {
	for _, kv := range *o {
		if kv.Key == key {
			return true
		}
	}
	return false
}

// Format implements the NodeFormatter interface.
func (o *KVOptions) Format(ctx *FmtCtx) {
	for i := range *o {
		n := &(*o)[i]
		if i > 0 {
			ctx.WriteString(", ")
		}
		// KVOption Key values never contain PII and should be distinguished
		// for feature tracking purposes.
		ctx.WithFlags(ctx.flags&^FmtMarkRedactionNode, func() {
			ctx.FormatNode(&n.Key)
		})
		if n.Value != nil {
			ctx.WriteString(` = `)
			ctx.FormatNode(n.Value)
		}
	}
}

// StringOrPlaceholderOptList is a list of strings or placeholders.
type StringOrPlaceholderOptList []Expr

// Format implements the NodeFormatter interface.
func (node *StringOrPlaceholderOptList) Format(ctx *FmtCtx) {
	if len(*node) > 1 {
		ctx.WriteString("(")
	}
	ctx.FormatNode((*Exprs)(node))
	if len(*node) > 1 {
		ctx.WriteString(")")
	}
}

// Format implements the NodeFormatter interface
func (o *BackupOptions) Format(ctx *FmtCtx) {
	var addSep bool
	maybeAddSep := func() {
		if addSep {
			ctx.WriteString(", ")
		}
		addSep = true
	}
	if o.CaptureRevisionHistory != nil {
		ctx.WriteString("revision_history = ")
		ctx.FormatNode(o.CaptureRevisionHistory)
		addSep = true
	}

	if o.EncryptionPassphrase != nil {
		maybeAddSep()
		ctx.WriteString("encryption_passphrase = ")
		if ctx.flags.HasFlags(FmtShowPasswords) {
			ctx.FormatNode(o.EncryptionPassphrase)
		} else {
			ctx.WriteString(PasswordSubstitution)
		}
	}

	if o.Detached != nil {
		maybeAddSep()
		ctx.WriteString("detached")
		if o.Detached != DBoolTrue {
			ctx.WriteString(" = FALSE")
		}
	}

	if o.EncryptionKMSURI != nil {
		maybeAddSep()
		ctx.WriteString("kms = ")
		ctx.FormatNode(&o.EncryptionKMSURI)
	}

	if o.IncrementalStorage != nil {
		maybeAddSep()
		ctx.WriteString("incremental_location = ")
		ctx.FormatNode(&o.IncrementalStorage)
	}
}

// CombineWith merges other backup options into this backup options struct.
// An error is returned if the same option merged multiple times.
func (o *BackupOptions) CombineWith(other *BackupOptions) error {
	if o.CaptureRevisionHistory != nil {
		if other.CaptureRevisionHistory != nil {
			return errors.New("revision_history option specified multiple times")
		}
	} else {
		o.CaptureRevisionHistory = other.CaptureRevisionHistory
	}

	if o.EncryptionPassphrase == nil {
		o.EncryptionPassphrase = other.EncryptionPassphrase
	} else if other.EncryptionPassphrase != nil {
		return errors.New("encryption_passphrase specified multiple times")
	}

	if o.Detached != nil {
		if other.Detached != nil {
			return errors.New("detached option specified multiple times")
		}
	} else {
		o.Detached = other.Detached
	}

	if o.EncryptionKMSURI == nil {
		o.EncryptionKMSURI = other.EncryptionKMSURI
	} else if other.EncryptionKMSURI != nil {
		return errors.New("kms specified multiple times")
	}

	if o.IncrementalStorage == nil {
		o.IncrementalStorage = other.IncrementalStorage
	} else if other.IncrementalStorage != nil {
		return errors.New("incremental_location option specified multiple times")
	}

	return nil
}

// IsDefault returns true if this backup options struct has default value.
func (o BackupOptions) IsDefault() bool {
	options := BackupOptions{}
	return o.CaptureRevisionHistory == options.CaptureRevisionHistory &&
		o.Detached == options.Detached &&
		cmp.Equal(o.EncryptionKMSURI, options.EncryptionKMSURI) &&
		o.EncryptionPassphrase == options.EncryptionPassphrase &&
		cmp.Equal(o.IncrementalStorage, options.IncrementalStorage)
}

// Format implements the NodeFormatter interface.
func (o *RestoreOptions) Format(ctx *FmtCtx) {
	var addSep bool
	maybeAddSep := func() {
		if addSep {
			ctx.WriteString(", ")
		}
		addSep = true
	}
	if o.EncryptionPassphrase != nil {
		addSep = true
		ctx.WriteString("encryption_passphrase = ")
		if ctx.flags.HasFlags(FmtShowPasswords) {
			ctx.FormatNode(o.EncryptionPassphrase)
		} else {
			ctx.WriteString(PasswordSubstitution)
		}
	}

	if o.DecryptionKMSURI != nil {
		maybeAddSep()
		ctx.WriteString("kms = ")
		ctx.FormatNode(&o.DecryptionKMSURI)
	}

	if o.IntoDB != nil {
		maybeAddSep()
		ctx.WriteString("into_db = ")
		ctx.FormatNode(o.IntoDB)
	}

	if o.DebugPauseOn != nil {
		maybeAddSep()
		ctx.WriteString("debug_pause_on = ")
		ctx.FormatNode(o.DebugPauseOn)
	}

	if o.SkipMissingFKs {
		maybeAddSep()
		ctx.WriteString("skip_missing_foreign_keys")
	}

	if o.SkipMissingSequenceOwners {
		maybeAddSep()
		ctx.WriteString("skip_missing_sequence_owners")
	}

	if o.SkipMissingSequences {
		maybeAddSep()
		ctx.WriteString("skip_missing_sequences")
	}

	if o.SkipMissingViews {
		maybeAddSep()
		ctx.WriteString("skip_missing_views")
	}

	if o.Detached {
		maybeAddSep()
		ctx.WriteString("detached")
	}

	if o.SkipLocalitiesCheck {
		maybeAddSep()
		ctx.WriteString("skip_localities_check")
	}

	if o.NewDBName != nil {
		maybeAddSep()
		ctx.WriteString("new_db_name = ")
		ctx.FormatNode(o.NewDBName)
	}

	if o.IncrementalStorage != nil {
		maybeAddSep()
		ctx.WriteString("incremental_location = ")
		ctx.FormatNode(&o.IncrementalStorage)
	}

	if o.AsTenant != nil {
		maybeAddSep()
		ctx.WriteString("tenant_name = ")
		ctx.FormatNode(o.AsTenant)
	}

	if o.ForceTenantID != nil {
		maybeAddSep()
		ctx.WriteString("tenant = ")
		ctx.FormatNode(o.ForceTenantID)
	}

	if o.SchemaOnly {
		maybeAddSep()
		ctx.WriteString("schema_only")
	}
	if o.VerifyData {
		maybeAddSep()
		ctx.WriteString("verify_backup_table_data")
	}
}

// CombineWith merges other backup options into this backup options struct.
// An error is returned if the same option merged multiple times.
func (o *RestoreOptions) CombineWith(other *RestoreOptions) error {
	if o.EncryptionPassphrase == nil {
		o.EncryptionPassphrase = other.EncryptionPassphrase
	} else if other.EncryptionPassphrase != nil {
		return errors.New("encryption_passphrase specified multiple times")
	}

	if o.DecryptionKMSURI == nil {
		o.DecryptionKMSURI = other.DecryptionKMSURI
	} else if other.DecryptionKMSURI != nil {
		return errors.New("kms specified multiple times")
	}

	if o.IntoDB == nil {
		o.IntoDB = other.IntoDB
	} else if other.IntoDB != nil {
		return errors.New("into_db specified multiple times")
	}

	if o.SkipMissingFKs {
		if other.SkipMissingFKs {
			return errors.New("skip_missing_foreign_keys specified multiple times")
		}
	} else {
		o.SkipMissingFKs = other.SkipMissingFKs
	}

	if o.SkipMissingSequences {
		if other.SkipMissingSequences {
			return errors.New("skip_missing_sequences specified multiple times")
		}
	} else {
		o.SkipMissingSequences = other.SkipMissingSequences
	}

	if o.SkipMissingSequenceOwners {
		if other.SkipMissingSequenceOwners {
			return errors.New("skip_missing_sequence_owners specified multiple times")
		}
	} else {
		o.SkipMissingSequenceOwners = other.SkipMissingSequenceOwners
	}

	if o.SkipMissingViews {
		if other.SkipMissingViews {
			return errors.New("skip_missing_views specified multiple times")
		}
	} else {
		o.SkipMissingViews = other.SkipMissingViews
	}

	if o.Detached {
		if other.Detached {
			return errors.New("detached option specified multiple times")
		}
	} else {
		o.Detached = other.Detached
	}

	if o.SkipLocalitiesCheck {
		if other.SkipLocalitiesCheck {
			return errors.New("skip_localities_check specified multiple times")
		}
	} else {
		o.SkipLocalitiesCheck = other.SkipLocalitiesCheck
	}

	if o.DebugPauseOn == nil {
		o.DebugPauseOn = other.DebugPauseOn
	} else if other.DebugPauseOn != nil {
		return errors.New("debug_pause_on specified multiple times")
	}

	if o.NewDBName == nil {
		o.NewDBName = other.NewDBName
	} else if other.NewDBName != nil {
		return errors.New("new_db_name specified multiple times")
	}

	if o.IncrementalStorage == nil {
		o.IncrementalStorage = other.IncrementalStorage
	} else if other.IncrementalStorage != nil {
		return errors.New("incremental_location option specified multiple times")
	}

	if o.AsTenant == nil {
		o.AsTenant = other.AsTenant
	} else if other.AsTenant != nil {
		return errors.New("tenant_name option specified multiple times")
	}

	if o.ForceTenantID == nil {
		o.ForceTenantID = other.ForceTenantID
	} else if other.ForceTenantID != nil {
		return errors.New("tenant option specified multiple times")
	}

	if o.SchemaOnly {
		if other.SchemaOnly {
			return errors.New("schema_only option specified multiple times")
		}
	} else {
		o.SchemaOnly = other.SchemaOnly
	}
	if o.VerifyData {
		if other.VerifyData {
			return errors.New("verify_backup_table_data option specified multiple times")
		}
	} else {
		o.VerifyData = other.VerifyData
	}
	return nil
}

// IsDefault returns true if this backup options struct has default value.
func (o RestoreOptions) IsDefault() bool {
	options := RestoreOptions{}
	return o.SkipMissingFKs == options.SkipMissingFKs &&
		o.SkipMissingSequences == options.SkipMissingSequences &&
		o.SkipMissingSequenceOwners == options.SkipMissingSequenceOwners &&
		o.SkipMissingViews == options.SkipMissingViews &&
		cmp.Equal(o.DecryptionKMSURI, options.DecryptionKMSURI) &&
		o.EncryptionPassphrase == options.EncryptionPassphrase &&
		o.IntoDB == options.IntoDB &&
		o.Detached == options.Detached &&
		o.SkipLocalitiesCheck == options.SkipLocalitiesCheck &&
		o.DebugPauseOn == options.DebugPauseOn &&
		o.NewDBName == options.NewDBName &&
		cmp.Equal(o.IncrementalStorage, options.IncrementalStorage) &&
		o.AsTenant == options.AsTenant &&
		o.ForceTenantID == options.ForceTenantID &&
		o.SchemaOnly == options.SchemaOnly &&
		o.VerifyData == options.VerifyData
}

// BackupTargetList represents a list of targets.
// Only one field may be non-nil.
type BackupTargetList struct {
	Databases NameList
	Schemas   ObjectNamePrefixList
	Tables    TableAttrs
	TenantID  TenantID
}

// Format implements the NodeFormatter interface.
func (tl *BackupTargetList) Format(ctx *FmtCtx) {
	if tl.Databases != nil {
		ctx.WriteString("DATABASE ")
		ctx.FormatNode(&tl.Databases)
	} else if tl.Schemas != nil {
		ctx.WriteString("SCHEMA ")
		ctx.FormatNode(&tl.Schemas)
	} else if tl.TenantID.Specified {
		ctx.WriteString("TENANT ")
		ctx.FormatNode(&tl.TenantID)
	} else {
		if tl.Tables.SequenceOnly {
			ctx.WriteString("SEQUENCE ")
		} else {
			ctx.WriteString("TABLE ")
		}
		ctx.FormatNode(&tl.Tables.TablePatterns)
	}
}
