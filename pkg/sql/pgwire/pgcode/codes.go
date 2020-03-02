// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package pgcode

// PG error codes from: http://www.postgresql.org/docs/9.5/static/errcodes-appendix.html.
// Specifically, errcodes.txt is copied from from Postgres' src/backend/utils/errcodes.txt.
//
// The error definitions were generated using the generate.sh script,
// with a bit of manual tweaking performed afterwards.
const (
	// Class 00 - Successful Completion
	SuccessfulCompletion = "00000"
	// Class 01 - Warning
	Warning                                 = "01000"
	WarningDynamicResultSetsReturned        = "0100C"
	WarningImplicitZeroBitPadding           = "01008"
	WarningNullValueEliminatedInSetFunction = "01003"
	WarningPrivilegeNotGranted              = "01007"
	WarningPrivilegeNotRevoked              = "01006"
	WarningStringDataRightTruncation        = "01004"
	WarningDeprecatedFeature                = "01P01"
	// Class 02 - No Data (this is also a warning class per the SQL standard)
	NoData                                = "02000"
	NoAdditionalDynamicResultSetsReturned = "02001"
	// Class 03 - SQL Statement Not Yet Complete
	SQLStatementNotYetComplete = "03000"
	// Class 08 - Connection Exception
	ConnectionException                           = "08000"
	ConnectionDoesNotExist                        = "08003"
	ConnectionFailure                             = "08006"
	SQLclientUnableToEstablishSQLconnection       = "08001"
	SQLserverRejectedEstablishmentOfSQLconnection = "08004"
	TransactionResolutionUnknown                  = "08007"
	ProtocolViolation                             = "08P01"
	// Class 09 - Triggered Action Exception
	TriggeredActionException = "09000"
	// Class 0A - Feature Not Supported
	FeatureNotSupported = "0A000"
	// Class 0B - Invalid Transaction Initiation
	InvalidTransactionInitiation = "0B000"
	// Class 0F - Locator Exception
	LocatorException            = "0F000"
	InvalidLocatorSpecification = "0F001"
	// Class 0L - Invalid Grantor
	InvalidGrantor        = "0L000"
	InvalidGrantOperation = "0LP01"
	// Class 0P - Invalid Role Specification
	InvalidRoleSpecification = "0P000"
	// Class 0Z - Diagnostics Exception
	DiagnosticsException                           = "0Z000"
	StackedDiagnosticsAccessedWithoutActiveHandler = "0Z002"
	// Class 20 - Case Not Found
	CaseNotFound = "20000"
	// Class 21 - Cardinality Violation
	CardinalityViolation = "21000"
	// Class 22 - Data Exception
	DataException                         = "22000"
	ArraySubscript                        = "2202E"
	CharacterNotInRepertoire              = "22021"
	DatetimeFieldOverflow                 = "22008"
	DivisionByZero                        = "22012"
	InvalidWindowFrameOffset              = "22013"
	ErrorInAssignment                     = "22005"
	EscapeCharacterConflict               = "2200B"
	IndicatorOverflow                     = "22022"
	IntervalFieldOverflow                 = "22015"
	InvalidArgumentForLogarithm           = "2201E"
	InvalidArgumentForNtileFunction       = "22014"
	InvalidArgumentForNthValueFunction    = "22016"
	InvalidArgumentForPowerFunction       = "2201F"
	InvalidArgumentForWidthBucketFunction = "2201G"
	InvalidCharacterValueForCast          = "22018"
	InvalidDatetimeFormat                 = "22007"
	InvalidEscapeCharacter                = "22019"
	InvalidEscapeOctet                    = "2200D"
	InvalidEscapeSequence                 = "22025"
	NonstandardUseOfEscapeCharacter       = "22P06"
	InvalidIndicatorParameterValue        = "22010"
	InvalidParameterValue                 = "22023"
	InvalidRegularExpression              = "2201B"
	InvalidRowCountInLimitClause          = "2201W"
	InvalidRowCountInResultOffsetClause   = "2201X"
	InvalidTimeZoneDisplacementValue      = "22009"
	InvalidUseOfEscapeCharacter           = "2200C"
	MostSpecificTypeMismatch              = "2200G"
	NullValueNotAllowed                   = "22004"
	NullValueNoIndicatorParameter         = "22002"
	NumericValueOutOfRange                = "22003"
	SequenceGeneratorLimitExceeded        = "2200H"
	StringDataLengthMismatch              = "22026"
	StringDataRightTruncation             = "22001"
	Substring                             = "22011"
	Trim                                  = "22027"
	UnterminatedCString                   = "22024"
	ZeroLengthCharacterString             = "2200F"
	FloatingPointException                = "22P01"
	InvalidTextRepresentation             = "22P02"
	InvalidBinaryRepresentation           = "22P03"
	BadCopyFileFormat                     = "22P04"
	UntranslatableCharacter               = "22P05"
	NotAnXMLDocument                      = "2200L"
	InvalidXMLDocument                    = "2200M"
	InvalidXMLContent                     = "2200N"
	InvalidXMLComment                     = "2200S"
	InvalidXMLProcessingInstruction       = "2200T"
	// Class 23 - Integrity Constraint Violation
	IntegrityConstraintViolation = "23000"
	RestrictViolation            = "23001"
	NotNullViolation             = "23502"
	ForeignKeyViolation          = "23503"
	UniqueViolation              = "23505"
	CheckViolation               = "23514"
	ExclusionViolation           = "23P01"
	// Class 24 - Invalid Cursor State
	InvalidCursorState = "24000"
	// Class 25 - Invalid Transaction State
	InvalidTransactionState                         = "25000"
	ActiveSQLTransaction                            = "25001"
	BranchTransactionAlreadyActive                  = "25002"
	HeldCursorRequiresSameIsolationLevel            = "25008"
	InappropriateAccessModeForBranchTransaction     = "25003"
	InappropriateIsolationLevelForBranchTransaction = "25004"
	NoActiveSQLTransactionForBranchTransaction      = "25005"
	ReadOnlySQLTransaction                          = "25006"
	SchemaAndDataStatementMixingNotSupported        = "25007"
	NoActiveSQLTransaction                          = "25P01"
	InFailedSQLTransaction                          = "25P02"
	// Class 26 - Invalid SQL Statement Name
	InvalidSQLStatementName = "26000"
	// Class 27 - Triggered Data Change Violation
	TriggeredDataChangeViolation = "27000"
	// Class 28 - Invalid Authorization Specification
	InvalidAuthorizationSpecification = "28000"
	InvalidPassword                   = "28P01"
	// Class 2B - Dependent Privilege Descriptors Still Exist
	DependentPrivilegeDescriptorsStillExist = "2B000"
	DependentObjectsStillExist              = "2BP01"
	// Class 2D - Invalid Transaction Termination
	InvalidTransactionTermination = "2D000"
	// Class 2F - SQL Routine Exception
	SQLRoutineException                               = "2F000"
	RoutineExceptionFunctionExecutedNoReturnStatement = "2F005"
	RoutineExceptionModifyingSQLDataNotPermitted      = "2F002"
	RoutineExceptionProhibitedSQLStatementAttempted   = "2F003"
	RoutineExceptionReadingSQLDataNotPermitted        = "2F004"
	// Class 34 - Invalid Cursor Name
	InvalidCursorName = "34000"
	// Class 38 - External Routine Exception
	ExternalRoutineException                       = "38000"
	ExternalRoutineContainingSQLNotPermitted       = "38001"
	ExternalRoutineModifyingSQLDataNotPermitted    = "38002"
	ExternalRoutineProhibitedSQLStatementAttempted = "38003"
	ExternalRoutineReadingSQLDataNotPermitted      = "38004"
	// Class 39 - External Routine Invocation Exception
	ExternalRoutineInvocationException     = "39000"
	ExternalRoutineInvalidSQLstateReturned = "39001"
	ExternalRoutineNullValueNotAllowed     = "39004"
	ExternalRoutineTriggerProtocolViolated = "39P01"
	ExternalRoutineSrfProtocolViolated     = "39P02"
	// Class 3B - Savepoint Exception
	SavepointException            = "3B000"
	InvalidSavepointSpecification = "3B001"
	// Class 3D - Invalid Catalog Name
	InvalidCatalogName = "3D000"
	// Class 3F - Invalid Schema Name
	InvalidSchemaName = "3F000"
	// Class 40 - Transaction Rollback
	TransactionRollback                     = "40000"
	TransactionIntegrityConstraintViolation = "40002"
	SerializationFailure                    = "40001"
	StatementCompletionUnknown              = "40003"
	DeadlockDetected                        = "40P01"
	// Class 42 - Syntax  or Access Rule Violation
	SyntaxErrorOrAccessRuleViolation   = "42000"
	Syntax                             = "42601"
	InsufficientPrivilege              = "42501"
	CannotCoerce                       = "42846"
	Grouping                           = "42803"
	Windowing                          = "42P20"
	InvalidRecursion                   = "42P19"
	InvalidForeignKey                  = "42830"
	InvalidName                        = "42602"
	NameTooLong                        = "42622"
	ReservedName                       = "42939"
	DatatypeMismatch                   = "42804"
	IndeterminateDatatype              = "42P18"
	CollationMismatch                  = "42P21"
	IndeterminateCollation             = "42P22"
	WrongObjectType                    = "42809"
	UndefinedColumn                    = "42703"
	UndefinedFunction                  = "42883"
	UndefinedTable                     = "42P01"
	UndefinedParameter                 = "42P02"
	UndefinedObject                    = "42704"
	DuplicateColumn                    = "42701"
	DuplicateCursor                    = "42P03"
	DuplicateDatabase                  = "42P04"
	DuplicateFunction                  = "42723"
	DuplicatePreparedStatement         = "42P05"
	DuplicateSchema                    = "42P06"
	DuplicateRelation                  = "42P07"
	DuplicateAlias                     = "42712"
	DuplicateObject                    = "42710"
	AmbiguousColumn                    = "42702"
	AmbiguousFunction                  = "42725"
	AmbiguousParameter                 = "42P08"
	AmbiguousAlias                     = "42P09"
	InvalidColumnReference             = "42P10"
	InvalidColumnDefinition            = "42611"
	InvalidCursorDefinition            = "42P11"
	InvalidDatabaseDefinition          = "42P12"
	InvalidFunctionDefinition          = "42P13"
	InvalidPreparedStatementDefinition = "42P14"
	InvalidSchemaDefinition            = "42P15"
	InvalidTableDefinition             = "42P16"
	InvalidObjectDefinition            = "42P17"
	FileAlreadyExists                  = "42C01"
	// Class 44 - WITH CHECK OPTION Violation
	WithCheckOptionViolation = "44000"
	// Class 53 - Insufficient Resources
	InsufficientResources      = "53000"
	DiskFull                   = "53100"
	OutOfMemory                = "53200"
	TooManyConnections         = "53300"
	ConfigurationLimitExceeded = "53400"
	// Class 54 - Program Limit Exceeded
	ProgramLimitExceeded = "54000"
	StatementTooComplex  = "54001"
	TooManyColumns       = "54011"
	TooManyArguments     = "54023"
	// Class 55 - Object Not In Prerequisite State
	ObjectNotInPrerequisiteState = "55000"
	ObjectInUse                  = "55006"
	CantChangeRuntimeParam       = "55P02"
	LockNotAvailable             = "55P03"
	// Class 57 - Operator Intervention
	OperatorIntervention = "57000"
	QueryCanceled        = "57014"
	AdminShutdown        = "57P01"
	CrashShutdown        = "57P02"
	CannotConnectNow     = "57P03"
	DatabaseDropped      = "57P04"
	// Class 58 - System
	System        = "58000"
	Io            = "58030"
	UndefinedFile = "58P01"
	DuplicateFile = "58P02"
	// Class F0 - Configuration File Error
	ConfigFile     = "F0000"
	LockFileExists = "F0001"
	// Class HV - Foreign Data Wrapper  (SQL/MED)
	Fdw                                  = "HV000"
	FdwColumnNameNotFound                = "HV005"
	FdwDynamicParameterValueNeeded       = "HV002"
	FdwFunctionSequence                  = "HV010"
	FdwInconsistentDescriptorInformation = "HV021"
	FdwInvalidAttributeValue             = "HV024"
	FdwInvalidColumnName                 = "HV007"
	FdwInvalidColumnNumber               = "HV008"
	FdwInvalidDataType                   = "HV004"
	FdwInvalidDataTypeDescriptors        = "HV006"
	FdwInvalidDescriptorFieldIdentifier  = "HV091"
	FdwInvalidHandle                     = "HV00B"
	FdwInvalidOptionIndex                = "HV00C"
	FdwInvalidOptionName                 = "HV00D"
	FdwInvalidStringLengthOrBufferLength = "HV090"
	FdwInvalidStringFormat               = "HV00A"
	FdwInvalidUseOfNullPointer           = "HV009"
	FdwTooManyHandles                    = "HV014"
	FdwOutOfMemory                       = "HV001"
	FdwNoSchemas                         = "HV00P"
	FdwOptionNameNotFound                = "HV00J"
	FdwReplyHandle                       = "HV00K"
	FdwSchemaNotFound                    = "HV00Q"
	FdwTableNotFound                     = "HV00R"
	FdwUnableToCreateExecution           = "HV00L"
	FdwUnableToCreateReply               = "HV00M"
	FdwUnableToEstablishConnection       = "HV00N"
	// Class P0 - PL/pgSQL Error
	PLpgSQL        = "P0000"
	RaiseException = "P0001"
	NoDataFound    = "P0002"
	TooManyRows    = "P0003"
	// Class XX - Internal Error
	Internal       = "XX000"
	DataCorrupted  = "XX001"
	IndexCorrupted = "XX002"
)

// The following errors are CockroachDB-specific.

const (
	// Uncategorized is used for errors that flow out to a client
	// when there's no code known yet.
	Uncategorized = "XXUUU"

	// CCLRequired signals that a CCL binary is required to complete this
	// task.
	CCLRequired = "XXC01"

	// CCLValidLicenseRequired signals that a valid CCL license is
	// required to complete this task.
	CCLValidLicenseRequired = "XXC02"

	// TransactionCommittedWithSchemaChangeFailure signals that the
	// non-DDL payload of a transaction was committed successfully but
	// some DDL operation failed, without rolling back the rest of the
	// transaction.
	//
	// We define a separate code instead of reusing a code from
	// PostgreSQL (like StatementCompletionUnknown) because that makes
	// it easier to document the error (this code only occurs in this
	// particular situation) in a way that's unique to CockroachDB.
	//
	// We also use a "XX" code for this for several reasons:
	// - it needs to override any other pg code set "underneath" in the cause.
	// - it forces implementers of logic tests to be mindful about
	//   this situation. The logic test runner will remind the implementer
	//   that:
	//       serious error with code "XXA00" occurred; if expected,
	//       must use 'error pgcode XXA00 ...'
	TransactionCommittedWithSchemaChangeFailure = "XXA00"

	// Class 22C - Semantic errors in the structure of a SQL statement.

	// ScalarOperationCannotRunWithoutFullSessionContext signals that an
	// operator or built-in function was used that requires a full
	// session contextand thus cannot be run in a background job or away
	// from the SQL gateway.
	ScalarOperationCannotRunWithoutFullSessionContext = "22C01"

	// Class 55C - Object Not In Prerequisite State (Cockroach extension)

	// SchemaChangeOccurred signals that a DDL change to the targets of a
	// CHANGEFEED has lead to its termination. If this error code is received
	// the CHANGEFEED will have previously emitted a resolved timestamp which
	// precedes the hlc timestamp of the relevant DDL transaction.
	SchemaChangeOccurred = "55C01"

	// Class 58C - System errors related to CockroachDB node problems.

	// RangeUnavailable signals that some data from the cluster cannot be
	// accessed (e.g. because all replicas awol).
	RangeUnavailable = "58C00"
	// DeprecatedRangeUnavailable is code that we used for RangeUnavailable until 19.2.
	// 20.1 needs to recognize it coming from 19.2 nodes.
	// TODO(andrei): remove in 20.2.
	DeprecatedRangeUnavailable = "XXC00"

	// InternalConnectionFailure refers to a networking error encountered
	// internally on a connection between different Cockroach nodes.
	InternalConnectionFailure = "58C01"
	// DeprecatedInternalConnectionFailure is code that we used for
	// InternalConnectionFailure until 19.2.
	// 20.1 needs to recognize it coming from 19.2 nodes.
	// TODO(andrei): remove in 20.2.
	DeprecatedInternalConnectionFailure = ConnectionFailure
)
