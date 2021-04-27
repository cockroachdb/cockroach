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

// Code is a wrapper around a string to ensure that pgcodes are used in
// different pgerror functions by avoiding accidental string input.
type Code struct {
	code string
}

// MakeCode converts a string into a Code.
func MakeCode(s string) Code {
	return Code{code: s}
}

// String returns the underlying pgcode string.
func (c Code) String() string {
	return c.code
}

// PG error codes from: http://www.postgresql.org/docs/9.5/static/errcodes-appendix.html.
// Specifically, errcodes.txt is copied from from Postgres' src/backend/utils/errcodes.txt.
//
// The error definitions were generated using the generate.sh script,
// with a bit of manual tweaking performed afterwards.
var (
	// Section: Class 00 - Successful Completion
	SuccessfulCompletion = MakeCode("00000")
	// Section: Class 01 - Warning
	Warning                                 = MakeCode("01000")
	WarningDynamicResultSetsReturned        = MakeCode("0100C")
	WarningImplicitZeroBitPadding           = MakeCode("01008")
	WarningNullValueEliminatedInSetFunction = MakeCode("01003")
	WarningPrivilegeNotGranted              = MakeCode("01007")
	WarningPrivilegeNotRevoked              = MakeCode("01006")
	WarningStringDataRightTruncation        = MakeCode("01004")
	WarningDeprecatedFeature                = MakeCode("01P01")
	// Section: Class 02 - No Data (this is also a warning class per the SQL standard)
	NoData                                = MakeCode("02000")
	NoAdditionalDynamicResultSetsReturned = MakeCode("02001")
	// Section: Class 03 - SQL Statement Not Yet Complete
	SQLStatementNotYetComplete = MakeCode("03000")
	// Section: Class 08 - Connection Exception
	ConnectionException                           = MakeCode("08000")
	ConnectionDoesNotExist                        = MakeCode("08003")
	ConnectionFailure                             = MakeCode("08006")
	SQLclientUnableToEstablishSQLconnection       = MakeCode("08001")
	SQLserverRejectedEstablishmentOfSQLconnection = MakeCode("08004")
	TransactionResolutionUnknown                  = MakeCode("08007")
	ProtocolViolation                             = MakeCode("08P01")
	// Section: Class 09 - Triggered Action Exception
	TriggeredActionException = MakeCode("09000")
	// Section: Class 0A - Feature Not Supported
	FeatureNotSupported = MakeCode("0A000")
	// Section: Class 0B - Invalid Transaction Initiation
	InvalidTransactionInitiation = MakeCode("0B000")
	// Section: Class 0F - Locator Exception
	LocatorException            = MakeCode("0F000")
	InvalidLocatorSpecification = MakeCode("0F001")
	// Section: Class 0L - Invalid Grantor
	InvalidGrantor        = MakeCode("0L000")
	InvalidGrantOperation = MakeCode("0LP01")
	// Section: Class 0P - Invalid Role Specification
	InvalidRoleSpecification = MakeCode("0P000")
	// Section: Class 0Z - Diagnostics Exception
	DiagnosticsException                           = MakeCode("0Z000")
	StackedDiagnosticsAccessedWithoutActiveHandler = MakeCode("0Z002")
	// Section: Class 20 - Case Not Found
	CaseNotFound = MakeCode("20000")
	// Section: Class 21 - Cardinality Violation
	CardinalityViolation = MakeCode("21000")
	// Section: Class 22 - Data Exception
	DataException                         = MakeCode("22000")
	ArraySubscript                        = MakeCode("2202E")
	CharacterNotInRepertoire              = MakeCode("22021")
	DatetimeFieldOverflow                 = MakeCode("22008")
	DivisionByZero                        = MakeCode("22012")
	InvalidWindowFrameOffset              = MakeCode("22013")
	ErrorInAssignment                     = MakeCode("22005")
	EscapeCharacterConflict               = MakeCode("2200B")
	IndicatorOverflow                     = MakeCode("22022")
	IntervalFieldOverflow                 = MakeCode("22015")
	InvalidArgumentForLogarithm           = MakeCode("2201E")
	InvalidArgumentForNtileFunction       = MakeCode("22014")
	InvalidArgumentForNthValueFunction    = MakeCode("22016")
	InvalidArgumentForPowerFunction       = MakeCode("2201F")
	InvalidArgumentForWidthBucketFunction = MakeCode("2201G")
	InvalidCharacterValueForCast          = MakeCode("22018")
	InvalidDatetimeFormat                 = MakeCode("22007")
	InvalidEscapeCharacter                = MakeCode("22019")
	InvalidEscapeOctet                    = MakeCode("2200D")
	InvalidEscapeSequence                 = MakeCode("22025")
	NonstandardUseOfEscapeCharacter       = MakeCode("22P06")
	InvalidIndicatorParameterValue        = MakeCode("22010")
	InvalidParameterValue                 = MakeCode("22023")
	InvalidRegularExpression              = MakeCode("2201B")
	InvalidRowCountInLimitClause          = MakeCode("2201W")
	InvalidRowCountInResultOffsetClause   = MakeCode("2201X")
	InvalidTimeZoneDisplacementValue      = MakeCode("22009")
	InvalidUseOfEscapeCharacter           = MakeCode("2200C")
	MostSpecificTypeMismatch              = MakeCode("2200G")
	NullValueNotAllowed                   = MakeCode("22004")
	NullValueNoIndicatorParameter         = MakeCode("22002")
	NumericValueOutOfRange                = MakeCode("22003")
	SequenceGeneratorLimitExceeded        = MakeCode("2200H")
	StringDataLengthMismatch              = MakeCode("22026")
	StringDataRightTruncation             = MakeCode("22001")
	Substring                             = MakeCode("22011")
	Trim                                  = MakeCode("22027")
	UnterminatedCString                   = MakeCode("22024")
	ZeroLengthCharacterString             = MakeCode("2200F")
	FloatingPointException                = MakeCode("22P01")
	InvalidTextRepresentation             = MakeCode("22P02")
	InvalidBinaryRepresentation           = MakeCode("22P03")
	BadCopyFileFormat                     = MakeCode("22P04")
	UntranslatableCharacter               = MakeCode("22P05")
	NotAnXMLDocument                      = MakeCode("2200L")
	InvalidXMLDocument                    = MakeCode("2200M")
	InvalidXMLContent                     = MakeCode("2200N")
	InvalidXMLComment                     = MakeCode("2200S")
	InvalidXMLProcessingInstruction       = MakeCode("2200T")
	// Section: Class 23 - Integrity Constraint Violation
	IntegrityConstraintViolation = MakeCode("23000")
	RestrictViolation            = MakeCode("23001")
	NotNullViolation             = MakeCode("23502")
	ForeignKeyViolation          = MakeCode("23503")
	UniqueViolation              = MakeCode("23505")
	CheckViolation               = MakeCode("23514")
	ExclusionViolation           = MakeCode("23P01")
	// Section: Class 24 - Invalid Cursor State
	InvalidCursorState = MakeCode("24000")
	// Section: Class 25 - Invalid Transaction State
	InvalidTransactionState                         = MakeCode("25000")
	ActiveSQLTransaction                            = MakeCode("25001")
	BranchTransactionAlreadyActive                  = MakeCode("25002")
	HeldCursorRequiresSameIsolationLevel            = MakeCode("25008")
	InappropriateAccessModeForBranchTransaction     = MakeCode("25003")
	InappropriateIsolationLevelForBranchTransaction = MakeCode("25004")
	NoActiveSQLTransactionForBranchTransaction      = MakeCode("25005")
	ReadOnlySQLTransaction                          = MakeCode("25006")
	SchemaAndDataStatementMixingNotSupported        = MakeCode("25007")
	NoActiveSQLTransaction                          = MakeCode("25P01")
	InFailedSQLTransaction                          = MakeCode("25P02")
	// Section: Class 26 - Invalid SQL Statement Name
	InvalidSQLStatementName = MakeCode("26000")
	// Section: Class 27 - Triggered Data Change Violation
	TriggeredDataChangeViolation = MakeCode("27000")
	// Section: Class 28 - Invalid Authorization Specification
	InvalidAuthorizationSpecification = MakeCode("28000")
	InvalidPassword                   = MakeCode("28P01")
	// Section: Class 2B - Dependent Privilege Descriptors Still Exist
	DependentPrivilegeDescriptorsStillExist = MakeCode("2B000")
	DependentObjectsStillExist              = MakeCode("2BP01")
	// Section: Class 2D - Invalid Transaction Termination
	InvalidTransactionTermination = MakeCode("2D000")
	// Section: Class 2F - SQL Routine Exception
	RoutineExceptionFunctionExecutedNoReturnStatement = MakeCode("2F005")
	RoutineExceptionModifyingSQLDataNotPermitted      = MakeCode("2F002")
	RoutineExceptionProhibitedSQLStatementAttempted   = MakeCode("2F003")
	RoutineExceptionReadingSQLDataNotPermitted        = MakeCode("2F004")
	// Section: Class 34 - Invalid Cursor Name
	InvalidCursorName = MakeCode("34000")
	// Section: Class 38 - External Routine Exception
	ExternalRoutineException                       = MakeCode("38000")
	ExternalRoutineContainingSQLNotPermitted       = MakeCode("38001")
	ExternalRoutineModifyingSQLDataNotPermitted    = MakeCode("38002")
	ExternalRoutineProhibitedSQLStatementAttempted = MakeCode("38003")
	ExternalRoutineReadingSQLDataNotPermitted      = MakeCode("38004")
	// Section: Class 39 - External Routine Invocation Exception
	ExternalRoutineInvocationException     = MakeCode("39000")
	ExternalRoutineInvalidSQLstateReturned = MakeCode("39001")
	ExternalRoutineNullValueNotAllowed     = MakeCode("39004")
	ExternalRoutineTriggerProtocolViolated = MakeCode("39P01")
	ExternalRoutineSrfProtocolViolated     = MakeCode("39P02")
	// Section: Class 3B - Savepoint Exception
	SavepointException            = MakeCode("3B000")
	InvalidSavepointSpecification = MakeCode("3B001")
	// Section: Class 3D - Invalid Catalog Name
	InvalidCatalogName = MakeCode("3D000")
	// Section: Class 3F - Invalid Schema Name
	InvalidSchemaName = MakeCode("3F000")
	// Section: Class 40 - Transaction Rollback
	TransactionRollback                     = MakeCode("40000")
	TransactionIntegrityConstraintViolation = MakeCode("40002")
	SerializationFailure                    = MakeCode("40001")
	StatementCompletionUnknown              = MakeCode("40003")
	DeadlockDetected                        = MakeCode("40P01")
	// Section: Class 42 - Syntax Error or Access Rule Violation
	SyntaxErrorOrAccessRuleViolation   = MakeCode("42000")
	Syntax                             = MakeCode("42601")
	InsufficientPrivilege              = MakeCode("42501")
	CannotCoerce                       = MakeCode("42846")
	Grouping                           = MakeCode("42803")
	Windowing                          = MakeCode("42P20")
	InvalidRecursion                   = MakeCode("42P19")
	InvalidForeignKey                  = MakeCode("42830")
	InvalidName                        = MakeCode("42602")
	NameTooLong                        = MakeCode("42622")
	ReservedName                       = MakeCode("42939")
	DatatypeMismatch                   = MakeCode("42804")
	IndeterminateDatatype              = MakeCode("42P18")
	CollationMismatch                  = MakeCode("42P21")
	IndeterminateCollation             = MakeCode("42P22")
	WrongObjectType                    = MakeCode("42809")
	UndefinedColumn                    = MakeCode("42703")
	UndefinedCursor                    = MakeCode("34000")
	UndefinedDatabase                  = MakeCode("3D000")
	UndefinedFunction                  = MakeCode("42883")
	UndefinedPreparedStatement         = MakeCode("26000")
	UndefinedSchema                    = MakeCode("3F000")
	UndefinedTable                     = MakeCode("42P01")
	UndefinedParameter                 = MakeCode("42P02")
	UndefinedObject                    = MakeCode("42704")
	DuplicateColumn                    = MakeCode("42701")
	DuplicateCursor                    = MakeCode("42P03")
	DuplicateDatabase                  = MakeCode("42P04")
	DuplicateFunction                  = MakeCode("42723")
	DuplicatePreparedStatement         = MakeCode("42P05")
	DuplicateSchema                    = MakeCode("42P06")
	DuplicateRelation                  = MakeCode("42P07")
	DuplicateAlias                     = MakeCode("42712")
	DuplicateObject                    = MakeCode("42710")
	AmbiguousColumn                    = MakeCode("42702")
	AmbiguousFunction                  = MakeCode("42725")
	AmbiguousParameter                 = MakeCode("42P08")
	AmbiguousAlias                     = MakeCode("42P09")
	InvalidColumnReference             = MakeCode("42P10")
	InvalidColumnDefinition            = MakeCode("42611")
	InvalidCursorDefinition            = MakeCode("42P11")
	InvalidDatabaseDefinition          = MakeCode("42P12")
	InvalidFunctionDefinition          = MakeCode("42P13")
	InvalidPreparedStatementDefinition = MakeCode("42P14")
	InvalidSchemaDefinition            = MakeCode("42P15")
	InvalidTableDefinition             = MakeCode("42P16")
	InvalidObjectDefinition            = MakeCode("42P17")
	FileAlreadyExists                  = MakeCode("42C01")
	// Section: Class 44 - WITH CHECK OPTION Violation
	WithCheckOptionViolation = MakeCode("44000")
	// Section: Class 53 - Insufficient Resources
	InsufficientResources      = MakeCode("53000")
	DiskFull                   = MakeCode("53100")
	OutOfMemory                = MakeCode("53200")
	TooManyConnections         = MakeCode("53300")
	ConfigurationLimitExceeded = MakeCode("53400")
	// Section: Class 54 - Program Limit Exceeded
	ProgramLimitExceeded = MakeCode("54000")
	StatementTooComplex  = MakeCode("54001")
	TooManyColumns       = MakeCode("54011")
	TooManyArguments     = MakeCode("54023")
	// Section: Class 55 - Object Not In Prerequisite State
	ObjectNotInPrerequisiteState = MakeCode("55000")
	ObjectInUse                  = MakeCode("55006")
	CantChangeRuntimeParam       = MakeCode("55P02")
	LockNotAvailable             = MakeCode("55P03")
	// Section: Class 57 - Operator Intervention
	OperatorIntervention = MakeCode("57000")
	QueryCanceled        = MakeCode("57014")
	AdminShutdown        = MakeCode("57P01")
	CrashShutdown        = MakeCode("57P02")
	CannotConnectNow     = MakeCode("57P03")
	DatabaseDropped      = MakeCode("57P04")
	// Section: Class 58 - System Error
	System        = MakeCode("58000")
	Io            = MakeCode("58030")
	UndefinedFile = MakeCode("58P01")
	DuplicateFile = MakeCode("58P02")
	// Section: Class F0 - Configuration File Error
	ConfigFile     = MakeCode("F0000")
	LockFileExists = MakeCode("F0001")
	// Section: Class HV - Foreign Data Wrapper Error (SQL/MED)
	FdwError                             = MakeCode("HV000")
	FdwColumnNameNotFound                = MakeCode("HV005")
	FdwDynamicParameterValueNeeded       = MakeCode("HV002")
	FdwFunctionSequenceError             = MakeCode("HV010")
	FdwInconsistentDescriptorInformation = MakeCode("HV021")
	FdwInvalidAttributeValue             = MakeCode("HV024")
	FdwInvalidColumnName                 = MakeCode("HV007")
	FdwInvalidColumnNumber               = MakeCode("HV008")
	FdwInvalidDataType                   = MakeCode("HV004")
	FdwInvalidDataTypeDescriptors        = MakeCode("HV006")
	FdwInvalidDescriptorFieldIdentifier  = MakeCode("HV091")
	FdwInvalidHandle                     = MakeCode("HV00B")
	FdwInvalidOptionIndex                = MakeCode("HV00C")
	FdwInvalidOptionName                 = MakeCode("HV00D")
	FdwInvalidStringLengthOrBufferLength = MakeCode("HV090")
	FdwInvalidStringFormat               = MakeCode("HV00A")
	FdwInvalidUseOfNullPointer           = MakeCode("HV009")
	FdwTooManyHandles                    = MakeCode("HV014")
	FdwOutOfMemory                       = MakeCode("HV001")
	FdwNoSchemas                         = MakeCode("HV00P")
	FdwOptionNameNotFound                = MakeCode("HV00J")
	FdwReplyHandle                       = MakeCode("HV00K")
	FdwSchemaNotFound                    = MakeCode("HV00Q")
	FdwTableNotFound                     = MakeCode("HV00R")
	FdwUnableToCreateExecution           = MakeCode("HV00L")
	FdwUnableToCreateReply               = MakeCode("HV00M")
	FdwUnableToEstablishConnection       = MakeCode("HV00N")
	// Section: Class P0 - PL/pgSQL Error
	PLpgSQL        = MakeCode("P0000")
	RaiseException = MakeCode("P0001")
	NoDataFound    = MakeCode("P0002")
	TooManyRows    = MakeCode("P0003")
	AssertFailure  = MakeCode("P0004")
	// Section: Class XX - Internal Error
	Internal       = MakeCode("XX000")
	DataCorrupted  = MakeCode("XX001")
	IndexCorrupted = MakeCode("XX002")
)

// The following errors are CockroachDB-specific.

var (
	// Uncategorized is used for errors that flow out to a client
	// when there's no code known yet.
	Uncategorized = MakeCode("XXUUU")

	// CCLRequired signals that a CCL binary is required to complete this
	// task.
	CCLRequired = MakeCode("XXC01")

	// CCLValidLicenseRequired signals that a valid CCL license is
	// required to complete this task.
	CCLValidLicenseRequired = MakeCode("XXC02")

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
	TransactionCommittedWithSchemaChangeFailure = MakeCode("XXA00")

	// Class 22C - Semantic errors in the structure of a SQL statement.

	// ScalarOperationCannotRunWithoutFullSessionContext signals that an
	// operator or built-in function was used that requires a full session
	// context and thus cannot be run in a background job or away from the SQL
	// gateway.
	ScalarOperationCannotRunWithoutFullSessionContext = MakeCode("22C01")

	// Class 55C - Object Not In Prerequisite State (Cockroach extension)

	// SchemaChangeOccurred signals that a DDL change to the targets of a
	// CHANGEFEED has lead to its termination. If this error code is received
	// the CHANGEFEED will have previously emitted a resolved timestamp which
	// precedes the hlc timestamp of the relevant DDL transaction.
	SchemaChangeOccurred = MakeCode("55C01")

	// NoPrimaryKey signals that a table descriptor is invalid because the table
	// does not have a primary key.
	NoPrimaryKey = MakeCode("55C02")

	// Class 58C - System errors related to CockroachDB node problems.

	// RangeUnavailable signals that some data from the cluster cannot be
	// accessed (e.g. because all replicas awol).
	RangeUnavailable = MakeCode("58C00")

	// InternalConnectionFailure refers to a networking error encountered
	// internally on a connection between different Cockroach nodes.
	InternalConnectionFailure = MakeCode("58C01")
)
