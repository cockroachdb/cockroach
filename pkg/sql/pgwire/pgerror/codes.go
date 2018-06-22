// Copyright 2016 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package pgerror

// PG error codes from: http://www.postgresql.org/docs/9.5/static/errcodes-appendix.html.
// Specifically, errcodes.txt is copied from from Postgres' src/backend/utils/errcodes.txt.
//
// The error definitions were generated using the generate.sh script,
// with a bit of manual tweaking performed afterwards.
const (
	// Class 00 - Successful Completion
	CodeSuccessfulCompletionError = "00000"
	// Class 01 - Warning
	CodeWarningError                                 = "01000"
	CodeWarningDynamicResultSetsReturnedError        = "0100C"
	CodeWarningImplicitZeroBitPaddingError           = "01008"
	CodeWarningNullValueEliminatedInSetFunctionError = "01003"
	CodeWarningPrivilegeNotGrantedError              = "01007"
	CodeWarningPrivilegeNotRevokedError              = "01006"
	CodeWarningStringDataRightTruncationError        = "01004"
	CodeWarningDeprecatedFeatureError                = "01P01"
	// Class 02 - No Data (this is also a warning class per the SQL standard)
	CodeNoDataError                                = "02000"
	CodeNoAdditionalDynamicResultSetsReturnedError = "02001"
	// Class 03 - SQL Statement Not Yet Complete
	CodeSQLStatementNotYetCompleteError = "03000"
	// Class 08 - Connection Exception
	CodeConnectionExceptionError                           = "08000"
	CodeConnectionDoesNotExistError                        = "08003"
	CodeConnectionFailureError                             = "08006"
	CodeSQLclientUnableToEstablishSQLconnectionError       = "08001"
	CodeSQLserverRejectedEstablishmentOfSQLconnectionError = "08004"
	CodeTransactionResolutionUnknownError                  = "08007"
	CodeProtocolViolationError                             = "08P01"
	// Class 09 - Triggered Action Exception
	CodeTriggeredActionExceptionError = "09000"
	// Class 0A - Feature Not Supported
	CodeFeatureNotSupportedError = "0A000"
	// Class 0B - Invalid Transaction Initiation
	CodeInvalidTransactionInitiationError = "0B000"
	// Class 0F - Locator Exception
	CodeLocatorExceptionError            = "0F000"
	CodeInvalidLocatorSpecificationError = "0F001"
	// Class 0L - Invalid Grantor
	CodeInvalidGrantorError        = "0L000"
	CodeInvalidGrantOperationError = "0LP01"
	// Class 0P - Invalid Role Specification
	CodeInvalidRoleSpecificationError = "0P000"
	// Class 0Z - Diagnostics Exception
	CodeDiagnosticsExceptionError                           = "0Z000"
	CodeStackedDiagnosticsAccessedWithoutActiveHandlerError = "0Z002"
	// Class 20 - Case Not Found
	CodeCaseNotFoundError = "20000"
	// Class 21 - Cardinality Violation
	CodeCardinalityViolationError = "21000"
	// Class 22 - Data Exception
	CodeDataExceptionError                         = "22000"
	CodeArraySubscriptError                        = "2202E"
	CodeCharacterNotInRepertoireError              = "22021"
	CodeDatetimeFieldOverflowError                 = "22008"
	CodeDivisionByZeroError                        = "22012"
	CodeInvalidWindowFrameOffsetError              = "22013"
	CodeErrorInAssignmentError                     = "22005"
	CodeEscapeCharacterConflictError               = "2200B"
	CodeIndicatorOverflowError                     = "22022"
	CodeIntervalFieldOverflowError                 = "22015"
	CodeInvalidArgumentForLogarithmError           = "2201E"
	CodeInvalidArgumentForNtileFunctionError       = "22014"
	CodeInvalidArgumentForNthValueFunctionError    = "22016"
	CodeInvalidArgumentForPowerFunctionError       = "2201F"
	CodeInvalidArgumentForWidthBucketFunctionError = "2201G"
	CodeInvalidCharacterValueForCastError          = "22018"
	CodeInvalidDatetimeFormatError                 = "22007"
	CodeInvalidEscapeCharacterError                = "22019"
	CodeInvalidEscapeOctetError                    = "2200D"
	CodeInvalidEscapeSequenceError                 = "22025"
	CodeNonstandardUseOfEscapeCharacterError       = "22P06"
	CodeInvalidIndicatorParameterValueError        = "22010"
	CodeInvalidParameterValueError                 = "22023"
	CodeInvalidRegularExpressionError              = "2201B"
	CodeInvalidRowCountInLimitClauseError          = "2201W"
	CodeInvalidRowCountInResultOffsetClauseError   = "2201X"
	CodeInvalidTimeZoneDisplacementValueError      = "22009"
	CodeInvalidUseOfEscapeCharacterError           = "2200C"
	CodeMostSpecificTypeMismatchError              = "2200G"
	CodeNullValueNotAllowedError                   = "22004"
	CodeNullValueNoIndicatorParameterError         = "22002"
	CodeNumericValueOutOfRangeError                = "22003"
	CodeSequenceGeneratorLimitExceeded             = "2200H"
	CodeStringDataLengthMismatchError              = "22026"
	CodeStringDataRightTruncationError             = "22001"
	CodeSubstringError                             = "22011"
	CodeTrimError                                  = "22027"
	CodeUnterminatedCStringError                   = "22024"
	CodeZeroLengthCharacterStringError             = "2200F"
	CodeFloatingPointExceptionError                = "22P01"
	CodeInvalidTextRepresentationError             = "22P02"
	CodeInvalidBinaryRepresentationError           = "22P03"
	CodeBadCopyFileFormatError                     = "22P04"
	CodeUntranslatableCharacterError               = "22P05"
	CodeNotAnXMLDocumentError                      = "2200L"
	CodeInvalidXMLDocumentError                    = "2200M"
	CodeInvalidXMLContentError                     = "2200N"
	CodeInvalidXMLCommentError                     = "2200S"
	CodeInvalidXMLProcessingInstructionError       = "2200T"
	// Class 23 - Integrity Constraint Violation
	CodeIntegrityConstraintViolationError = "23000"
	CodeRestrictViolationError            = "23001"
	CodeNotNullViolationError             = "23502"
	CodeForeignKeyViolationError          = "23503"
	CodeUniqueViolationError              = "23505"
	CodeCheckViolationError               = "23514"
	CodeExclusionViolationError           = "23P01"
	// Class 24 - Invalid Cursor State
	CodeInvalidCursorStateError = "24000"
	// Class 25 - Invalid Transaction State
	CodeInvalidTransactionStateError                         = "25000"
	CodeActiveSQLTransactionError                            = "25001"
	CodeBranchTransactionAlreadyActiveError                  = "25002"
	CodeHeldCursorRequiresSameIsolationLevelError            = "25008"
	CodeInappropriateAccessModeForBranchTransactionError     = "25003"
	CodeInappropriateIsolationLevelForBranchTransactionError = "25004"
	CodeNoActiveSQLTransactionForBranchTransactionError      = "25005"
	CodeReadOnlySQLTransactionError                          = "25006"
	CodeSchemaAndDataStatementMixingNotSupportedError        = "25007"
	CodeNoActiveSQLTransactionError                          = "25P01"
	CodeInFailedSQLTransactionError                          = "25P02"
	// Class 26 - Invalid SQL Statement Name
	CodeInvalidSQLStatementNameError = "26000"
	// Class 27 - Triggered Data Change Violation
	CodeTriggeredDataChangeViolationError = "27000"
	// Class 28 - Invalid Authorization Specification
	CodeInvalidAuthorizationSpecificationError = "28000"
	CodeInvalidPasswordError                   = "28P01"
	// Class 2B - Dependent Privilege Descriptors Still Exist
	CodeDependentPrivilegeDescriptorsStillExistError = "2B000"
	CodeDependentObjectsStillExistError              = "2BP01"
	// Class 2D - Invalid Transaction Termination
	CodeInvalidTransactionTerminationError = "2D000"
	// Class 2F - SQL Routine Exception
	CodeSQLRoutineExceptionError                               = "2F000"
	CodeRoutineExceptionFunctionExecutedNoReturnStatementError = "2F005"
	CodeRoutineExceptionModifyingSQLDataNotPermittedError      = "2F002"
	CodeRoutineExceptionProhibitedSQLStatementAttemptedError   = "2F003"
	CodeRoutineExceptionReadingSQLDataNotPermittedError        = "2F004"
	// Class 34 - Invalid Cursor Name
	CodeInvalidCursorNameError = "34000"
	// Class 38 - External Routine Exception
	CodeExternalRoutineExceptionError                       = "38000"
	CodeExternalRoutineContainingSQLNotPermittedError       = "38001"
	CodeExternalRoutineModifyingSQLDataNotPermittedError    = "38002"
	CodeExternalRoutineProhibitedSQLStatementAttemptedError = "38003"
	CodeExternalRoutineReadingSQLDataNotPermittedError      = "38004"
	// Class 39 - External Routine Invocation Exception
	CodeExternalRoutineInvocationExceptionError     = "39000"
	CodeExternalRoutineInvalidSQLstateReturnedError = "39001"
	CodeExternalRoutineNullValueNotAllowedError     = "39004"
	CodeExternalRoutineTriggerProtocolViolatedError = "39P01"
	CodeExternalRoutineSrfProtocolViolatedError     = "39P02"
	// Class 3B - Savepoint Exception
	CodeSavepointExceptionError            = "3B000"
	CodeInvalidSavepointSpecificationError = "3B001"
	// Class 3D - Invalid Catalog Name
	CodeInvalidCatalogNameError = "3D000"
	// Class 3F - Invalid Schema Name
	CodeInvalidSchemaNameError = "3F000"
	// Class 40 - Transaction Rollback
	CodeTransactionRollbackError                     = "40000"
	CodeTransactionIntegrityConstraintViolationError = "40002"
	CodeSerializationFailureError                    = "40001"
	CodeStatementCompletionUnknownError              = "40003"
	CodeDeadlockDetectedError                        = "40P01"
	// Class 42 - Syntax Error or Access Rule Violation
	CodeSyntaxErrorOrAccessRuleViolationError   = "42000"
	CodeSyntaxError                             = "42601"
	CodeInsufficientPrivilegeError              = "42501"
	CodeCannotCoerceError                       = "42846"
	CodeGroupingError                           = "42803"
	CodeWindowingError                          = "42P20"
	CodeInvalidRecursionError                   = "42P19"
	CodeInvalidForeignKeyError                  = "42830"
	CodeInvalidNameError                        = "42602"
	CodeNameTooLongError                        = "42622"
	CodeReservedNameError                       = "42939"
	CodeDatatypeMismatchError                   = "42804"
	CodeIndeterminateDatatypeError              = "42P18"
	CodeCollationMismatchError                  = "42P21"
	CodeIndeterminateCollationError             = "42P22"
	CodeWrongObjectTypeError                    = "42809"
	CodeUndefinedColumnError                    = "42703"
	CodeUndefinedFunctionError                  = "42883"
	CodeUndefinedTableError                     = "42P01"
	CodeUndefinedParameterError                 = "42P02"
	CodeUndefinedObjectError                    = "42704"
	CodeDuplicateColumnError                    = "42701"
	CodeDuplicateCursorError                    = "42P03"
	CodeDuplicateDatabaseError                  = "42P04"
	CodeDuplicateFunctionError                  = "42723"
	CodeDuplicatePreparedStatementError         = "42P05"
	CodeDuplicateSchemaError                    = "42P06"
	CodeDuplicateRelationError                  = "42P07"
	CodeDuplicateAliasError                     = "42712"
	CodeDuplicateObjectError                    = "42710"
	CodeAmbiguousColumnError                    = "42702"
	CodeAmbiguousFunctionError                  = "42725"
	CodeAmbiguousParameterError                 = "42P08"
	CodeAmbiguousAliasError                     = "42P09"
	CodeInvalidColumnReferenceError             = "42P10"
	CodeInvalidColumnDefinitionError            = "42611"
	CodeInvalidCursorDefinitionError            = "42P11"
	CodeInvalidDatabaseDefinitionError          = "42P12"
	CodeInvalidFunctionDefinitionError          = "42P13"
	CodeInvalidPreparedStatementDefinitionError = "42P14"
	CodeInvalidSchemaDefinitionError            = "42P15"
	CodeInvalidTableDefinitionError             = "42P16"
	CodeInvalidObjectDefinitionError            = "42P17"
	// Class 44 - WITH CHECK OPTION Violation
	CodeWithCheckOptionViolationError = "44000"
	// Class 53 - Insufficient Resources
	CodeInsufficientResourcesError      = "53000"
	CodeDiskFullError                   = "53100"
	CodeOutOfMemoryError                = "53200"
	CodeTooManyConnectionsError         = "53300"
	CodeConfigurationLimitExceededError = "53400"
	// Class 54 - Program Limit Exceeded
	CodeProgramLimitExceededError = "54000"
	CodeStatementTooComplexError  = "54001"
	CodeTooManyColumnsError       = "54011"
	CodeTooManyArgumentsError     = "54023"
	// Class 55 - Object Not In Prerequisite State
	CodeObjectNotInPrerequisiteStateError = "55000"
	CodeObjectInUseError                  = "55006"
	CodeCantChangeRuntimeParamError       = "55P02"
	CodeLockNotAvailableError             = "55P03"
	// Class 57 - Operator Intervention
	CodeOperatorInterventionError = "57000"
	CodeQueryCanceledError        = "57014"
	CodeAdminShutdownError        = "57P01"
	CodeCrashShutdownError        = "57P02"
	CodeCannotConnectNowError     = "57P03"
	CodeDatabaseDroppedError      = "57P04"
	// Class 58 - System Error (errors external to PostgreSQL itself)
	CodeSystemError        = "58000"
	CodeIoError            = "58030"
	CodeUndefinedFileError = "58P01"
	CodeDuplicateFileError = "58P02"
	// Class F0 - Configuration File Error
	CodeConfigFileError     = "F0000"
	CodeLockFileExistsError = "F0001"
	// Class HV - Foreign Data Wrapper Error (SQL/MED)
	CodeFdwError                                  = "HV000"
	CodeFdwColumnNameNotFoundError                = "HV005"
	CodeFdwDynamicParameterValueNeededError       = "HV002"
	CodeFdwFunctionSequenceError                  = "HV010"
	CodeFdwInconsistentDescriptorInformationError = "HV021"
	CodeFdwInvalidAttributeValueError             = "HV024"
	CodeFdwInvalidColumnNameError                 = "HV007"
	CodeFdwInvalidColumnNumberError               = "HV008"
	CodeFdwInvalidDataTypeError                   = "HV004"
	CodeFdwInvalidDataTypeDescriptorsError        = "HV006"
	CodeFdwInvalidDescriptorFieldIdentifierError  = "HV091"
	CodeFdwInvalidHandleError                     = "HV00B"
	CodeFdwInvalidOptionIndexError                = "HV00C"
	CodeFdwInvalidOptionNameError                 = "HV00D"
	CodeFdwInvalidStringLengthOrBufferLengthError = "HV090"
	CodeFdwInvalidStringFormatError               = "HV00A"
	CodeFdwInvalidUseOfNullPointerError           = "HV009"
	CodeFdwTooManyHandlesError                    = "HV014"
	CodeFdwOutOfMemoryError                       = "HV001"
	CodeFdwNoSchemasError                         = "HV00P"
	CodeFdwOptionNameNotFoundError                = "HV00J"
	CodeFdwReplyHandleError                       = "HV00K"
	CodeFdwSchemaNotFoundError                    = "HV00Q"
	CodeFdwTableNotFoundError                     = "HV00R"
	CodeFdwUnableToCreateExecutionError           = "HV00L"
	CodeFdwUnableToCreateReplyError               = "HV00M"
	CodeFdwUnableToEstablishConnectionError       = "HV00N"
	// Class P0 - PL/pgSQL Error
	CodePLpgSQLError        = "P0000"
	CodeRaiseExceptionError = "P0001"
	CodeNoDataFoundError    = "P0002"
	CodeTooManyRowsError    = "P0003"
	// Class XX - Internal Error
	CodeInternalError       = "XX000"
	CodeDataCorruptedError  = "XX001"
	CodeIndexCorruptedError = "XX002"
)
