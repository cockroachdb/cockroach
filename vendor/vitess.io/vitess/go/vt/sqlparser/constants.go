/*
Copyright 2019 The Vitess Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package sqlparser

const (
	// Select.Distinct
	DistinctStr         = "distinct "
	StraightJoinHint    = "straight_join "
	SQLCalcFoundRowsStr = "sql_calc_found_rows "

	// Select.Lock
	NoLockStr    = ""
	ForUpdateStr = " for update"
	ShareModeStr = " lock in share mode"

	// Select.Cache
	SQLCacheStr   = "sql_cache "
	SQLNoCacheStr = "sql_no_cache "

	// Union.Type
	UnionStr         = "union"
	UnionAllStr      = "union all"
	UnionDistinctStr = "union distinct"

	// DDL strings.
	InsertStr  = "insert"
	ReplaceStr = "replace"

	// Set.Scope or Show.Scope
	SessionStr        = "session"
	GlobalStr         = "global"
	VitessMetadataStr = "vitess_metadata"
	VariableStr       = "variable"
	LocalStr          = "local"
	ImplicitStr       = ""

	// DDL strings.
	CreateStr           = "create"
	AlterStr            = "alter"
	DropStr             = "drop"
	RenameStr           = "rename"
	TruncateStr         = "truncate"
	FlushStr            = "flush"
	CreateVindexStr     = "create vindex"
	DropVindexStr       = "drop vindex"
	AddVschemaTableStr  = "add vschema table"
	DropVschemaTableStr = "drop vschema table"
	AddColVindexStr     = "on table add vindex"
	DropColVindexStr    = "on table drop vindex"
	AddSequenceStr      = "add sequence"
	AddAutoIncStr       = "add auto_increment"

	// Online DDL hint
	OnlineStr = "online"

	// Vindex DDL param to specify the owner of a vindex
	VindexOwnerStr = "owner"

	// Partition strings
	ReorganizeStr = "reorganize partition"

	// JoinTableExpr.Join
	JoinStr             = "join"
	StraightJoinStr     = "straight_join"
	LeftJoinStr         = "left join"
	RightJoinStr        = "right join"
	NaturalJoinStr      = "natural join"
	NaturalLeftJoinStr  = "natural left join"
	NaturalRightJoinStr = "natural right join"

	// Index hints.
	UseStr    = "use "
	IgnoreStr = "ignore "
	ForceStr  = "force "

	// Where.Type
	WhereStr  = "where"
	HavingStr = "having"

	// ComparisonExpr.Operator
	EqualStr         = "="
	LessThanStr      = "<"
	GreaterThanStr   = ">"
	LessEqualStr     = "<="
	GreaterEqualStr  = ">="
	NotEqualStr      = "!="
	NullSafeEqualStr = "<=>"
	InStr            = "in"
	NotInStr         = "not in"
	LikeStr          = "like"
	NotLikeStr       = "not like"
	RegexpStr        = "regexp"
	NotRegexpStr     = "not regexp"

	// RangeCond.Operator
	BetweenStr    = "between"
	NotBetweenStr = "not between"

	// IsExpr.Operator
	IsNullStr     = "is null"
	IsNotNullStr  = "is not null"
	IsTrueStr     = "is true"
	IsNotTrueStr  = "is not true"
	IsFalseStr    = "is false"
	IsNotFalseStr = "is not false"

	// BinaryExpr.Operator
	BitAndStr               = "&"
	BitOrStr                = "|"
	BitXorStr               = "^"
	PlusStr                 = "+"
	MinusStr                = "-"
	MultStr                 = "*"
	DivStr                  = "/"
	IntDivStr               = "div"
	ModStr                  = "%"
	ShiftLeftStr            = "<<"
	ShiftRightStr           = ">>"
	JSONExtractOpStr        = "->"
	JSONUnquoteExtractOpStr = "->>"

	// UnaryExpr.Operator
	UPlusStr   = "+"
	UMinusStr  = "-"
	TildaStr   = "~"
	BangStr    = "!"
	BinaryStr  = "binary "
	UBinaryStr = "_binary "
	Utf8mb4Str = "_utf8mb4 "
	Utf8Str    = "_utf8 "
	Latin1Str  = "_latin1 "

	// ConvertType.Operator
	CharacterSetStr = " character set"
	NoOperatorStr   = ""
	CharsetStr      = "charset"

	// MatchExpr.Option
	NoOptionStr                              = ""
	BooleanModeStr                           = " in boolean mode"
	NaturalLanguageModeStr                   = " in natural language mode"
	NaturalLanguageModeWithQueryExpansionStr = " in natural language mode with query expansion"
	QueryExpansionStr                        = " with query expansion"

	// Order.Direction
	AscScr  = "asc"
	DescScr = "desc"

	// SetExpr.Expr, for SET TRANSACTION ... or START TRANSACTION
	// TransactionStr is the Name for a SET TRANSACTION statement
	TransactionStr = "transaction"

	// Transaction isolation levels
	ReadUncommittedStr = "read uncommitted"
	ReadCommittedStr   = "read committed"
	RepeatableReadStr  = "repeatable read"
	SerializableStr    = "serializable"

	TxReadOnly  = "read only"
	TxReadWrite = "read write"

	// Explain formats
	EmptyStr       = ""
	TreeStr        = "tree"
	JSONStr        = "json"
	VitessStr      = "vitess"
	TraditionalStr = "traditional"
	AnalyzeStr     = "analyze"
)

// Constants for Enum type - AccessMode
const (
	ReadOnly AccessMode = iota
	ReadWrite
)

//Constants for Enum type - IsolationLevel
const (
	ReadUncommitted IsolationLevel = iota
	ReadCommitted
	RepeatableRead
	Serializable
)

// Constants for Union.Type
const (
	UnionBasic UnionType = iota
	UnionAll
	UnionDistinct
)

// Constants for Enum Type - Insert.Action
const (
	InsertAct InsertAction = iota
	ReplaceAct
)

// Constants for Enum Type - DBDDL.Action
const (
	CreateDBDDLAction DBDDLAction = iota
	AlterDBDDLAction
	DropDBDDLAction
)

// Constants for Enum Type - DDL.Action
const (
	CreateDDLAction DDLAction = iota
	AlterDDLAction
	DropDDLAction
	RenameDDLAction
	TruncateDDLAction
	FlushDDLAction
	CreateVindexDDLAction
	DropVindexDDLAction
	AddVschemaTableDDLAction
	DropVschemaTableDDLAction
	AddColVindexDDLAction
	DropColVindexDDLAction
	AddSequenceDDLAction
	AddAutoIncDDLAction
)

// Constants for Enum Type - Scope
const (
	ImplicitScope Scope = iota
	SessionScope
	GlobalScope
	VitessMetadataScope
	VariableScope
	LocalScope
)

// Constants for Enum Type - Lock
const (
	NoLock Lock = iota
	ForUpdateLock
	ShareModeLock
)

// Constants for Enum Type - WhereType
const (
	WhereClause WhereType = iota
	HavingClause
)

// Constants for Enum Type - JoinType
const (
	NormalJoinType JoinType = iota
	StraightJoinType
	LeftJoinType
	RightJoinType
	NaturalJoinType
	NaturalLeftJoinType
	NaturalRightJoinType
)

// Constants for Enum Type - ComparisonExprOperator
const (
	EqualOp ComparisonExprOperator = iota
	LessThanOp
	GreaterThanOp
	LessEqualOp
	GreaterEqualOp
	NotEqualOp
	NullSafeEqualOp
	InOp
	NotInOp
	LikeOp
	NotLikeOp
	RegexpOp
	NotRegexpOp
)

// Constant for Enum Type - RangeCondOperator
const (
	BetweenOp RangeCondOperator = iota
	NotBetweenOp
)

// Constant for Enum Type - IsExprOperator
const (
	IsNullOp IsExprOperator = iota
	IsNotNullOp
	IsTrueOp
	IsNotTrueOp
	IsFalseOp
	IsNotFalseOp
)

// Constant for Enum Type - BinaryExprOperator
const (
	BitAndOp BinaryExprOperator = iota
	BitOrOp
	BitXorOp
	PlusOp
	MinusOp
	MultOp
	DivOp
	IntDivOp
	ModOp
	ShiftLeftOp
	ShiftRightOp
	JSONExtractOp
	JSONUnquoteExtractOp
)

// Constant for Enum Type - UnaryExprOperator
const (
	UPlusOp UnaryExprOperator = iota
	UMinusOp
	TildaOp
	BangOp
	BinaryOp
	UBinaryOp
	Utf8mb4Op
	Utf8Op
	Latin1Op
)

// Constant for Enum Type - MatchExprOption
const (
	NoOption MatchExprOption = iota
	BooleanModeOpt
	NaturalLanguageModeOpt
	NaturalLanguageModeWithQueryExpansionOpt
	QueryExpansionOpt
)

// Constant for Enum Type - OrderDirection
const (
	AscOrder OrderDirection = iota
	DescOrder
)

// Constant for Enum Type - ConvertTypeOperator
const (
	NoOperator ConvertTypeOperator = iota
	CharacterSetOp
)

// Constant for Enum Type - IndexHintsType
const (
	UseOp IndexHintsType = iota
	IgnoreOp
	ForceOp
)

// Constant for Enum Type - PartitionSpecAction
const (
	ReorganizeAction PartitionSpecAction = iota
)

// Constant for Enum Type - ExplainType
const (
	EmptyType ExplainType = iota
	TreeType
	JSONType
	VitessType
	TraditionalType
	AnalyzeType
)
