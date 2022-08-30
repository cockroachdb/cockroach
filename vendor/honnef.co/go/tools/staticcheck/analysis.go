package staticcheck

import (
	"honnef.co/go/tools/analysis/facts/deprecated"
	"honnef.co/go/tools/analysis/facts/generated"
	"honnef.co/go/tools/analysis/facts/nilness"
	"honnef.co/go/tools/analysis/facts/purity"
	"honnef.co/go/tools/analysis/facts/tokenfile"
	"honnef.co/go/tools/analysis/facts/typedness"
	"honnef.co/go/tools/analysis/lint"
	"honnef.co/go/tools/internal/passes/buildir"

	"golang.org/x/tools/go/analysis"
	"golang.org/x/tools/go/analysis/passes/inspect"
)

func makeCallCheckerAnalyzer(rules map[string]CallCheck, extraReqs ...*analysis.Analyzer) *analysis.Analyzer {
	reqs := []*analysis.Analyzer{buildir.Analyzer, tokenfile.Analyzer}
	reqs = append(reqs, extraReqs...)
	return &analysis.Analyzer{
		Run:      callChecker(rules),
		Requires: reqs,
	}
}

var Analyzers = lint.InitializeAnalyzers(Docs, map[string]*analysis.Analyzer{
	"SA1000": makeCallCheckerAnalyzer(checkRegexpRules),
	"SA1001": {
		Run:      CheckTemplate,
		Requires: []*analysis.Analyzer{inspect.Analyzer},
	},
	"SA1002": makeCallCheckerAnalyzer(checkTimeParseRules),
	"SA1003": makeCallCheckerAnalyzer(checkEncodingBinaryRules),
	"SA1004": {
		Run:      CheckTimeSleepConstant,
		Requires: []*analysis.Analyzer{inspect.Analyzer},
	},
	"SA1005": {
		Run:      CheckExec,
		Requires: []*analysis.Analyzer{inspect.Analyzer},
	},
	"SA1006": {
		Run:      CheckUnsafePrintf,
		Requires: []*analysis.Analyzer{inspect.Analyzer},
	},
	"SA1007": makeCallCheckerAnalyzer(checkURLsRules),
	"SA1008": {
		Run:      CheckCanonicalHeaderKey,
		Requires: []*analysis.Analyzer{inspect.Analyzer},
	},
	"SA1010": makeCallCheckerAnalyzer(checkRegexpFindAllRules),
	"SA1011": makeCallCheckerAnalyzer(checkUTF8CutsetRules),
	"SA1012": {
		Run:      CheckNilContext,
		Requires: []*analysis.Analyzer{inspect.Analyzer},
	},
	"SA1013": {
		Run:      CheckSeeker,
		Requires: []*analysis.Analyzer{inspect.Analyzer},
	},
	"SA1014": makeCallCheckerAnalyzer(checkUnmarshalPointerRules),
	"SA1015": {
		Run:      CheckLeakyTimeTick,
		Requires: []*analysis.Analyzer{buildir.Analyzer},
	},
	"SA1016": {
		Run:      CheckUntrappableSignal,
		Requires: []*analysis.Analyzer{inspect.Analyzer},
	},
	"SA1017": makeCallCheckerAnalyzer(checkUnbufferedSignalChanRules),
	"SA1018": makeCallCheckerAnalyzer(checkStringsReplaceZeroRules),
	"SA1019": {
		Run:      CheckDeprecated,
		Requires: []*analysis.Analyzer{inspect.Analyzer, deprecated.Analyzer, generated.Analyzer},
	},
	"SA1020": makeCallCheckerAnalyzer(checkListenAddressRules),
	"SA1021": makeCallCheckerAnalyzer(checkBytesEqualIPRules),
	"SA1023": {
		Run:      CheckWriterBufferModified,
		Requires: []*analysis.Analyzer{buildir.Analyzer},
	},
	"SA1024": makeCallCheckerAnalyzer(checkUniqueCutsetRules),
	"SA1025": {
		Run:      CheckTimerResetReturnValue,
		Requires: []*analysis.Analyzer{buildir.Analyzer},
	},
	"SA1026": makeCallCheckerAnalyzer(checkUnsupportedMarshal),
	"SA1027": makeCallCheckerAnalyzer(checkAtomicAlignment),
	"SA1028": makeCallCheckerAnalyzer(checkSortSliceRules),
	"SA1029": makeCallCheckerAnalyzer(checkWithValueKeyRules),
	"SA1030": makeCallCheckerAnalyzer(checkStrconvRules),

	"SA2000": {
		Run:      CheckWaitgroupAdd,
		Requires: []*analysis.Analyzer{inspect.Analyzer},
	},
	"SA2001": {
		Run:      CheckEmptyCriticalSection,
		Requires: []*analysis.Analyzer{inspect.Analyzer},
	},
	"SA2002": {
		Run:      CheckConcurrentTesting,
		Requires: []*analysis.Analyzer{buildir.Analyzer},
	},
	"SA2003": {
		Run:      CheckDeferLock,
		Requires: []*analysis.Analyzer{buildir.Analyzer},
	},

	"SA3000": {
		Run:      CheckTestMainExit,
		Requires: []*analysis.Analyzer{inspect.Analyzer},
	},
	"SA3001": {
		Run:      CheckBenchmarkN,
		Requires: []*analysis.Analyzer{inspect.Analyzer},
	},

	"SA4000": {
		Run:      CheckLhsRhsIdentical,
		Requires: []*analysis.Analyzer{inspect.Analyzer, tokenfile.Analyzer, generated.Analyzer},
	},
	"SA4001": {
		Run:      CheckIneffectiveCopy,
		Requires: []*analysis.Analyzer{inspect.Analyzer},
	},
	"SA4003": {
		Run:      CheckExtremeComparison,
		Requires: []*analysis.Analyzer{inspect.Analyzer},
	},
	"SA4004": {
		Run:      CheckIneffectiveLoop,
		Requires: []*analysis.Analyzer{inspect.Analyzer},
	},
	"SA4005": {
		Run:      CheckIneffectiveFieldAssignments,
		Requires: []*analysis.Analyzer{buildir.Analyzer},
	},
	"SA4006": {
		Run:      CheckUnreadVariableValues,
		Requires: []*analysis.Analyzer{buildir.Analyzer, generated.Analyzer},
	},
	"SA4008": {
		Run:      CheckLoopCondition,
		Requires: []*analysis.Analyzer{buildir.Analyzer},
	},
	"SA4009": {
		Run:      CheckArgOverwritten,
		Requires: []*analysis.Analyzer{buildir.Analyzer},
	},
	"SA4010": {
		Run:      CheckIneffectiveAppend,
		Requires: []*analysis.Analyzer{buildir.Analyzer},
	},
	"SA4011": {
		Run:      CheckScopedBreak,
		Requires: []*analysis.Analyzer{inspect.Analyzer},
	},
	"SA4012": {
		Run:      CheckNaNComparison,
		Requires: []*analysis.Analyzer{buildir.Analyzer},
	},
	"SA4013": {
		Run:      CheckDoubleNegation,
		Requires: []*analysis.Analyzer{inspect.Analyzer},
	},
	"SA4014": {
		Run:      CheckRepeatedIfElse,
		Requires: []*analysis.Analyzer{inspect.Analyzer},
	},
	"SA4015": makeCallCheckerAnalyzer(checkMathIntRules),
	"SA4016": {
		Run:      CheckSillyBitwiseOps,
		Requires: []*analysis.Analyzer{inspect.Analyzer, tokenfile.Analyzer},
	},
	"SA4017": {
		Run:      CheckPureFunctions,
		Requires: []*analysis.Analyzer{buildir.Analyzer, purity.Analyzer},
	},
	"SA4018": {
		Run:      CheckSelfAssignment,
		Requires: []*analysis.Analyzer{inspect.Analyzer, generated.Analyzer, tokenfile.Analyzer, purity.Analyzer},
	},
	"SA4019": {
		Run:      CheckDuplicateBuildConstraints,
		Requires: []*analysis.Analyzer{generated.Analyzer},
	},
	"SA4020": {
		Run:      CheckUnreachableTypeCases,
		Requires: []*analysis.Analyzer{inspect.Analyzer},
	},
	"SA4021": {
		Run:      CheckSingleArgAppend,
		Requires: []*analysis.Analyzer{inspect.Analyzer, generated.Analyzer, tokenfile.Analyzer},
	},
	"SA4022": {
		Run:      CheckAddressIsNil,
		Requires: []*analysis.Analyzer{inspect.Analyzer},
	},
	"SA4023": {
		Run:      CheckTypedNilInterface,
		Requires: []*analysis.Analyzer{buildir.Analyzer, typedness.Analysis, nilness.Analysis},
	},
	"SA4024": {
		Run:      CheckBuiltinZeroComparison,
		Requires: []*analysis.Analyzer{inspect.Analyzer},
	},
	"SA4025": {
		Run:      CheckIntegerDivisionEqualsZero,
		Requires: []*analysis.Analyzer{inspect.Analyzer},
	},
	"SA4026": {
		Run:      CheckNegativeZeroFloat,
		Requires: []*analysis.Analyzer{inspect.Analyzer},
	},
	"SA4027": {
		Run:      CheckIneffectiveURLQueryModification,
		Requires: []*analysis.Analyzer{inspect.Analyzer},
	},
	"SA4028": {
		Run:      CheckModuloOne,
		Requires: []*analysis.Analyzer{inspect.Analyzer},
	},
	"SA4029": {
		Run:      CheckIneffectiveSort,
		Requires: []*analysis.Analyzer{inspect.Analyzer},
	},
	"SA4030": {
		Run:      CheckIneffectiveRandInt,
		Requires: []*analysis.Analyzer{inspect.Analyzer},
	},
	"SA4031": {
		Run:      CheckAllocationNilCheck,
		Requires: []*analysis.Analyzer{buildir.Analyzer, inspect.Analyzer, tokenfile.Analyzer},
	},

	"SA5000": {
		Run:      CheckNilMaps,
		Requires: []*analysis.Analyzer{buildir.Analyzer},
	},
	"SA5001": {
		Run:      CheckEarlyDefer,
		Requires: []*analysis.Analyzer{inspect.Analyzer},
	},
	"SA5002": {
		Run:      CheckInfiniteEmptyLoop,
		Requires: []*analysis.Analyzer{inspect.Analyzer},
	},
	"SA5003": {
		Run:      CheckDeferInInfiniteLoop,
		Requires: []*analysis.Analyzer{inspect.Analyzer},
	},
	"SA5004": {
		Run:      CheckLoopEmptyDefault,
		Requires: []*analysis.Analyzer{inspect.Analyzer},
	},
	"SA5005": {
		Run:      CheckCyclicFinalizer,
		Requires: []*analysis.Analyzer{buildir.Analyzer},
	},
	"SA5007": {
		Run:      CheckInfiniteRecursion,
		Requires: []*analysis.Analyzer{buildir.Analyzer},
	},
	"SA5008": {
		Run:      CheckStructTags,
		Requires: []*analysis.Analyzer{inspect.Analyzer},
	},
	"SA5009": makeCallCheckerAnalyzer(checkPrintfRules),
	"SA5010": {
		Run:      CheckImpossibleTypeAssertion,
		Requires: []*analysis.Analyzer{buildir.Analyzer, tokenfile.Analyzer},
	},
	"SA5011": {
		Run:      CheckMaybeNil,
		Requires: []*analysis.Analyzer{buildir.Analyzer},
	},
	"SA5012": {
		Run:       CheckEvenSliceLength,
		FactTypes: []analysis.Fact{new(evenElements)},
		Requires:  []*analysis.Analyzer{buildir.Analyzer},
	},

	"SA6000": makeCallCheckerAnalyzer(checkRegexpMatchLoopRules),
	"SA6001": {
		Run:      CheckMapBytesKey,
		Requires: []*analysis.Analyzer{buildir.Analyzer},
	},
	"SA6002": makeCallCheckerAnalyzer(checkSyncPoolValueRules),
	"SA6003": {
		Run:      CheckRangeStringRunes,
		Requires: []*analysis.Analyzer{buildir.Analyzer},
	},
	"SA6005": {
		Run:      CheckToLowerToUpperComparison,
		Requires: []*analysis.Analyzer{inspect.Analyzer},
	},

	"SA9001": {
		Run:      CheckDubiousDeferInChannelRangeLoop,
		Requires: []*analysis.Analyzer{inspect.Analyzer},
	},
	"SA9002": {
		Run:      CheckNonOctalFileMode,
		Requires: []*analysis.Analyzer{inspect.Analyzer},
	},
	"SA9003": {
		Run:      CheckEmptyBranch,
		Requires: []*analysis.Analyzer{buildir.Analyzer, tokenfile.Analyzer, generated.Analyzer},
	},
	"SA9004": {
		Run:      CheckMissingEnumTypesInDeclaration,
		Requires: []*analysis.Analyzer{inspect.Analyzer},
	},
	// Filtering generated code because it may include empty structs generated from data models.
	"SA9005": makeCallCheckerAnalyzer(checkNoopMarshal, generated.Analyzer),
	"SA9006": {
		Run:      CheckStaticBitShift,
		Requires: []*analysis.Analyzer{inspect.Analyzer},
	},
	"SA9007": {
		Run:      CheckBadRemoveAll,
		Requires: []*analysis.Analyzer{buildir.Analyzer},
	},

	"SA9008": {
		Run:      CheckTypeAssertionShadowingElse,
		Requires: []*analysis.Analyzer{inspect.Analyzer, buildir.Analyzer, tokenfile.Analyzer},
	},
})
