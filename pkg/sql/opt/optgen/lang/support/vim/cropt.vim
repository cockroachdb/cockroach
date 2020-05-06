" Vim syntax file
" Language:         Andy's optimizer DSL prototype
" Maintainer:       Radu Berinde <radu@cockroachlabs.com>
"
" To use, copy the file to ~/.vim/syntax/
" Recommended in .vimrc:
"    autocmd BufNewFile,BufRead *.opt set filetype=cropt tw=0

if exists("b:current_syntax")
  finish
endif

syn match Comment display '#.*$' contains=Todo

syn keyword Todo TODO XXX

syn region ruletags start='^\[' end='\]$' contains=ruletag
syn match ruletag contained display '[A-Za-z0-9_]*' 

syn region list start='(' end=')' contains=list,func,var,operator
syn match func contained display '\((\)\@<=[A-Za-z0-9_]\+\( | [A-Za-z0-9_]\+\)*'

syn match var '\$[A-Za-z0-9_]*'

syn match Special '^=>$'

syn region def start='^define [A-Za-z0-9_]\+ {' end='}' contains=define,operator

hi def link ruletag Identifier
hi def link func Type
hi def link var Macro

syn keyword define contained define

syn match func contained '(A-Za-z0-9_)*'

syn keyword operator Subquery SubqueryPrivate Any Exists Variable Const Null True False Placeholder
syn keyword operator Tuple Projections ColPrivate Aggregations AggregationsItem Filters FiltersItem
syn keyword operator Zip ZipItem ZipItemPrivate And Or Not Eq Lt Gt Le Ge Ne In NotIn
syn keyword operator Like NotLike ILike NotILike SimilarTo NotSimilarTo RegMatch NotRegMatch
syn keyword operator RegIMatch NotRegIMatch Is IsNot Contains JsonExists JsonAllExists
syn keyword operator JsonSomeExists AnyScalar Bitand Bitor Bitxor Plus Minus Mult Div FloorDiv
syn keyword operator Mod Pow Concat LShift RShift FetchVal FetchText FetchValPath FetchTextPath
syn keyword operator UnaryMinus UnaryComplement Cast Case When Array Indirection
syn keyword operator Function FunctionPrivate Coalesce ColumnAccess UnsupportedExpr
syn keyword operator Avg BoolAnd BoolOr ConcatAgg Count CountRows Max Min SumInt Sum SqrDiff
syn keyword operator Variance StdDev XorAgg JsonAgg JsonbAgg ConstAgg ConstNotNullAgg
syn keyword operator AnyNotNullAgg FirstAgg AggDistinct ScalarList
syn keyword operator Scan ScanPrivate VirtualScan VirtualScanPrivate Values Select Project
syn keyword operator InnerJoin LeftJoin RightJoin FullJoin SemiJoin AntiJoin
syn keyword operator IndexJoin IndexJoinPrivate LookupJoin LookupJoinPrivate
syn keyword operator MergeJoin MergeJoinPrivate
syn keyword operator InnerJoinApply LeftJoinApply
syn keyword operator SemiJoinApply AntiJoinApply
syn keyword operator GroupBy GroupingPrivate ScalarGroupBy
syn keyword operator DistinctOn EnsureDistinctOn UpsertDistinctOn EnsureUpsertDistinctOn
syn keyword operator Union SetPrivate Intersect Except UnionAll IntersectAll ExceptAll
syn keyword operator Limit Offset Max1Row Explain ExplainPrivate
syn keyword operator ShowTraceForSession ShowTracePrivate RowNumber RowNumberPrivate ProjectSet
syn keyword operator Sort Insert Update Upsert Delete CreateTable OpName

let b:current_syntax = "cropt"
