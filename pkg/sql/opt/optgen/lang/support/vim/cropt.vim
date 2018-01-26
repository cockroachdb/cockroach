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

syn keyword operator Subquery Variable Const Placeholder List OrderedList Filters Projections Exists
syn keyword operator And Or Not Eq Lt Gt Le Ge Ne In NotIn
syn keyword operator Like NotLike ILike NotILike SimilarTo NotSimilarTo RegMatch NotRegMatch RegIMatch NotRegIMatch
syn keyword operator IsDistinctFrom IsNotDistinctFrom Is IsNot Any Some All
syn keyword operator Bitand Bitor Bitxor Plus Minus Mult Div FloorDiv Mod Pow Concat LShift RShift
syn keyword operator UnaryPlus UnaryMinus UnaryComplement Function True False
syn keyword operator Scan Values Select Project Join InnerJoin LeftJoin RightJoin FullJoin SemiJoin AntiJoin
syn keyword operator JoinApply InnerJoinApply LeftJoinApply RightJoinApply FullJoinApply SemiJoinApply AntiJoinApply GroupBy
syn keyword operator Union Intersect Except Sort Arrange OpName

let b:current_syntax = "cropt"
