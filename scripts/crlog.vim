" Vim syntax file
" Language:         CockroachDB logs
" Maintainer:       Radu Berinde <radu@cockroachlabs.com>

if exists("b:current_syntax")
  finish
endif

syn match   messageBeginI       display '^I' nextgroup=messageDate
syn match   messageBeginW       display '^W' nextgroup=messageDate
syn match   messageBeginE       display '^E' nextgroup=messageDate
syn match   messageBeginF       display '^F' nextgroup=messageDate

syn match   messageDate         contained display '\d\d\d\d\d\d ' nextgroup=messageTime

syn match   messageTime         contained display '\d\d:\d\d:\d\d.\d* ' nextgroup=messageGoID

syn match   messageGoID         contained display '[0-9a-fA-F]* ' nextgroup=messageFile

syn match   messageFile         contained display '[_./0-9a-zA-Z]*:\d*  ' nextgroup=messageTags

syn match   messageTags         contained display '\[[^\]]*\] '

syn match   messageNumber       '0x[0-9a-fA-F]*\|\[<[0-9a-f]\+>\]\|\<\d[0-9a-fA-F]*'

hi def link messageBeginI Identifier
hi def link messageBeginW Special
hi def link messageBeginE ErrorMsg
hi def link messageBeginF ErrorMsg
hi def link messageDate   Constant
hi def link messageTime   Type
hi def link messageFile   Comment
hi def link messageTags   Keyword
hi def link messageGoID   Keyword
hi def link messageNumber Number


syn match   gotestRun           display '^=== RUN .*$'
syn match   gotestPass          display '^--- PASS: .*$'
syn match   gotestFail          display '^--- FAIL: .*$'
syn match   gotestFail          display '^WARNING: DATA RACE'

hi def link gotestRun  Keyword
hi def link gotestPass Type
hi def link gotestFail ErrorMsg

let b:current_syntax = "crlog"
