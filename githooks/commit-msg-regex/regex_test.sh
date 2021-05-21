#!/usr/bin/env bash
#
# -u: we want the variables to be properly assigned.
# -o pipefail: we want to test the result of pipes.
set -euo pipefail


# Creating common error file

TEXT='''
Missing one of: issue reference, epic reference or Docs note: section

Try one of these:

  Fixes #12345
  Epic CRDB-12345
  See also: #62585
  Docs note: This change is being made because ...
'''

echo "$TEXT" > common_fail_message.txt 

red=$([ -t 2 ] && { tput setaf 1 || tput AF 1; } 2>/dev/null)
reset=$([ -t 2 ] && { tput sgr0 || tput me; } 2>/dev/null)
warn() {
    echo >&2
    echo "${red}$*${reset}" >&2
    echo >&2
}

green=$([ -t 2 ] && { tput setaf 2 || tput AF 2; } 2>/dev/null)
reset=$([ -t 2 ] && { tput sgr0 || tput me; } 2>/dev/null)
testing() {
    echo >&2
    echo "${green}$*${reset}" >&2
    echo >&2
}

PositiveTest() {
    # $1 Name of the file
    # $2 Name of the test
    # $3 String that you are testing it
    ./../commit-msg $1 > out_test.txt 2>&1
    if [ -s out_test.txt ]; then
    warn "$2 test is failed, files not matching"
    echo "'$3' string is failing"
    PASS="FALSE"
    fi
}

NegativeTest() {
    # $1 Name of the file
    # $2 Name of the test
    # $3 String that you are testing it
    ./../commit-msg $1 > out_test.txt 2>&1
    if ! cmp -s common_fail_message.txt out_test.txt; then
    warn "$2 test is failed, files not matching"
    echo "'$3' is failing"
    PASS="FALSE"
    fi
}

PASS="TRUE"

testing "Testing: 'isdocs'"
# Positive test
PositiveTest isdocs/pass_test.txt "isdocs" "docs note: Passing test"

# Negative test
NegativeTest isdocs/fail_test.txt "isdocs" "FAIL"


testing "Testing: 'isepic'"
# Positive tests
PositiveTest isepic/pass_test1.txt "isdocs" "epic CRDB-31"
PositiveTest isepic/pass_test2.txt "isdocs" "Epic:    CRDB-4810   "

# # Negative tests
NegativeTest isepic/fail_test1.txt "isdocs" "Epic:CRDB-4820323232323232"
NegativeTest isepic/fail_test2.txt "isdocs" "epic:  CRDB-4802asdf"


testing "Testing: 'iskeyphrase1'"
# Positive tests
PositiveTest iskeyphrase1/pass_test1.txt "iskeyphrase1" "See also: #323234"
PositiveTest iskeyphrase1/pass_test2.txt "iskeyphrase1" "see also: #323234"
PositiveTest iskeyphrase1/pass_test3.txt "iskeyphrase1" "See also: #323234, #3333, #434343"
PositiveTest iskeyphrase1/pass_test4.txt "iskeyphrase1" "Informs: #3434343"
PositiveTest iskeyphrase1/pass_test5.txt "iskeyphrase1" "informs: #3434343"
PositiveTest iskeyphrase1/pass_test6.txt "iskeyphrase1" "Informs: #3434343, #434343, #4444"
PositiveTest iskeyphrase1/pass_test7.txt "iskeyphrase1" "informs: #2322, 34343"
PositiveTest iskeyphrase1/pass_test8.txt "iskeyphrase1" "see also: #434343, 3333"

# Negative tests
NegativeTest iskeyphrase1/fail_test1.txt "iskeyphrase1" "see also #3333h"
NegativeTest iskeyphrase1/fail_test2.txt "iskeyphrase1" "informs #333h"


testing "Testing: 'iskeyphrase2'"
# Positive tests
PositiveTest iskeyphrase2/pass_test1.txt "iskeyphrase2" "Informs: test/test#3232"
PositiveTest iskeyphrase2/pass_test2.txt "iskeyphrase2" "informs: test/test#3232, #43433"
PositiveTest iskeyphrase2/pass_test3.txt "iskeyphrase2" "informs: test/test#3232, test1/test1#5454"
PositiveTest iskeyphrase2/pass_test4.txt "iskeyphrase2" "informs:    test/test#3232,    test1/test1#5454"
PositiveTest iskeyphrase2/pass_test5.txt "iskeyphrase2" "see also:    test/test#3232,    test2/test2#5454, test3/test3#5454"

# Negative tests
NegativeTest iskeyphrase2/fail_test4.txt "iskeyphrase2" "informs:test/test#3232,test1/test1#5454"
NegativeTest iskeyphrase2/fail_test5.txt "iskeyphrase2" "informs: test/test"


testing "Testing: 'isissue1'"
# Positive tests
PositiveTest isissue1/pass_test1.txt "isissue1" "Fixes: #434343, #4444"
PositiveTest isissue1/pass_test2.txt "isissue1" "Resolved: #434343, #4444, #43434"
PositiveTest isissue1/pass_test3.txt "isissue1" "close: #434343"
PositiveTest isissue1/pass_test4.txt "isissue1" "fix: #434343, #4444, #4343jj"
PositiveTest isissue1/pass_test5.txt "isissue1" "Resolve: #434343, #4444, eeee"
PositiveTest isissue1/pass_test6.txt "isissue1" "Closes: #434343, #4444,#4343"

# Negative tests
NegativeTest isissue1/fail_test1.txt "isissue1" "Fixes: 434343, #4444"
NegativeTest isissue1/fail_test2.txt "isissue1" "Fixes: #434343-"


testing "Testing: 'isissue2'"
# Positive tests
PositiveTest isissue2/pass_test1.txt "isissue2" "Fixes: test/test#2212"
PositiveTest isissue2/pass_test2.txt "isissue2" "Fixes: test/test#2212, test1/test1#43343"
PositiveTest isissue2/pass_test3.txt "isissue2" "Closed: test/test#2212,test1/test1#4343"
PositiveTest isissue2/pass_test4.txt "isissue2" "Fixed: test/test#2212,test1/test1#4343"
PositiveTest isissue2/pass_test5.txt "isissue2" "Resolves: test/test#32323, #323232"

# Negative tests
NegativeTest isissue2/fail_test1.txt "isissue2" "Fixes: 434343, #4444"
NegativeTest isissue2/fail_test2.txt "isissue2" "Fixes #323232-"

# Removing dummy out file and commit message
rm out_test.txt
rm common_fail_message.txt

# Final review
if [ "$PASS" == "FALSE" ]; then
    warn "Some of the tests are failing, please review the log."
    exit 1
fi
