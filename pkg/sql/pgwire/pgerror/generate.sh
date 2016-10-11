#!/bin/sh

set -eu

# This script will generate a list of error codes. It will not perform
# any cleanup, so some manual post-processing may be necessary for
# duplicate 'Error' strings, capitalizing initialisms and acronyms,
# and fixing Golint errors.
cat errcodes.txt   |
    sed "/^\s*$/d" |
    sed "/^#.*$/d" |
    sed -E "s|^(Section.*)$|// \1|" |
    sed -E "s|^([A-Z0-9]{5})    .    ERRCODE_([A-Z_]+).*$|CODE_\2_ERROR = \"\1\"|" |
    awk '{$1=tolower($1); print $0}' |
    perl -pe 's/(^|_)./uc($&)/ge;s/_//g' > errcodes.generated
