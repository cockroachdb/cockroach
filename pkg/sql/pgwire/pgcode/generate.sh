#!/bin/sh

set -eu

# This script will generate a list of error codes. It will not perform
# any cleanup, so some manual post-processing may be necessary for
# duplicate 'Error' strings, capitalizing initialisms and acronyms,
# and fixing Golint errors.
sed '/^\s*$/d' errcodes.txt |
sed '/^#.*$/d' |
sed -E 's|^(Section.*)$|// \1|' |
sed -E 's|^([A-Z0-9]{5})    .    ERRCODE_([A-Z_]+).*$|\2 = "\1"|' |
# Postgres uses class 58 just for external errors, but we've extended it with some errors
# internal to the cluster (inspired by DB2).
sed -E 's|// Section: Class 58 - System Error \(errors external to PostgreSQL itself\)|// Section: Class 58 - System Error|' |
awk '{$1=tolower($1); print $0}' |
perl -pe 's/(^|_)./uc($&)/ge;s/_//g' > errcodes.generated
