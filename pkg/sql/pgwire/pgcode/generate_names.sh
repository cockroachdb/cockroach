#!/bin/sh

# Copyright 2023 The Cockroach Authors.
#
# Use of this software is governed by the CockroachDB Software License
# included in the /LICENSE file.


set -eu

# This script will generate a mapping from condition names to error codes.
# It will not perform any cleanup, so some manual post-processing may be
# necessary for duplicate 'Error' strings, capitalizing initialisms and
# acronyms, and fixing Golint errors.
sed '/^\s*$/d' errcodes.txt |
sed '/^#.*$/d' |
sed -E 's|^(Section.*)$|// \1|' |
sed -E 's|^([A-Z0-9]{5})    .    ([A-Z_]+)[[:space:]]+([a-z_]+).*$|"\3": {"\1"},|' |
# Postgres uses class 58 just for external errors, but we've extended it with some errors
# internal to the cluster (inspired by DB2).
sed -E 's|// Section: Class 58 - System Error \(errors external to PostgreSQL itself\)|// Section: Class 58 - System Error|' > errcodes.generated
