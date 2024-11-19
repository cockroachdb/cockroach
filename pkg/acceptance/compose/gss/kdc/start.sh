#!/bin/sh

# Copyright 2018 The Cockroach Authors.
#
# Use of this software is governed by the CockroachDB Software License
# included in the /LICENSE file.


set -e

# The /keytab directory is volume mounted on both kdc and cockroach. kdc
# can create the keytab with kadmin.local here and it is then useable
# by cockroach.
kadmin.local -q "ktadd -k /keytab/crdb.keytab postgres/gss_cockroach_1.gss_default@MY.EX"

krb5kdc -n
