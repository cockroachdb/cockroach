# Copyright 2022 The Cockroach Authors.
#
# Use of this software is governed by the CockroachDB Software License
# included in the /LICENSE file.

import zipfile
import sys

z = zipfile.ZipFile(sys.argv[1])
z.extractall(path=sys.argv[2])
