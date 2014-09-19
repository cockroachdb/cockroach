# Copyright 2012, Google Inc. All rights reserved.
# Use of this source code is governed by a BSD-style license that can
# be found in the LICENSE file.

MAKEFLAGS = -s

sql.go: sql.y
	go tool yacc -o sql.go sql.y
	gofmt -w sql.go

clean:
	rm -f y.output sql.go
