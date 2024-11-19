# Copyright 2018 The Cockroach Authors.
#
# Use of this software is governed by the CockroachDB Software License
# included in the /LICENSE file.

TESTS := $(wildcard test*.sh)
BASH ?= /usr/bin/env bash
PYTHON ?= python
NOTESCRIPT := $(PWD)/../release-notes.py
TDIR := $(shell pwd)

all: test

check: test

test: $(TESTS:.sh=.test)

.SUFFIXES: .test

%.test: %.sh %.graph.ref.txt %.notes.ref.txt $(NOTESCRIPT)
	@echo
	@echo "**** Testing for $* ****"
	@echo
	$(BASH) $(TDIR)/$*.sh $(NOTESCRIPT)

clean:
	rm -f *.graph.txt *.notes.txt
	rm -rf $(TESTS:.sh=)
