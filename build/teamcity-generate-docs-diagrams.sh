#!/usr/bin/env bash

set -euxo pipefail

make bin/docgen

ls

ls bin

bin/docgen grammar bnf bnfs

bin/docgen grammar svg bnfs grammar_svgs

#bin/docgen grammar bnf bnfs
#
#bin/docgen grammar svg bnfs grammar_svgs