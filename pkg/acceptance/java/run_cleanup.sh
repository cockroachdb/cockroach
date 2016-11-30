#!/usr/bin/env bash

# cleans all {.class, .pk8} files in the current directory
find . -type f -name "*.class" -exec rm -f {} \;
find . -type f -name "*.pk8" -exec rm -f {} \;
