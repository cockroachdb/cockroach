#!/bin/bash

# Verify arcanist is installed.
command -v arc &> /dev/null
if [ $? -eq 1 ]; then
  cat <<EOF
Please install Arcanist (part of Phabricator):

  http://www.phabricator.com/docs/phabricator/article/Arcanist_User_Guide.html
EOF
  exit 1
fi

GO_GET="go get"
GO_GET_FLAGS="-v"

set -e -x

# Init submodules.
git submodule init
git submodule update

# Grab binaries required by git hooks.
$GO_GET $GO_GET_FLAGS github.com/golang/lint/golint
$GO_GET $GO_GET_FLAGS code.google.com/p/go.tools/cmd/vet
$GO_GET $GO_GET_FLAGS code.google.com/p/go.tools/cmd/goimports

# Grab gogoprotobuf package.
$GO_GET $GO_GET_FLAGS -u code.google.com/p/gogoprotobuf/{proto,protoc-gen-gogo,gogoproto}

# Create symlinks to all git hooks in your own .git dir.
for f in $(ls -d githooks/*); do
  rm .git/hooks/$(basename $f)
  ln -s ../../$f .git/hooks/$(basename $f)
done && ls -al .git/hooks | grep githooks
