#!/bin/sh

# Verify arcanist is installed.

if ! which arc > /dev/null 2>/dev/null; then
  cat <<EOF
Please install Arcanist (part of Phabricator):

  http://www.phabricator.com/docs/phabricator/article/Arcanist_User_Guide.html
EOF
  exit 1
fi

set -e -x

# Init submodules.
git submodule init
git submodule update

# Grab binaries required by git hooks.
go get github.com/golang/lint/golint
go get code.google.com/p/go.tools/cmd/vet
go get code.google.com/p/go.tools/cmd/goimports

# Create symlinks to all git hooks in your own .git dir.
for f in $(find githooks -mindepth 1 -maxdepth 1); do
  rm .git/hooks/$(basename "$f")
  ln -s "../../$f" ".git/hooks/$(basename $f)"
done && find .git/hooks -regex '*githooks*'
