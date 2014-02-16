#!/bin/bash

# Verify "git review" is installed.
if [ ! -x /usr/local/bin/git-review ]; then
  cat <<EOF
Please install git-review:

  sudo pip install git-review
EOF
  exit 1
fi

# Verify "git config gitreview.username" is set.
if [ -z "$(git config gitreview.username)" ]; then
  cat <<EOF
Please set gitreview username:

  git config --add gitreview.username <username>
EOF
  exit 1
fi

set -e -x

# Grab binaries required by git hooks.
go get github.com/golang/lint/golint
go get code.google.com/p/go.tools/cmd/vet
go get code.google.com/p/go.tools/cmd/goimports

# Create symlinks to all git hooks in your own .git dir.
for f in $(ls -d githooks/*); do
  rm .git/hooks/$(basename $f)
  ln -s ../../$f .git/hooks/$(basename $f)
done && ls -al .git/hooks | grep githooks
