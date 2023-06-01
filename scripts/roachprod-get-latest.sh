#!/usr/bin/env bash

#Downloads the latest roachprod binary from teamcity to the specified directory
# or the current directory if none is specified.
# This does NOT use the TeamCity REST API, but rather a direct download link for the latest successful build
# for the current platform.

TC_BASE="https://teamcity.cockroachdb.com/repository/download"
TC_ARTIFACT_PATH="latest.lastSuccessful/bazel-bin/pkg/cmd/roachprod/roachprod_/roachprod"

OS=$(uname | tr '[:upper:]' '[:lower:]')
ARCH=$(arch | tr '[:upper:]' '[:lower:]')

dest_dir=${1:-.}
dest_file="$dest_dir/roachprod"

build_for_plat() {
  plat="$OS-$ARCH"
  if [ "$plat" = "linux-aarch64" ]; then
    echo "Cockroach_UnitTests_BazelBuildLinuxArmCross"
  elif [ "$plat" = "linux-x86_64" ]; then
    echo "Cockroach_Ci_Builds_BuildLinuxX8664"
  elif [ "$plat" = "darwin-arm64" ]; then
    echo "Cockroach_Ci_Builds_BuildMacOSArm64"
  elif [ "$plat" = "darwin-x86_64" ]; then
    echo "Cockroach_UnitTests_BazelBuildMacOSCross"
  else
    echo "No build for $PLAT"
    exit 1
  fi
}

prompt_to_overwrite() {
  read -p "File already exists. Overwrite? [y/N] " -n 1 -r
  echo
  [[ $REPLY =~ ^[Yy]$ ]]
}

build=$(build_for_plat)
URL="$TC_BASE/$build/$TC_ARTIFACT_PATH"
echo "Download URL: $URL"
if command -v curl &> /dev/null; then
  if [[ ! -f "$dest_file" ]] || prompt_to_overwrite; then
    curl -L -o "$dest_file" -# -u guest: "$URL"
    chmod +x "$dest_file"
  fi
else
  echo "curl is not installed. roachprod can be manually downloaded"
fi
