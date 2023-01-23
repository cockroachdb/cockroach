#!/bin/bash

# This script updates the .proto files under the opentelemetry-proto dir with
# upstream protos from https://github.com/open-telemetry/opentelemetry-proto.
#
# Use as
# cd pkg/obsservice/obspb
# ./update-opentelemetry-proto.sh [-k] [<SHA>]
#
# We copy the protos that we need from opentelemetry-proto into our tree because
# vendoring the upstream repo proved too difficult (it's not `go get`-able, some
# of the protos in it don't build with gogoproto (*), and also we already vendor
# https://github.com/open-telemetry/opentelemetry-proto-go, which contains the
# protoc-compiled protos. This other repo clashes with the import path the
# opentelemetry-proto wants.
#
# (*) opentelemetry-collector also uses gogoproto, and has a complicated build
# pipeline for the protos. For example, they transform all "optional" fields
# into "oneof" using sed:
# https://github.com/open-telemetry/opentelemetry-collector/blob/feab9491538a882737a5bceb8757b4458a86edd3/proto_patch.sed

set -euo pipefail

if [[ ! $(pwd) == */obspb ]]; then
  echo "$0 needs to be run from the obspb dir."
  exit
fi

keep=""
while getopts k flag
do
    case "$flag" in
        k)
          keep=true
          ;;
    esac
done
shift $(($OPTIND - 1))

SHA=""
if [ $# -eq 1 ]; then
    SHA=$1
    echo "No SHA supplied; will use latest from opentelemetry-proto."
fi

WORK_DIR=`mktemp -d`

# deletes the temp directory
function cleanup {
  if [ ! "$keep" ] ; then
    rm -rf "$WORK_DIR"
    echo "Deleted temp working directory $WORK_DIR"
  else
    echo "-k specified; keeping temp dir $WORK_DIR"
  fi
}
# register the cleanup function to be called on the EXIT signal
trap cleanup EXIT

echo "Cloning opentelemetry-proto in $WORK_DIR."
git clone git@github.com:open-telemetry/opentelemetry-proto.git $WORK_DIR --quiet
if [[ ! -z $SHA ]]; then
  echo "Checking out SHA: $SHA."
  git -C $WORK_DIR checkout --quiet $SHA
fi

BKDIR=`mktemp -d`
echo "Making a backup copy of opentelemetry-proto in $BKDIR."
cp -r opentelemetry-proto $BKDIR/
DEST_DIR=opentelemetry-proto

echo "Copying protos."
# Copy the protos from the repo.
rsync -avrq --include "*/" --include="common.proto" --include="resource.proto" --include="logs.proto" --include="logs_service.proto" --exclude="*" --prune-empty-dirs $WORK_DIR/opentelemetry/proto/* $DEST_DIR

# Massage the protos so that they work in our tree.
echo "Editing protos."

# Change lines like:
# option go_package = "go.opentelemetry.io/proto/otlp/common/v1";
# into:
# option go_package = "v1";
find $DEST_DIR -type f -name "*.proto" -exec sed -i.bak -e "s/option go_package = \"go.opentelemetry.io\/proto\/otlp\/.*\/v1\"/option go_package = \"v1\"/" {} + ;

# Change lines like:
# import "obsservice/obspb/opentelemetry-proto/common/v1/common.proto";
# into
# import "opentelemetry/proto/common/v1/common.proto";
find $DEST_DIR -type f -name "*.proto" -exec sed -i.bak -e "s/import \"opentelemetry\/proto\/\(.*\)\/v1/import \"obsservice\/obspb\/opentelemetry-proto\/\1\/v1/" {} + ;

# delete the .bak files created in previous steps
find . -name "*.bak" -type f -delete

# Apply a final patch customizing the code generation (sprinkle some gogo.nullable=false).
git apply opentelemetry-proto.patch

echo 'Done. Do not forget to run `./dev generate bazel` and `./dev generate protobuf`'
