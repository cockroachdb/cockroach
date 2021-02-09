# Common helpers for release-*.sh scripts.

# Matching the version name regex from within the cockroach code except
# for the `metadata` part at the end because Docker tags don't support
# `+` in the tag name.
# https://github.com/cockroachdb/cockroach/blob/4c6864b44b9044874488cfedee3a31e6b23a6790/pkg/util/version/version.go#L75
build_name="$(echo "${NAME}" | grep -E -o '^v(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)(-[-.0-9A-Za-z]+)?$')"
#                                             ^major           ^minor           ^patch         ^preRelease

if [[ -z "$build_name" ]] ; then
    echo "Invalid NAME \"${NAME}\". Must be of the format \"vMAJOR.MINOR.PATCH(-PRERELEASE)?\"."
    exit 1
fi

release_branch=$(echo ${build_name} | grep -E -o '^v[0-9]+\.[0-9]+')
version=$(echo ${build_name} | sed -e 's/^v//' | cut -d- -f 1)
# Hard coded release number used only by the RedHat images
release=1

if [[ -z "${DRY_RUN}" ]] ; then
  bucket="${BUCKET:-binaries.cockroachdb.com}"
  google_credentials="$GOOGLE_COCKROACH_CLOUD_IMAGES_CREDENTIALS"
  if [[ -z "${PRE_RELEASE}" ]] ; then
    dockerhub_repository="docker.io/cockroachdb/cockroach"
  else
    dockerhub_repository="docker.io/cockroachdb/cockroach-unstable"
  fi
  gcr_repository="us.gcr.io/cockroach-cloud-images/cockroach"
  s3_download_hostname="${bucket}"
  git_repo_for_tag="cockroachdb/cockroach"
else
  bucket="${BUCKET:-cockroach-builds-test}"
  google_credentials="$GOOGLE_COCKROACH_RELEASE_CREDENTIALS"
  dockerhub_repository="docker.io/cockroachdb/cockroach-misc"
  gcr_repository="us.gcr.io/cockroach-release/cockroach-test"
  s3_download_hostname="${bucket}.s3.amazonaws.com"
  git_repo_for_tag="cockroachlabs/release-staging"
  if [[ -z "$(echo ${build_name} | grep -E -o '^v[0-9]+\.[0-9]+\.[0-9]+$')" ]] ; then
    # Using `.` to match how we usually format the pre-release portion of the
    # version string using '.' separators.
    # ex: v20.2.0-rc.2.dryrun
    build_name="${build_name}.dryrun"
  else
    # Using `-` to put dryrun in the pre-release portion of the version string.
    # ex: v20.2.0-dryrun
    build_name="${build_name}-dryrun"
  fi
fi

# Used for docker login for gcloud
gcr_hostname="us.gcr.io"
