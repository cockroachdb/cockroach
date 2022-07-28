# WARNING:
# This Dockerfile must be built from the root of cockroachdb.git with a prebuilt
# cockroach binary at ./upstream_artifactsl/cockroach. Running it from other
# directories or without a prebuilt cockroach binary will cause errors.
#
# Build (from the root of cockroach.git):
# $ DOCKER_BUILDKIT=1 docker build \
#     -f ./pkg/ui/workspaces/e2e-tests/Dockerfile \
#     -t cockroachdb/cockroach-ci-ui \
#     $PWD
#
# Run Locally (from the root of cockroach.git):
# $ docker run \
#     --rm \
#     -it \
#     -v $PWD:/upstream_artifacts \
#     -v $PWD/pkg/ui/workspaces/e2e-tests/cypress:/cypress
#     cockroachdb/cockroach-ci-ui
#
# Run in TeamCity (from the root of cockroach.git):
# $ docker run \
#     --rm \
#     -it \
#     -v $PWD/upstream_artifacts:/upstream_artifacts \
#     -v $PWD/artifacts:/artifacts \
#     cockroachdb/cockroach-ci-ui

FROM cypress/browsers:node16.14.2-slim-chrome100-ff99-edge

# Install CRDB prerequisites
RUN apt install libresolv-wrapper

# Add the tests and install NPM dependencies
WORKDIR /cypress
COPY --chown=node ./pkg/ui/yarn-vendor ./yarn-vendor/
COPY --chown=node ./pkg/ui/workspaces/e2e-tests ./
ADD ./pkg/ui/.yarnrc ./
RUN yarn install

ENV IS_DOCKER=1

# Use the compiled CRDB binary from a previous TC job via Artifact Dependency:
# TODO(barag): include a TC link here
# bazel-bin/pkg/cmd/cockroach/cockroach_/cockroach=>upstream_artifacts
ENV COCKROACH=/upstream_artifacts/cockroach

VOLUME /artifacts

# Execute 'yarn test' by default, allowing extra arguments to be appended during
# 'docker run'.
ENTRYPOINT ["/usr/local/bin/yarn", "test"]
