FROM cockroachdb/docker_base:latest

MAINTAINER Spencer Kimball <spencer.kimball@gmail.com>

# Copy the contents of the cockroach source directory to the image.
# Any changes which have been made to the source directory will cause
# the docker image to be rebuilt starting at this cached step.
#
# NOTE: the .dockerignore file excludes the _vendor subdirectory. This
# is done to avoid rebuilding rocksdb in the common case where changes
# are only made to cockroach. If rocksdb is being hacked, remove the
# "_vendor" exclude from .dockerignore.
ADD . /cockroach/

# Update to the correct version of our submodules and rebuild any changes
# in rocksdb (in case the submodule revision is different from the current
# master)
# Build the cockroach executable and run the tests.
RUN cd -P /cockroach && git submodule update --init && \
 cd -P /cockroach/_vendor/rocksdb && make static_lib && \
 cd -P /cockroach && make build

# Expose the http status port.
EXPOSE 8080

# This is the command to run when this image is launched as a container.
ENTRYPOINT ["/cockroach/deploy/wrapper.sh"]

# These are default arguments to the cockroach binary.
CMD ["--help"]
