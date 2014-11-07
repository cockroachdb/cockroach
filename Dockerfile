FROM cockroachdb/cockroach_base:latest

MAINTAINER Tobias Schottdorf <tobias.schottdorf@gmail.com>


# Copy the contents of the cockroach source directory to the image.
# Any changes which have been made to the source directory will cause
# the docker image to be rebuilt starting at this cached step.
#
# NOTE: the .dockerignore file excludes the _vendor subdirectory. This
# is done to avoid rebuilding rocksdb in the common case where changes
# are only made to cockroach. If rocksdb is being hacked, remove the
# "_vendor" exclude from .dockerignore.
ADD . /cockroach/
RUN ln -s /cockroach/build/devbase/cockroach.sh /cockroach/cockroach.sh

# Update to the correct version of our submodules and rebuild any changes
# in RocksDB (in case the submodule revision is different from the current
# master)
# Build the cockroach executable.
RUN cd -P /cockroach && git submodule update --init && \
 cd -P /cockroach/_vendor/rocksdb && make static_lib
RUN cd -P /cockroach && make build

# Expose the http status port.
EXPOSE 8080

WORKDIR /cockroach/

# This is the command to run when this image is launched as a container.
ENTRYPOINT ["/cockroach/cockroach.sh"]

# These are default arguments to the cockroach binary.
CMD ["--help"]
