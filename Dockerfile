FROM cockroachdb/cockroach-devbase:latest

MAINTAINER Tobias Schottdorf <tobias.schottdorf@gmail.com>

ENV ROACHPATH /go/src/github.com/cockroachdb

# Copy the contents of the cockroach source directory to the image.
# Any changes which have been made to the source directory will cause
# the docker image to be rebuilt starting at this cached step.
ADD . ${ROACHPATH}/cockroach/
RUN ln -s ${ROACHPATH}/cockroach/build/devbase/cockroach.sh ${ROACHPATH}/cockroach/cockroach.sh

# Build the cockroach executable.
RUN cd -P ${ROACHPATH}/cockroach && make build

# Expose the http status port.
EXPOSE 8080

# This is the command to run when this image is launched as a container.
# Environment variable expansion doesn't seem to work here.
ENTRYPOINT ["/go/src/github.com/cockroachdb/cockroach/cockroach.sh"]

# These are default arguments to the cockroach binary.
CMD ["--help"]
