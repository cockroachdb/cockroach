# use a scratch image with glibc/passwd/certs
FROM gcr.io/distroless/base

# set the right release channel
ENV COCKROACH_CHANNEL=official-docker

# copy in the cockroach binary with correct permissions
COPY --chown=nonroot cockroach /

# exposed network ports
EXPOSE 26257 8080

# run as non-privileged user
USER nonroot

# entrypoint command
ENTRYPOINT ["/cockroach"]
