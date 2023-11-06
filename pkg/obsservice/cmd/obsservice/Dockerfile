# Generates a Docker image of the obsservice binary.
# Use OTLP_ADDR and HTTP_ADDR env vars when running the image to control
# the ports/addresses that are listened on for each. Defaults are used
# otherwise (:4317 and :8081).
#
# You'll need to expose relevant ports when running the image.

FROM --platform=linux/amd64 debian:stable-slim
WORKDIR /bin
ENV OTLP_ADDR=localhost:4317
ENV HTTP_ADDR=localhost:8081
COPY ./artifact_obsservice /bin/
CMD /bin/artifact_obsservice --otlp-addr=$OTLP_ADDR --http-addr=$HTTP_ADDR
