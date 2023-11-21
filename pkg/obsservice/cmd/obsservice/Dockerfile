# Generates a Docker image of the obsservice binary.
# Use OTLP_ADDR and HTTP_ADDR env vars when running the image to control
# the ports/addresses that are listened on for each. Defaults are used
# otherwise (:4317 and :8081).
#
# You'll need to expose relevant ports when running the image.

FROM --platform=linux/amd64 debian:stable-slim
WORKDIR /bin
ENV OTLP_ADDR=0.0.0.0:4317
ENV HTTP_ADDR=0.0.0.0:8081
ENV SINK_PGURL="postgresql://root@0.0.0.0:26257?sslmode=disable"
ENV NO_DB=false
COPY ./artifact_obsservice /bin/
CMD /bin/artifact_obsservice --otlp-addr=$OTLP_ADDR --http-addr=$HTTP_ADDR --sink-pgurl=$SINK_PGURL --no-db=$NO_DB
