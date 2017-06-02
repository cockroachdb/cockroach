FROM gcr.io/google_containers/peer-finder:0.2

MAINTAINER Alex Robinson <alexdwanerobinson@gmail.com>

COPY on-start.sh /
RUN chmod -c 755 /on-start.sh

ENTRYPOINT ["/peer-finder"]
