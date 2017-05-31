FROM gcr.io/google_containers/peer-finder:0.2

COPY on-start.sh /
RUN chmod -c 755 /on-start.sh

ENTRYPOINT ["/peer-finder"]
