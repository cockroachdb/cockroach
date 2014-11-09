FROM busybox:buildroot-2014.02

MAINTAINER Tobias Schottdorf <tobias.schottdorf@gmail.com>

RUN mkdir -p /test /cockroach
ADD cockroach /cockroach/
ADD cockroach.sh /cockroach/
ADD test.sh /test/

EXPOSE 8080
ENTRYPOINT ["/cockroach/cockroach.sh"]
CMD ["--help"]
