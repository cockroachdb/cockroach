FROM golang:1.5.2

MAINTAINER Peter Mattis <peter@cockroachlabs.com>

RUN curl --silent --location https://deb.nodesource.com/setup_5.x | bash - && \
 apt-get dist-upgrade -y && \
 apt-get install --no-install-recommends --auto-remove -y git build-essential file nodejs && \
 apt-get clean autoclean && \
 apt-get autoremove -y && \
 curl --silent --location http://www.musl-libc.org/releases/musl-1.1.12.tar.gz | tar -xz && \
 cd musl-1.1.12 && ./configure && make && make install && \
 rm -rf /tmp/*

ENV SKIP_BOOTSTRAP=1

CMD ["/bin/bash"]
