FROM golang:1.5.1

MAINTAINER Peter Mattis <peter@cockroachlabs.com>

RUN curl --silent --location https://deb.nodesource.com/setup_5.x | bash - && \
 apt-get dist-upgrade -y && \
 apt-get install --no-install-recommends --auto-remove -y git build-essential file nodejs && \
 apt-get clean autoclean && \
 apt-get autoremove -y && \
 rm -rf /tmp/*

ENV SKIP_BOOTSTRAP=1

CMD ["/bin/bash"]
