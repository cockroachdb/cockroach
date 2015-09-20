FROM golang:1.5

MAINTAINER Peter Mattis <peter@cockroachlabs.com>

RUN apt-get update -y && \
 apt-get dist-upgrade -y && \
 apt-get install --no-install-recommends --auto-remove -y git build-essential file npm nodejs && \
 apt-get clean autoclean && \
 apt-get autoremove -y && \
 rm -rf /tmp/* && \
 ln -s /usr/bin/nodejs /usr/bin/node

COPY npm-shrinkwrap.json package.json ./

RUN npm install && \
 rm npm-shrinkwrap.json package.json && \
 go get golang.org/x/tools/cmd/vet

CMD ["/bin/bash"]
