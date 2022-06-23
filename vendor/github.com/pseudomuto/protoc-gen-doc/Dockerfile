FROM debian:jessie-slim
LABEL maintainer="pseudomuto <david.muto@gmail.com>" protoc_version="3.6.1"

WORKDIR /

ADD https://github.com/google/protobuf/releases/download/v3.6.1/protoc-3.6.1-linux-x86_64.zip ./
RUN apt-get -q -y update && \
  apt-get -q -y install unzip && \
  unzip protoc-3.6.1-linux-x86_64.zip -d ./usr/local && \
  rm protoc-3.6.1-linux-x86_64.zip && \
  apt-get remove --purge -y unzip && \
  apt-get autoremove && \
  rm -rf /var/lib/apt/lists/*

ADD dist/protoc-gen-doc /usr/local/bin/
ADD script/entrypoint.sh ./

VOLUME ["/out", "/protos"]

ENTRYPOINT ["/entrypoint.sh"]
CMD ["--doc_opt=html,index.html"]
