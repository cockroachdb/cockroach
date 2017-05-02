# Building this Docker container will sanity-check a source tarball. Source
# tarballs are expected to build and install a functional cockroach binary into
# the PATH, even when the tarball is extracted outside of GOPATH.

FROM golang:1.8.1

MAINTAINER Nikhil Benesch <nikhil.benesch@gmail.com>

RUN apt-get update && apt-get install -y cmake xz-utils

ENV GOPATH /garbage

COPY cockroach.src.tgz .

RUN tar xzf cockroach.src.tgz \
  && rm cockroach.src.tgz \
  && cd cockroach-* \
  && make install

COPY expected .

RUN cockroach start --insecure --store type=mem,size=1GiB --background \
  && cockroach sql --insecure -e " \
    CREATE DATABASE bank; \
    CREATE TABLE bank.accounts (id INT PRIMARY KEY, balance DECIMAL); \
    INSERT INTO bank.accounts VALUES (1, 1000.50); \
    SELECT * FROM bank.accounts;" \
  | diff expected -
