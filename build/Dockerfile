FROM ubuntu:xenial-20170119

MAINTAINER Tamir Duberstein <tamird@gmail.com>

RUN apt-get update && apt-get install -y --no-install-recommends \
    apt-transport-https \
    ca-certificates \
    curl

RUN curl -fsSL https://deb.nodesource.com/gpgkey/nodesource.gpg.key | apt-key add - && \
    echo 'deb https://deb.nodesource.com/node_6.x xenial main' | tee /etc/apt/sources.list.d/nodesource.list && \
    curl -fsSL https://dl.yarnpkg.com/debian/pubkey.gpg | apt-key add - && \
    echo 'deb https://dl.yarnpkg.com/debian/ stable main' | tee /etc/apt/sources.list.d/yarn.list

# cmake and python are used to build libcxx.
# nodejs is used to build and test the UI.
# bzip2 and fontconfig are used by phantomjs-prebuilt to test the UI.
# iptables is used in the acceptance tests' partition nemesis.
# yarn is the dependency manager for the UI, as an alternative to npm.
RUN apt-get update && apt-get install -y --no-install-recommends \
    bzip2 \
    clang \
    cmake \
    fontconfig \
    git \
    iptables \
    make \
    nodejs \
    python \
    unzip \
    yarn

ENV CC clang
ENV CXX clang++

# Install an msan-enabled build of libc++, following instructions from
# https://github.com/google/sanitizers/wiki/MemorySanitizerLibcxxHowTo

# Build libcxx from head, because rocksdb 4.11 doesn't build with libcxx 3.9 on linux.
# TODO(bdarnell): after we upgrade to rocksdb 5.0, replace these git commands with the
# commented-out tarball installation below.

RUN git                  clone --depth 1 --branch release_40 https://github.com/llvm-mirror/llvm.git && \
    git -C llvm/projects clone --depth 1 --branch release_40 https://github.com/llvm-mirror/libcxx.git && \
    git -C llvm/projects clone --depth 1 --branch release_40 https://github.com/llvm-mirror/libcxxabi.git && \
    mkdir libcxx_msan && (cd libcxx_msan && cmake ../llvm -DCMAKE_BUILD_TYPE=Release -DLLVM_USE_SANITIZER=Memory && make cxx -j16)

#mkdir ~/llvm && curl -sfSL http://releases.llvm.org/3.9.1/llvm-3.9.1.src.tar.xz | tar --strip-components=1 -C ~/llvm -xJ
#mkdir ~/llvm/projects/libcxx && curl -sfSL http://releases.llvm.org/3.9.1/libcxx-3.9.1.src.tar.xz | tar --strip-components=1 -C ~/llvm/projects/libcxx -xJ
#mkdir ~/llvm/projects/libcxxabi && curl -sfSL http://releases.llvm.org/3.9.1/libcxxabi-3.9.1.src.tar.xz | tar --strip-components=1 -C ~/llvm/projects/libcxxabi -xJ

# BEGIN https://github.com/docker-library/golang/blob/master/1.7/Dockerfile

ENV GOLANG_VERSION 1.7.4
ENV GOLANG_DOWNLOAD_URL https://golang.org/dl/go$GOLANG_VERSION.linux-amd64.tar.gz
ENV GOLANG_DOWNLOAD_SHA256 47fda42e46b4c3ec93fa5d4d4cc6a748aa3f9411a2a2b7e08e3a6d80d753ec8b

RUN curl -fsSL "$GOLANG_DOWNLOAD_URL" -o golang.tar.gz && \
    echo "$GOLANG_DOWNLOAD_SHA256  golang.tar.gz" | sha256sum -c - && \
    tar -C /usr/local -xzf golang.tar.gz && \
    rm golang.tar.gz

ENV GOPATH /go
ENV PATH $GOPATH/bin:/usr/local/go/bin:$PATH

RUN mkdir -p "$GOPATH/src" "$GOPATH/bin" && chmod -R 777 "$GOPATH"
WORKDIR $GOPATH

# END https://github.com/docker-library/golang/blob/master/1.7/Dockerfile

RUN chmod -R a+w $(go env GOTOOLDIR)

# Allow Go support files in gdb.
RUN echo "add-auto-load-safe-path $(go env GOROOT)/src/runtime/runtime-gdb.py" > ~/.gdbinit

RUN curl -fsSL https://releases.hashicorp.com/terraform/0.7.7/terraform_0.7.7_linux_amd64.zip -o terraform.zip && \
    unzip -d /usr/local/bin terraform.zip && \
    rm terraform.zip

ENV PATH /opt/backtrace/bin:$PATH

CMD ["/bin/bash"]
