FROM ubuntu:xenial-20170915

RUN apt-get update && apt-get install -y --no-install-recommends \
    apt-transport-https \
    autoconf \
    bison \
    ca-certificates \
    curl \
    flex \
    git \
    libncurses-dev \
    make \
 && apt-get clean

# clang-3.9 - msan, libtapi needs 3.9+
# cmake - msan / c-deps: libroach, protobuf, et al.
# python - msan
RUN apt-get update \
  && apt-get install -y --no-install-recommends \
    clang-3.9 \
    cmake \
  && update-alternatives --install /usr/bin/clang clang /usr/bin/clang-3.9 100 \
    --slave /usr/bin/clang++ clang++ /usr/bin/clang++-3.9

# xenial installs cmake 3.5.1. We need a newer version. Run this step
# after the llvm/cross-compile step which is exceedingly slow.
#
# NOTE: When upgrading cmake, bump the rebuild counters in
# c-deps/*-rebuild to force recreating the makefiles. This prevents
# strange build errors caused by those makefiles depending on the
# installed version of cmake.
RUN curl -fsSL https://github.com/Kitware/CMake/releases/download/v3.17.0/cmake-3.17.0-Linux-x86_64.tar.gz -o cmake.tar.gz \
 && echo 'b44685227b9f9be103e305efa2075a8ccf2415807fbcf1fc192da4d36aacc9f5 cmake.tar.gz' | sha256sum -c - \
 && tar --strip-components=1 -C /usr -xzf cmake.tar.gz \
 && rm cmake.tar.gz

RUN echo "deb [arch=amd64] https://storage.googleapis.com/bazel-apt stable jdk1.8" | tee /etc/apt/sources.list.d/bazel.list \
 && curl https://bazel.build/bazel-release.pub.gpg | apt-key add - \
 && apt-get update \
 && apt-get install -y --no-install-recommends \
    bazel-3.6.0

# git - Upgrade to a more modern version
RUN apt-get install dh-autoreconf libcurl4-gnutls-dev libexpat1-dev gettext libz-dev libssl-dev -y && \
    curl -fsSL https://github.com/git/git/archive/v2.29.2.zip -o "git-2.29.2.zip" && \
    unzip "git-2.29.2.zip" && \
    cd git-2.29.2 && \
    make configure && \
    ./configure && \
    make && \
    make install && \
    cd .. && \
    rm -rf git-2.29.2.zip git-2.29.2

RUN apt-get purge -y \
    apt-transport-https \
    flex \
    gettext \
 && apt-get autoremove -y

RUN rm -rf /tmp/* /var/lib/apt/lists/*

RUN ln -s /usr/bin/bazel-3.6.0 /usr/bin/bazel

COPY bazelbuild.sh /usr/local/bin
