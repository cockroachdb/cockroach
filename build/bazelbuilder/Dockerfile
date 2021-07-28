FROM ubuntu:focal-20210119

RUN apt-get update \
 && DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends \
    apt-transport-https \
    autoconf \
    bison \
    ca-certificates \
    clang-10 \
    cmake \
    curl \
    flex \
    g++ \
    git \
    gnupg2 \
    libncurses-dev \
    libtinfo-dev \
    make \
    netbase \
    unzip \
 && update-alternatives --install /usr/bin/clang clang /usr/bin/clang-10 100 \
    --slave /usr/bin/clang++ clang++ /usr/bin/clang++-10 \
 && apt-get clean

# We need a newer version of cmake.
#
# NOTE: When upgrading cmake, bump the rebuild counters in
# c-deps/*-rebuild to force recreating the makefiles. This prevents
# strange build errors caused by those makefiles depending on the
# installed version of cmake.
RUN curl -fsSL https://github.com/Kitware/CMake/releases/download/v3.20.3/cmake-3.20.3-linux-x86_64.tar.gz -o cmake.tar.gz \
 && echo '97bf730372f9900b2dfb9206fccbcf92f5c7f3b502148b832e77451aa0f9e0e6 cmake.tar.gz' | sha256sum -c - \
 && tar --strip-components=1 -C /usr -xzf cmake.tar.gz \
 && rm cmake.tar.gz

# git - Upgrade to a more modern version
RUN apt-get update && \
    DEBIAN_FRONTEND=noninteractive apt-get install dh-autoreconf libcurl4-gnutls-dev libexpat1-dev gettext libz-dev libssl-dev -y && \
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

# Install Bazelisk as Bazel.
# NOTE: you should keep this in sync with build/packer/teamcity-agent.sh -- if
# an update is necessary here, it's probably necessary in the agent as well.
RUN curl -fsSL https://github.com/bazelbuild/bazelisk/releases/download/v1.10.1/bazelisk-linux-amd64 > /tmp/bazelisk \
 && echo '4cb534c52cdd47a6223d4596d530e7c9c785438ab3b0a49ff347e991c210b2cd /tmp/bazelisk' | sha256sum -c - \
 && chmod +x /tmp/bazelisk \
 && mv /tmp/bazelisk /usr/bin/bazel

RUN rm -rf /tmp/* /var/lib/apt/lists/*

COPY entrypoint.sh /usr/bin
COPY autom4te /usr/local/sbin
ENTRYPOINT ["/usr/bin/entrypoint.sh"]
CMD ["/usr/bin/bash"]
