# We use a docker image mirror to avoid pulling from 3rd party repos, which sometimes have reliability issues.
# See https://cockroachlabs.atlassian.net/wiki/spaces/devinf/pages/3462594561/Docker+image+sync for the details.
FROM us-east1-docker.pkg.dev/crl-docker-sync/docker-io/library/ubuntu:focal
ARG TARGETPLATFORM

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
    llvm \
    lsof \
    make \
    netbase \
    openjdk-8-jre \
    openssh-client \
    patchelf \
    python-is-python3 \
    python3 \
    python3.8-venv \
    unzip \
    zip \
 && update-alternatives --install /usr/bin/clang clang /usr/bin/clang-10 100 \
    --slave /usr/bin/clang++ clang++ /usr/bin/clang++-10 \
 && apt-get clean

# We need a newer version of cmake.
#
# NOTE: When upgrading cmake, bump the rebuild counters in
# c-deps/*-rebuild to force recreating the makefiles. This prevents
# strange build errors caused by those makefiles depending on the
# installed version of cmake.
RUN case ${TARGETPLATFORM} in \
    "linux/amd64") ARCH=x86_64; SHASUM=97bf730372f9900b2dfb9206fccbcf92f5c7f3b502148b832e77451aa0f9e0e6 ;; \
    "linux/arm64") ARCH=aarch64; SHASUM=77620f99e9d5f39cf4a49294c6a68c89a978ecef144894618974b9958efe3c2a ;; \
  esac \
 && curl -fsSL "https://github.com/Kitware/CMake/releases/download/v3.20.3/cmake-3.20.3-linux-$ARCH.tar.gz" -o cmake.tar.gz \
 && echo "$SHASUM cmake.tar.gz" | sha256sum -c - \
 && tar --strip-components=1 -C /usr -xzf cmake.tar.gz \
 && rm cmake.tar.gz

# git - Upgrade to a more modern version
RUN apt-get update && \
    DEBIAN_FRONTEND=noninteractive apt-get install dh-autoreconf libcurl4-gnutls-dev libexpat1-dev gettext libz-dev libssl-dev -y && \
    apt-get clean && \
    curl -fsSL https://github.com/git/git/archive/v2.29.2.zip -o "git-2.29.2.zip" && \
    unzip "git-2.29.2.zip" && \
    cd git-2.29.2 && \
    make configure && \
    ./configure && \
    make && \
    make install && \
    cd .. && \
    rm -rf git-2.29.2.zip git-2.29.2

RUN curl -fsSL https://packages.cloud.google.com/apt/doc/apt-key.gpg | apt-key add - \
 && echo 'deb https://packages.cloud.google.com/apt cloud-sdk main' | tee /etc/apt/sources.list.d/gcloud.list \
 && curl -fsLS https://packages.microsoft.com/keys/microsoft.asc | gpg --dearmor | apt-key add - \
 && echo "deb https://packages.microsoft.com/repos/azure-cli/ focal main" > /etc/apt/sources.list.d/azure-cli.list \
 && apt-get update \
 && DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends \
    azure-cli \
    google-cloud-sdk \
    google-cloud-cli-gke-gcloud-auth-plugin \
    && apt-get clean

RUN apt-get purge -y \
    apt-transport-https \
    flex \
    gettext \
 && apt-get autoremove -y

# awscli - roachtests
# NB: we don't use apt-get because we need an up to date version of awscli
RUN case ${TARGETPLATFORM} in \
    "linux/amd64") ARCH=x86_64; SHASUM=e679933eec90b0e5a75d485be6c2fae0f89a3f9ccdcb1748be69f8f456e9a85f ;; \
    "linux/arm64") ARCH=aarch64; SHASUM=7d6460f795712ebdac7e3c60d4800dde682d136d909810402aac164f2789b860 ;; \
  esac && \
 curl -fsSL "https://awscli.amazonaws.com/awscli-exe-linux-$ARCH-2.13.9.zip" -o "awscliv2.zip" && \
 echo "$SHASUM awscliv2.zip" | sha256sum -c - && \
 unzip awscliv2.zip && \
 ./aws/install && \
 rm -rf aws awscliv2.zip

# Install Bazelisk as Bazel.
# NOTE: you should keep this in sync with build/packer/teamcity-agent.sh and
# build/bootstrap/bootstrap-debian.sh -- if an update is necessary here, it's probably
# necessary in the agent as well.
RUN case ${TARGETPLATFORM} in \
    "linux/amd64") ARCH=amd64; SHASUM=4cb534c52cdd47a6223d4596d530e7c9c785438ab3b0a49ff347e991c210b2cd ;; \
    "linux/arm64") ARCH=arm64; SHASUM=c1de6860dd4f8d5e2ec270097bd46d6a211b971a0b8b38559784bd051ea950a1 ;; \
  esac \
 && curl -fsSL "https://github.com/bazelbuild/bazelisk/releases/download/v1.10.1/bazelisk-linux-$ARCH" > /tmp/bazelisk \
 && echo "$SHASUM /tmp/bazelisk" | sha256sum -c - \
 && chmod +x /tmp/bazelisk \
 && mv /tmp/bazelisk /usr/bin/bazel

# Replace the nm command with LLVM's version, llvm-nm, which knows how to read
# binaries build for platforms others than Linux.
RUN ln -sf /usr/bin/llvm-nm /usr/bin/nm

RUN rm -rf /tmp/* /var/lib/apt/lists/*

RUN case ${TARGETPLATFORM} in \
    "linux/amd64") ARCH=amd64; SHASUM=442dae58b727a79f81368127fac141d7f95501ffa05f8c48943d27c4e807deb7 ;; \
    "linux/arm64") ARCH=arm64; SHASUM=b216bebfbe30c3c156144cff07233654e23025e26ab5827058c9b284e130599e ;; \
   esac \
  && curl -fsSL "https://github.com/benesch/autouseradd/releases/download/1.3.0/autouseradd-1.3.0-$ARCH.tar.gz" -o autouseradd.tar.gz \
  && echo "$SHASUM autouseradd.tar.gz" | sha256sum -c - \
  && tar xzf autouseradd.tar.gz --strip-components 1 \
  && rm autouseradd.tar.gz

ENTRYPOINT ["autouseradd", "--user", "roach", "--no-create-home"]
CMD ["/usr/bin/bash"]
