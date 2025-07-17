# We use a docker image mirror to avoid pulling from 3rd party repos, which sometimes have reliability issues.
# See https://cockroachlabs.atlassian.net/wiki/spaces/devinf/pages/3462594561/Docker+image+sync for the details.
FROM us-east1-docker.pkg.dev/crl-docker-sync/docker-io/library/ubuntu:focal
ARG TARGETPLATFORM

SHELL ["/usr/bin/bash", "-c"]

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
RUN if [[ ${TARGETPLATFORM} == "linux/s390x" ]] ; then \
   curl -fsSL "https://github.com/Kitware/CMake/archive/refs/tags/v3.20.3.tar.gz" -o cmake.tar.gz \
   && echo "aa059c7f89b56215301f1baac8f88a70a67a334495c9ab6a728b97e1defab763 cmake.tar.gz" | sha256sum -c - \
   && tar -xzf cmake.tar.gz \
   && cd CMake-3.20.3 \
   && ./bootstrap -- -DCMAKE_USE_OPENSSL=OFF \
   && make \
   && make install \
   && cd .. \
   && rm -rf CMake-3.20.3 \
   && rm cmake.tar.gz ; \
 else \
  case ${TARGETPLATFORM} in \
    "linux/amd64") ARCH=x86_64; SHASUM=97bf730372f9900b2dfb9206fccbcf92f5c7f3b502148b832e77451aa0f9e0e6 ;; \
    "linux/arm64") ARCH=aarch64; SHASUM=77620f99e9d5f39cf4a49294c6a68c89a978ecef144894618974b9958efe3c2a ;; \
  esac \
   && curl -fsSL "https://github.com/Kitware/CMake/releases/download/v3.20.3/cmake-3.20.3-linux-$ARCH.tar.gz" -o cmake.tar.gz \
   && echo "$SHASUM cmake.tar.gz" | sha256sum -c - \
   && tar --strip-components=1 -C /usr -xzf cmake.tar.gz \
   && rm cmake.tar.gz ; \
 fi

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

# NB: Don't install the azure CLI on s390x which doesn't support it.
RUN if [[ ${TARGETPLATFORM} != "linux/s390x" ]]; then \
 curl -fsLS https://packages.microsoft.com/keys/microsoft.asc | gpg --dearmor | apt-key add - \
 && echo "deb https://packages.microsoft.com/repos/azure-cli/ focal main" > /etc/apt/sources.list.d/azure-cli.list \
 && apt-get update \
 && DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends azure-cli \
 && apt-get clean ; \
 fi

# NB: As above, this is not available on `s390x`.
RUN if [[ ${TARGETPLATFORM} != "linux/s390x" ]]; then \
 case ${TARGETPLATFORM} in \
     "linux/amd64") ARCH=x86_64; SHASUM= ;; \
     "linux/arm64") ARCH=arm; SHASUM=e6153461e3154ebce61d35b73005bdd14a0ecacd42e5008f66e25b4ad231e5c9 ;; \
 esac  \
 && curl -fsSL "https://dl.google.com/dl/cloudsdk/channels/rapid/downloads/google-cloud-cli-linux-$ARCH.tar.gz" -o gcloud.tar.gz \
 && tar -xzf gcloud.tar.gz \
 && rm gcloud.tar.gz ; \
 fi

ENV PATH="$PATH:/google-cloud-sdk/bin"

# NB: We're going to run `dev` builds inside the Docker image on `s390x`,
# as we can't cross-compile them (there are no cross-toolchains for `s390x`
# hosts. This means we need these extra dependencies installed specifically
# on that platform. Don't install them on other platforms to avoid taking
# unintended dependencies on them.
RUN if [[ ${TARGETPLATFORM} == "linux/s390x" ]]; then \
  DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends \
    keyutils \
    libresolv-wrapper \
  && apt-get clean ; \
 fi

RUN apt-get purge -y \
    apt-transport-https \
    flex \
    gettext \
 && apt-get autoremove -y

# awscli - roachtests
# NB: we don't use apt-get because we need an up to date version of awscli
# NB: Don't install these SDK's that are unavailable for s390x.
RUN if [[ ${TARGETPLATFORM} != "linux/s390x" ]]; then \
 case ${TARGETPLATFORM} in \
    "linux/amd64") ARCH=x86_64; SHASUM=e679933eec90b0e5a75d485be6c2fae0f89a3f9ccdcb1748be69f8f456e9a85f ;; \
    "linux/arm64") ARCH=aarch64; SHASUM=7d6460f795712ebdac7e3c60d4800dde682d136d909810402aac164f2789b860 ;; \
  esac \
 && curl -fsSL "https://awscli.amazonaws.com/awscli-exe-linux-$ARCH-2.13.9.zip" -o "awscliv2.zip" \
 && echo "$SHASUM awscliv2.zip" | sha256sum -c - \
 && unzip awscliv2.zip \
 && ./aws/install \
 && rm -rf aws awscliv2.zip ; \
 fi

RUN case ${TARGETPLATFORM} in \
    "linux/amd64") ARCH=x86_64; SHASUM=a3fb9c1de3512bc91f27cc47297d6d6cf208adee9b64ed719130da59ac13e26b ;; \
    "linux/arm64") ARCH=aarch64; SHASUM=e5165eb592a317e1f6da0ac7fcbccf60d7fb8e5ac1f0d7336a9be51c23308b06 ;; \
    "linux/s390x") ARCH=s390x; SHASUM=4969ae702488cb79afd14bf91c10b496996852b82a364907c1ebfa5f1667a139 ;; \
  esac && \
 curl -fsSL "https://github.com/NixOS/patchelf/releases/download/0.17.2/patchelf-0.17.2-$ARCH.tar.gz" -o "patchelf.tar.gz" && \
 echo "$SHASUM patchelf.tar.gz" | sha256sum -c - && \
 tar --strip-components=1 -C /usr -xzf patchelf.tar.gz && \
 rm -rf patchelf.tar.gz

# Install Bazelisk as Bazel.
# NOTE: you should keep this in sync with build/packer/teamcity-agent.sh and
# build/bootstrap/bootstrap-debian.sh -- if an update is necessary here, it's probably
# necessary in the agent as well.
RUN case ${TARGETPLATFORM} in \
    "linux/amd64") ARCH=amd64; SHASUM=84916c44c8d81cb64f6c9a9f8fd8fa059342e872bfc1ce185f5dcbf70c6aadea ;; \
    "linux/arm64") ARCH=arm64; SHASUM=7937c941e5140a6a22f6b84919e561b9b77ec49e307852ed0b3cc2a45beace9e ;; \
    "linux/s390x") ARCH=s390x; SHASUM=df7a5cfe632da022bb2cdc51824e8b04634d86d3ad4a24610c4da758c2e5708f ;; \
  esac \
 && curl -fsSL "https://github.com/cockroachdb/bazelisk/releases/download/2025-07-14/bazelisk-linux-$ARCH" > /tmp/bazelisk \
 && echo "$SHASUM /tmp/bazelisk" | sha256sum -c - \
 && chmod +x /tmp/bazelisk \
 && mv /tmp/bazelisk /usr/bin/bazel

# Replace the nm command with LLVM's version, llvm-nm, which knows how to read
# binaries build for platforms others than Linux.
RUN ln -sf /usr/bin/llvm-nm /usr/bin/nm

RUN rm -rf /tmp/* /var/lib/apt/lists/*

RUN if [[ ${TARGETPLATFORM} == "linux/s390x" ]]; then \
  curl -fsSL "https://github.com/benesch/autouseradd/archive/refs/tags/1.3.0.tar.gz" -o autouseradd.tar.gz \
  && echo "da70cbb00878ab395276b0f6191815a763bc8aa2fc120fb36580f6313de4c41f autouseradd.tar.gz" | sha256sum -c - \
  && tar -xzf autouseradd.tar.gz \
  && cd autouseradd-1.3.0 \
  && make \
  && make install \
  && cd .. \
  && rm -rf autouseradd-1.3.0 \
  && rm autouseradd.tar.gz ; \
 else \
  case ${TARGETPLATFORM} in \
    "linux/amd64") ARCH=amd64; SHASUM=442dae58b727a79f81368127fac141d7f95501ffa05f8c48943d27c4e807deb7 ;; \
    "linux/arm64") ARCH=arm64; SHASUM=b216bebfbe30c3c156144cff07233654e23025e26ab5827058c9b284e130599e ;; \
   esac \
  && curl -fsSL "https://github.com/benesch/autouseradd/releases/download/1.3.0/autouseradd-1.3.0-$ARCH.tar.gz" -o autouseradd.tar.gz \
  && echo "$SHASUM autouseradd.tar.gz" | sha256sum -c - \
  && tar xzf autouseradd.tar.gz --strip-components 1 \
  && rm autouseradd.tar.gz ; \
  fi

ENTRYPOINT ["autouseradd", "--user", "roach", "--no-create-home"]
CMD ["/usr/bin/bash"]
