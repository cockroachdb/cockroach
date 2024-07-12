# We use a docker image mirror to avoid pulling from 3rd party repos, which sometimes have reliability issues.
# See https://cockroachlabs.atlassian.net/wiki/spaces/devinf/pages/3462594561/Docker+image+sync for the details.
FROM us-east1-docker.pkg.dev/crl-docker-sync/docker-io/library/ubuntu:noble
ARG TARGETPLATFORM

RUN apt-get update \
 && DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends \
    apt-transport-https \
    autoconf \
    bison \
    ca-certificates \
    clang-18 \
    cmake \
    curl \
    flex \
    g++ \
    git \
    gnupg2 \
    libncurses-dev \
    libtinfo-dev \
    lld \
    llvm \
    lsb-release \
    lsof \
    make \
    netbase \
    openjdk-8-jre \
    openssh-client \
    patch \
    patchelf \
    python-is-python3 \
    python3 \
    python3.12-venv \
    unzip \
    zip \
 && update-alternatives --install /usr/bin/clang clang /usr/bin/clang-18 100 \
    --slave /usr/bin/clang++ clang++ /usr/bin/clang++-18 \
 && apt-get clean

RUN mkdir -p /etc/apt/keyrings

# Note that we "should be" downloading this key from https://packages.microsoft.com/keys/microsoft.asc.
# However, as of this writing, that fails when trying to curl the file from
# our CI machines. I don't know why. We work around it by simply checking the
# key in and copying it from our source control.
COPY microsoft.gpg /etc/apt/keyrings/microsoft.gpg

RUN chmod go+r /etc/apt/keyrings/microsoft.gpg \
 && echo "Types: deb\nURIs: https://packages.microsoft.com/repos/azure-cli/\nSuites: $(lsb_release -cs)\nComponents: main\nArchitectures: $(dpkg --print-architecture)\nSigned-by: /etc/apt/keyrings/microsoft.gpg" | tee /etc/apt/sources.list.d/azure-cli.sources \
 && apt-get update \
 && apt-get install azure-cli \
 && apt-get clean

RUN curl -O https://dl.google.com/dl/cloudsdk/channels/rapid/downloads/google-cloud-cli-linux-x86_64.tar.gz -o google-cloud-cli.tar.gz \
 && tar -xf google-cloud-cli-linux-x86_64.tar.gz \
 && ./google-cloud-sdk/install.sh --quiet --usage-reporting false --command-completion false --path-update false --additional-components gke-gcloud-auth-plugin --no-compile-python

ENV PATH="$PATH:/google-cloud-sdk/bin"

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
RUN ln -sf /usr/bin/llvm-nm /usr/bin/nm \
 && ln -sf /usr/bin/lld /usr/bin/ld

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
