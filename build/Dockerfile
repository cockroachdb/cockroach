FROM golang:1.7.1

MAINTAINER Peter Mattis <peter@cockroachlabs.com>

# nodejs is used to build and test the UI.
# bzip2 and fontconfig are used by phantomjs-prebuilt to test the UI.
# iptables is used in the acceptance tests' partition nemesis.
# parallel is used to speed up `make check`.
RUN \
 curl --silent --location https://deb.nodesource.com/setup_6.x | bash - && \
 apt-get dist-upgrade -y && \
 apt-get install --no-install-recommends --auto-remove -y \
 bzip2 \
 fontconfig \
 iptables \
 nodejs \
 parallel \
 unzip \
 && \
 apt-get clean autoclean && \
 apt-get autoremove -y && \
 git clone --depth 1 https://chromium.googlesource.com/chromium/src/tools/clang && \
 clang/scripts/update.py && \
 rm -rf /tmp/*

RUN \
  cd /usr/local/bin && \
  curl -O --silent https://releases.hashicorp.com/terraform/0.7.3/terraform_0.7.3_linux_amd64.zip && \
  unzip terraform_0.7.3_linux_amd64.zip && \
  rm terraform_0.7.3_linux_amd64.zip

ENV PATH=/opt/backtrace/bin:/third_party/llvm-build/Release+Asserts/bin:$PATH

RUN chmod -R a+w $(go env GOTOOLDIR)

CMD ["/bin/bash"]
