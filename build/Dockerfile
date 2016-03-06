FROM golang:1.6

MAINTAINER Peter Mattis <peter@cockroachlabs.com>

RUN \
 curl --silent --location https://deb.nodesource.com/setup_5.x | bash - && \
 apt-get dist-upgrade -y && \
 apt-get install --no-install-recommends --auto-remove -y nodejs iptables && \
 apt-get clean autoclean && \
 apt-get autoremove -y && \
 git clone --depth 1 https://chromium.googlesource.com/chromium/src/tools/clang && \
 clang/scripts/update.py && \
 rm -rf /tmp/*

ENV PATH=/third_party/llvm-build/Release+Asserts/bin:$PATH

ENV SKIP_BOOTSTRAP=1

CMD ["/bin/bash"]
