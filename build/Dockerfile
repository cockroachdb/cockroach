FROM golang:1.7rc4

MAINTAINER Peter Mattis <peter@cockroachlabs.com>

RUN \
 curl --silent --location https://deb.nodesource.com/setup_6.x | bash - && \
 apt-get dist-upgrade -y && \
 apt-get install --no-install-recommends --auto-remove -y bzip2 fontconfig nodejs iptables && \
 apt-get clean autoclean && \
 apt-get autoremove -y && \
 git clone --depth 1 https://chromium.googlesource.com/chromium/src/tools/clang && \
 clang/scripts/update.py && \
 rm -rf /tmp/*

ENV PATH=/opt/backtrace/bin:/third_party/llvm-build/Release+Asserts/bin:$PATH

RUN chmod -R a+w $(go env GOROOT)

CMD ["/bin/bash"]
