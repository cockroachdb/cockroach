FROM golang:1.7.3

MAINTAINER Peter Mattis <peter@cockroachlabs.com>

RUN \
 apt-key adv --keyserver pgp.mit.edu --recv D101F7899D41F3C3 && \
 echo 'deb http://dl.yarnpkg.com/debian/ stable main' | tee /etc/apt/sources.list.d/yarn.list

# nodejs is used to build and test the UI.
# bzip2 and fontconfig are used by phantomjs-prebuilt to test the UI.
# iptables is used in the acceptance tests' partition nemesis.
# yarn is the dependency manager for the UI, as an alternative to npm.
RUN \
 curl --silent --location https://deb.nodesource.com/setup_6.x | bash - && \
 apt-get dist-upgrade -y && \
 apt-get install --no-install-recommends --auto-remove -y \
 bzip2 \
 fontconfig \
 iptables \
 nodejs \
 unzip \
 yarn \
 && \
 apt-get clean autoclean && \
 apt-get autoremove -y && \
 git clone --depth 1 https://chromium.googlesource.com/chromium/src/tools/clang && \
 clang/scripts/update.py && \
 rm -rf /tmp/*

RUN \
 cd /usr/local/bin && \
 curl --silent https://releases.hashicorp.com/terraform/0.7.7/terraform_0.7.7_linux_amd64.zip -o terraform_linux_amd64.zip && \
 unzip terraform_linux_amd64.zip && \
 rm terraform_linux_amd64.zip

ENV PATH=/opt/backtrace/bin:/third_party/llvm-build/Release+Asserts/bin:$PATH

RUN chmod -R a+w $(go env GOTOOLDIR)

CMD ["/bin/bash"]
