FROM postgres:11

RUN apt-get update && \
  DEBIAN_FRONTEND=noninteractive apt-get install --yes --no-install-recommends \
  build-essential \
  ca-certificates \
  curl \
  git \
  krb5-user

RUN curl --retry 5 https://dl.google.com/go/go1.11.5.linux-amd64.tar.gz | tar xz -C /usr/local

ENV PATH="/usr/local/go/bin:${PATH}"

COPY gss_test.go /test/

# Fetch the go packages we need but remove the script so it can be
# volume mounted at run-time enabling it to be changed without rebuilding
# the image.
RUN cd /test \
  && go get -d -t -tags gss_compose \
  && rm -rf /test

ENTRYPOINT ["/start.sh"]
