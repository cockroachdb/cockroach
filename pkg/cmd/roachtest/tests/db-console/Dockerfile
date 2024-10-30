FROM cypress/included:13.13.2

RUN apt-get -y update
RUN apt-get -y install curl
RUN curl -fsSL https://get.pnpm.io/install.sh | env SHELL=bash PNPM_HOME=/usr/local/bin PNPM_VERSION=8.6.11 sh -
RUN mkdir /e2e
COPY . /e2e
WORKDIR /e2e
RUN pnpm install
