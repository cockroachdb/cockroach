FROM cypress/included:13.13.2

RUN npm install -g pnpm@9.15.5
RUN mkdir /e2e
COPY . /e2e
WORKDIR /e2e
RUN pnpm install
