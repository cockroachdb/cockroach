FROM python:3.10
ENV PYTHONUNBUFFERED 1

RUN wget --quiet -O - https://www.postgresql.org/media/keys/ACCC4CF8.asc | apt-key add - && \
  echo "deb http://apt.postgresql.org/pub/repos/apt/ bullseye-pgdg main" | tee  /etc/apt/sources.list.d/pgdg.list && \
  apt-get update && \
  DEBIAN_FRONTEND=noninteractive apt-get install --yes --no-install-recommends \
  curl \
  krb5-user \
  postgresql-client-11

RUN curl -fsSL "https://github.com/benesch/autouseradd/releases/download/1.3.0/autouseradd-1.3.0-amd64.tar.gz" -o autouseradd.tar.gz \
  && echo "442dae58b727a79f81368127fac141d7f95501ffa05f8c48943d27c4e807deb7 autouseradd.tar.gz" | sha256sum -c - \
  && tar xzf autouseradd.tar.gz --strip-components 1 \
  && rm autouseradd.tar.gz

RUN mkdir /code
WORKDIR /code
COPY requirements.txt /code/
RUN pip install -r requirements.txt
COPY . /code/

ENTRYPOINT ["autouseradd", "--user", "roach", "--no-create-home", "/start.sh"]
