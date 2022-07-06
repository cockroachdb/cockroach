FROM alpine:3.15

RUN apk add --no-cache \
  krb5-server \
  && rm -rf /var/cache/apk/*

COPY krb5.conf /etc/krb5.conf

RUN kdb5_util create -s -P kpass \
  && kadmin.local -q "addprinc -pw psql tester@MY.EX" \
  && kadmin.local -q "addprinc -randkey postgres/gss_cockroach_1.gss_default@MY.EX"

CMD ["/start.sh"]
