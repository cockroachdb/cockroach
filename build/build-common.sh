# Build utility functions.

function check_static() {
    local libs=$(ldd $1 | egrep -v '(linux-vdso\.|librt\.|libpthread\.|libm\.|libc\.|ld-linux-)')
    if [ -n "${libs}" ]; then
        echo "$1 is not properly statically linked"
        ldd $1
        exit 1
    fi
}

# push_one_binary takes the path to the binary inside the repo.
# eg: push_one_binary sql/sql.test sql/sql-foo.test
# The file will be pushed to: s3://BUCKET_NAME/REPO_NAME/sql-foo.test.SHA
# The S3 basename will be stored in s3://BUCKET_NAME/REPO_NAME/sql-foo.test.LATEST
function push_one_binary {
  rel_path=$1
  binary_name=${2-$(dirname "${1}")}

  cd $(basename $0)/..
  time aws s3 cp ${rel_path} s3://${BUCKET_NAME}/${REPO_NAME}/${binary_name}.${SHA}

  # Upload LATEST file.
  tmpfile=$(mktemp /tmp/cockroach-push.XXXXXX)
  echo ${SHA} > ${tmpfile}
  time aws s3 cp ${tmpfile} s3://${BUCKET_NAME}/${REPO_NAME}/${binary_name}${LATEST_SUFFIX}
  rm -f ${tmpfile}
}
