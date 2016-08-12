set -ex

cat GLOCKFILE | while read -a i; do
  case "${i[0]}" in
  cmd)
    ;;
  github.com/cockroachdb/c-protobuf)
    if [[ -d vendor/${i[0]} ]]; then
      gvt delete ${i[0]}
    fi
    gvt fetch -no-recurse --revision ${i[1]} ${i[0]}
    ;;
  github.com/coreos/etcd|github.com/gogo/protobuf|github.com/grpc-ecosystem/grpc-gateway)
    if [[ -d vendor/${i[0]} ]]; then
      gvt delete ${i[0]}
    fi
    gvt fetch -a -no-recurse  --revision ${i[1]} ${i[0]}
    ;;
  *)
    if [[ -d vendor/${i[0]} ]]; then
      gvt delete ${i[0]}
    fi
    gvt fetch -no-recurse  --revision ${i[1]} ${i[0]}
    ;;
  esac
done
