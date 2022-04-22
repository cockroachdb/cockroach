package evalhelper

import "github.com/cockroachdb/cockroach/pkg/kv"

func EvalCtxTxnToKVTxn(txn interface{}) *kv.Txn {
	return txn.(*kv.Txn)
}
