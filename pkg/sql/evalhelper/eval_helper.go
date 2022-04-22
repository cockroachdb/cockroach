package evalhelper

import "github.com/cockroachdb/cockroach/pkg/kv"

func EvalCtxTxnToKVTxn(txn interface{}) *kv.Txn {
	if txn == nil {
		return nil
	}
	return txn.(*kv.Txn)
}
