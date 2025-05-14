package main

import (
	"log"

	"github.com/cockroachdb/cockroach/pkg/rpc/drpc"
)

func main() {
	if err := drpc.StartServer(); err != nil {
		log.Fatal(err)
	}
}
