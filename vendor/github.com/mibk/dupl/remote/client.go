package remote

import (
	"log"
	"math"
	"net/rpc"
	"sync"

	"github.com/mibk/dupl/syntax"
)

type router struct {
	Slots map[int][]*rpc.Client
}

func newRouter() *router {
	return &router{make(map[int][]*rpc.Client)}
}

func (r *router) AddClient(slotId int, client *rpc.Client) {
	if _, ok := r.Slots[slotId]; !ok {
		r.Slots[slotId] = make([]*rpc.Client, 0, 1)
	}
	r.Slots[slotId] = append(r.Slots[slotId], client)
}

type worker struct {
	clients  []*rpc.Client
	router   *router
	batchCnt int
	verbose  bool
}

func newWorker(addrs []string) *worker {
	w := &worker{}
	usedAddrs := make(map[string]bool)
	w.clients = make([]*rpc.Client, 0, len(addrs))
	for _, addr := range addrs {
		if _, present := usedAddrs[addr]; present {
			// ignore duplicate addr
			continue
		}
		client, err := rpc.Dial("tcp", addr)
		if err != nil {
			log.Fatal(err)
		}
		w.clients = append(w.clients, client)
		usedAddrs[addr] = true
	}

	w.batchCnt = batchCount(len(w.clients))

	w.router = newRouter()
	id := 0
	for i := 0; i < w.batchCnt-1; i++ {
		for j := i + 1; j < w.batchCnt; j++ {
			w.router.AddClient(i, w.clients[id])
			w.router.AddClient(j, w.clients[id])
			id = (id + 1) % len(w.clients)
		}
	}
	return w
}

func (w *worker) Work(schan chan []*syntax.Node, duplChan chan syntax.Match, threshold int) {
	if w.verbose {
		log.Println("Servers are building suffix tree")
	}
	batch := 0
	var wg sync.WaitGroup
	for seq := range schan {
		for _, client := range w.router.Slots[batch] {
			seq, client := seq, client
			wg.Add(1)
			go func() {
				defer wg.Done()
				err := client.Call("Dupl.UpdateTree", seq, nil)
				if err != nil {
					log.Fatal(err)
				}
			}()
		}
		batch = (batch + 1) % w.batchCnt
	}

	wg.Wait()
	if w.verbose {
		log.Println("Servers are searching for clones")
	}
	clientCnt := len(w.clients)
	for _, client := range w.clients {
		client := client
		go func() {
			err := client.Call("Dupl.FinishAndSetThreshold", threshold, nil)
			if err != nil {
				log.Fatal(err)
			}
			for {
				var reply Response
				err := client.Call("Dupl.NextMatch", false, &reply)

				if err != nil {
					log.Fatal(err)
				}
				if reply.Done {
					clientCnt--
					if clientCnt == 0 {
						close(duplChan)
					}
					return
				}
				duplChan <- reply.Match
			}
		}()
	}
}

// batchCount returns number of batches for the given number
// of clients.
func batchCount(clientsCnt int) int {
	return int(math.Ceil(math.Sqrt(2*float64(clientsCnt)+0.25) + 0.5))
}

func RunClient(addrs []string, threshold int, schan chan []*syntax.Node, verbose bool) <-chan syntax.Match {
	w := newWorker(addrs)
	w.verbose = verbose
	if verbose {
		log.Println("Connections established")
	}

	duplChan := make(chan syntax.Match)
	go w.Work(schan, duplChan, threshold)

	return duplChan
}
