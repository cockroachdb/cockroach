package remote

import (
	"errors"
	"log"
	"net"
	"net/rpc"

	"github.com/mibk/dupl/job"
	"github.com/mibk/dupl/suffixtree"
	"github.com/mibk/dupl/syntax"
)

type Dupl struct {
	stree     *suffixtree.STree
	data      *[]*syntax.Node
	threshold int
	schan     chan []*syntax.Node
	mchan     <-chan suffixtree.Match
	done      chan bool
	finished  bool
}

func (d *Dupl) UpdateTree(seq []*syntax.Node, ignore *bool) error {
	if d.finished {
		return errors.New("suffix tree has been finished")
	}
	d.schan <- seq
	return nil
}

func (d *Dupl) FinishAndSetThreshold(threshold int, ignore *bool) error {
	if d.finished {
		return errors.New("suffix tree has been already finished")
	}
	d.finished = true
	close(d.schan)
	<-d.done
	d.stree.Update(&syntax.Node{Type: -1})
	d.mchan = d.stree.FindDuplOver(threshold)
	d.threshold = threshold
	return nil
}

func (d *Dupl) NextMatch(ignore bool, r *Response) error {
	if !d.finished {
		return errors.New("suffix tree is not finished yet")
	}
	for {
		m, ok := <-d.mchan
		if ok {
			r.Match = syntax.FindSyntaxUnits(*d.data, m, d.threshold)
			if len(r.Match.Frags) == 0 {
				continue
			}
		}
		r.Done = !ok
		return nil
	}
}

type Response struct {
	Match syntax.Match
	Done  bool
}

func RunServer(port string) {
	d := new(Dupl)
	rpc.Register(d)

	l, err := net.Listen("tcp", ":"+port)
	if err != nil {
		log.Fatal("error:", err)
	}
	log.Println("server started")

	for {
		if conn, err := l.Accept(); err != nil {
			log.Fatal(err.Error())
		} else {
			log.Println("connection accepted")
			d.finished = false
			d.schan = make(chan []*syntax.Node)
			d.stree, d.data, d.done = job.BuildTree(d.schan)

			rpc.ServeConn(conn)
			log.Println("done")
		}
	}
}
