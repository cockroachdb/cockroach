package readline

import (
	"container/list"
	"fmt"
	"os"
	"time"
)

func sleep(n int) {
	Debug(n)
	time.Sleep(2000 * time.Millisecond)
}

// print a linked list to Debug()
func debugList(l *list.List) {
	idx := 0
	for e := l.Front(); e != nil; e = e.Next() {
		Debug(idx, fmt.Sprintf("%+v", e.Value))
		idx++
	}
}

// append log info to another file
func Debug(o ...interface{}) {
	f, _ := os.OpenFile("debug.tmp", os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
	fmt.Fprintln(f, o...)
	f.Close()
}
