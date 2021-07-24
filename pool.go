package rplib

import (
	"container/list"
	"sync"
)

type cachePool struct {
	size  int
	count int
	lock  sync.RWMutex
	list  *list.List
}

func (o *cachePool) get() Command {
	o.lock.Lock()
	defer o.lock.Unlock()
	e := o.list.Front()
	if e != nil {
		value := o.list.Remove(e).(Command)
		return value
	}
	return nil
}

func (o *cachePool) put(b Command) {
	o.lock.Lock()
	defer o.lock.Unlock()
	if o.list.Len() > o.count {
		return
	}
	o.list.PushBack(b)
}

func (o *cachePool) Allocate() Command {
	old := o.get()
	if old != nil {
		return old
	}
	return Command(make([]byte, o.size))
}

func (o *cachePool) Release(b Command) {
	if len(b) != o.size {
		panic("cache size bugcheck!")
	}
	o.put(b)
}

var gPool = &cachePool{
	size:  commandCapSize,
	count: 64,
	list:  list.New(),
	lock:  sync.RWMutex{},
}
