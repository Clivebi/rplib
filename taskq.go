package rplib

import (
	"sort"
	"time"
)

type task struct {
	fn     func()
	time   time.Time
	signal chan int
}

type taskQueue []*task

func (o taskQueue) Len() int {
	return len(o)
}

func (o taskQueue) Swap(i, j int) {
	o[i], o[j] = o[j], o[i]
}

func (o taskQueue) Less(i, j int) bool {
	return o[j].time.After(o[i].time)
}

func pushTask(o taskQueue, t *task) taskQueue {
	o = append(o, t)
	sort.Sort(o)
	return o
}

func popTaskN(o taskQueue, count int) taskQueue {
	if count == 0 {
		return o
	}
	if o.Len() <= count {
		return taskQueue{}
	}
	return o[count:]
}

//TaskQueue the task queue,
//do task async or sync on same goroutine
type TaskQueue struct {
	tqueue taskQueue
	equeue chan *task
	iqueue chan *task
}

//NewTaskQueue new instance TaskQueue
//num,the cache size
func NewTaskQueue(num int) *TaskQueue {
	tq := &TaskQueue{
		tqueue: taskQueue{},
		equeue: make(chan *task, num),
		iqueue: make(chan *task, num),
	}
	go tq.scanLoop()
	go tq.executeLoop()
	return tq
}

//SendTask post task and wait the func exit
func (o *TaskQueue) SendTask(fn func()) {
	task := &task{
		fn:     fn,
		signal: make(chan int),
	}
	o.equeue <- task
	<-task.signal
	close(task.signal)
}

//PostDelayTask post a function to call
func (o *TaskQueue) PostDelayTask(fn func(), d time.Duration) {
	ts := &task{fn: fn, time: time.Now().Add(d)}
	o.iqueue <- ts
}

//PostTask post a function to call
func (o *TaskQueue) PostTask(fn func()) {
	ts := &task{fn: fn, time: time.Now()}
	o.equeue <- ts
}

//Stop stop task queue
func (o *TaskQueue) Stop() {
	o.equeue <- nil
	o.iqueue <- nil
}

func (o *TaskQueue) orderTask(nt *task) time.Duration {
	de := time.Minute * 10
	if nt != nil {
		o.tqueue = pushTask(o.tqueue, nt)
	}
	t := time.Now()
	count := 0
	for _, v := range o.tqueue {
		if t.After(v.time) {
			o.equeue <- v
			count++
		} else {
			break
		}
	}
	o.tqueue = popTaskN(o.tqueue, count)
	if o.tqueue.Len() > 0 {
		de = o.tqueue[0].time.Sub(t)
	}
	return de
}

func (o *TaskQueue) scanLoop() {
	var nt *task
	for {
		de := o.orderTask(nt)
		ter := time.NewTimer(de)
		select {
		case nt = <-o.iqueue:
			ter.Stop()
			if nt == nil {
				return
			}
		case <-ter.C:
			nt = nil
		}
	}
}

func (o *TaskQueue) executeLoop() {
	for {
		t := <-o.equeue
		if t == nil {
			break
		}
		t.fn()
		if t.signal != nil {
			t.signal <- 0
		}
	}
}
