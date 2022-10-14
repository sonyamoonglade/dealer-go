package dealer

import (
	"sync"

	"go.uber.org/zap"
)

//Default Strategy is Semaphore
type Strategy int

const (
	Semaphore Strategy = iota
	WorkerPool
)

var started bool

type Dealer struct {
	sem        chan struct{}
	shutdown   chan interface{}
	jobq       chan *Job
	logger     *zap.SugaredLogger
	wg         *sync.WaitGroup
	strategy   Strategy
	maxWorkers int
}

func New(logger *zap.SugaredLogger, maxWorkers int) *Dealer {
	return &Dealer{
		logger:     logger,
		maxWorkers: maxWorkers,
		sem:        make(chan struct{}, maxWorkers),
		jobq:       make(chan *Job, maxWorkers),
		shutdown:   make(chan interface{}),
		wg:         new(sync.WaitGroup),
	}
}

//Sets strategy to a dealer instance
func (d *Dealer) WithStrategy(strategy Strategy) {
	d.strategy = strategy
}

func (d *Dealer) Start(debug bool) {
	started = true
	switch d.strategy {
	case Semaphore:
		go d.startWithSemaphore()
		d.logger.Debugf("dealing has started with semaphore")
	case WorkerPool:
		go d.startWorkerPool()
		d.logger.Debugf("dealing has started with workerPool")
	}

}

func (d *Dealer) Stop() {
	started = false
	//If worker pool is selected, should close jobq first, to stop all workers
	switch d.strategy {
	case Semaphore:
		d.wg.Wait()
		close(d.jobq)
		close(d.shutdown)
	case WorkerPool:
		close(d.jobq)
		d.wg.Wait()
		close(d.shutdown)
	}
}

//Adds job to job queue
func (d *Dealer) AddJob(j *Job) {
	if !started {
		panic("dealer has not started yet!")
	}

	d.jobq <- j
}

func (d *Dealer) startWorkerPool() {
	for n := 1; n <= d.maxWorkers; n++ {
		d.wg.Add(1)
		go d.startWorker(n)
	}
}

func (d *Dealer) startWorker(n int) {
	d.logger.Debugf("worker %d is up\n", n)
	for j := range d.jobq {
		j.resultch <- j.F()
	}
	defer d.wg.Done()
}

func (d *Dealer) startWithSemaphore() {
	for j := range d.jobq {
		d.acquire()
		d.wg.Add(1)
		go func(j *Job) {
			j.resultch <- j.F()
			d.logger.Debugf("job %d is complete", j.ID)
			defer func() {
				d.wg.Done()
				d.release()
			}()
		}(j)
	}
}

func (d *Dealer) acquire() {
	d.sem <- struct{}{}
}

func (d *Dealer) release() {
	<-d.sem
}
