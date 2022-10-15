package dealer

import (
	"sync"
	"time"

	"go.uber.org/zap"
)

type Strategy int

const (
	Semaphore Strategy = iota
	WorkerPool
)

type Dealer struct {
	sem        chan struct{}
	shutdown   chan interface{}
	jobq       chan *Job
	logger     *zap.SugaredLogger
	wg         *sync.WaitGroup
	maxWorkers int
	strategy   Strategy
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

func (d *Dealer) WithStrategy(strategy Strategy) {
	d.logger.Debugf("")
}

func (d *Dealer) acquire() {
	d.sem <- struct{}{}
}

func (d *Dealer) release() {
	<-d.sem
}

func (d *Dealer) startWorker(n int) {
	d.logger.Debugf("worker %d is up\n", n)
	for j := range d.jobq {
		d.wg.Add(1)
		go func(j *Job) {
			j.resultch <- j.F()
			d.logger.Debugf("job %d is complete by worker: %d", j.ID, n)
			defer func() {
				d.wg.Done()
			}()
		}(j)
	}
}

func (d *Dealer) Start(debug bool) {

	go d.deal()
	d.logger.Debugf("started dealing")

	if debug {
		go d.debug()
	}

}

func (d *Dealer) Stop() {
	d.wg.Wait()
	close(d.jobq)
	close(d.shutdown)
}

func (d *Dealer) AddJob(j *Job) {
	d.jobq <- j
}

func (d *Dealer) deal() {
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

func (d *Dealer) debug() {
	t := time.NewTicker(time.Second * 5)
	for {
		select {
		case <-d.shutdown:
			t.Stop()
			return
		case <-t.C:
			d.logger.Debugf("jobs in queue: %d", len(d.jobq))
		}
	}
}
