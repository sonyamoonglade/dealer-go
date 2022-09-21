package dealer

import (
	"sync"
	"time"

	"go.uber.org/zap"
)

type Dealer struct {
	sem      chan struct{}
	logger   *zap.SugaredLogger
	jobq     chan *Job
	shutdown chan interface{}
	wg       *sync.WaitGroup
}

func New(logger *zap.SugaredLogger, maxWorkers int) *Dealer {
	return &Dealer{
		sem:      make(chan struct{}, maxWorkers),
		jobq:     make(chan *Job, maxWorkers),
		logger:   logger,
		shutdown: make(chan interface{}),
		wg:       new(sync.WaitGroup),
	}
}

func (d *Dealer) Acquire() {
	d.sem <- struct{}{}
}

func (d *Dealer) Release() {
	<-d.sem
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
		d.Acquire()
		d.wg.Add(1)
		go func(j *Job) {
			j.errch <- j.F()
			d.logger.Debugf("job %d is complete", j.ID)
			defer func() {
				d.wg.Done()
				d.Release()
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
