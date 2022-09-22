package dealer

import "time"

type JobResult struct {
	Out interface{}
	Err error
}

func NewJobResult(out interface{}, err error) *JobResult {
	return &JobResult{
		Out: out,
		Err: err,
	}
}

type JobFunc func() *JobResult

type Job struct {
	ID       int64
	F        JobFunc
	resultch chan *JobResult
}

func NewJob(f JobFunc) *Job {
	return &Job{
		ID:       time.Now().Unix(),
		F:        f,
		resultch: make(chan *JobResult),
	}
}

func (j *Job) WaitResult() *JobResult {
	return <-j.resultch
}
