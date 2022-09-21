package dealer

import "time"

type JobFunc func() error

type Job struct {
	ID    int64
	F     JobFunc
	errch chan error
}

func NewJob(f JobFunc) *Job {
	return &Job{
		ID:    time.Now().Unix(),
		F:     f,
		errch: make(chan error),
	}
}

func (j *Job) ReadErr() error {
	return <-j.errch
}
