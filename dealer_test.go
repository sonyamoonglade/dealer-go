package dealer

import (
	"bytes"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

const maxWorkers = 5

func TestCanStartStop(t *testing.T) {

	logger, _ := zap.NewProduction()

	d := New(logger.Sugar(), maxWorkers)
	d.Start(true)
	d.Stop()

}
func TestCanExecuteOneJobSync(t *testing.T) {

	logger, _ := zap.NewProduction()

	d := New(logger.Sugar(), maxWorkers)
	d.Start(true)

	f := func() *JobResult {
		for i := 0; i < 50; i++ {
		}
		return NewJobResult(nil, nil)
	}

	j := NewJob(f)
	for i := 0; i < 50; i++ {
		d.AddJob(j)
		res := j.WaitResult()
		require.NoError(t, res.Err)
	}

	d.Stop()
}
func TestCanExecuteOneJobAsync(t *testing.T) {

	logger, _ := zap.NewProduction()

	d := New(logger.Sugar(), maxWorkers)
	d.Start(true)

	f := func() *JobResult {
		for i := 0; i < 50; i++ {
		}
		return NewJobResult(nil, nil)
	}

	j := NewJob(f)
	wg := new(sync.WaitGroup)

	for i := 0; i < 50; i++ {
		//async
		wg.Add(1)
		go func() {
			d.AddJob(j)
			res := j.WaitResult()
			wg.Done() //important order
			require.NoError(t, res.Err)
		}()
	}

	wg.Wait()
	d.Stop()
}
func TestCanExecuteLongJobsAndReadErrorsAsync(t *testing.T) {

	logger, _ := zap.NewProduction()

	d := New(logger.Sugar(), maxWorkers)
	jobCount := 50

	//Assuming that f has time.Sleep = 200ms,
	//having 5 workers and 50 jobs for 200ms results in 2 seconds of execute time.
	//Add 1% of time to each of jobs, so time becomes 200ms * 1.01 = 202ms
	//Resulting totally in (50 * 202ms) / (maxWorkers * 1000ms) = 2.02 seconds
	//The success will be if:
	// - limit(2.02seconds) - actualExecutionTime > 0
	limit := float64(200) * float64(jobCount) * 1.01 / float64(maxWorkers*1000)

	d.Start(true)

	wg := new(sync.WaitGroup)
	start := time.Now()
	for i := 0; i < jobCount; i++ {
		wg.Add(1)
		go func() {
			f := func() *JobResult {
				time.Sleep(time.Millisecond * 200)
				err := errors.New("err")
				return NewJobResult(nil, err)
			}
			j := NewJob(f)
			d.AddJob(j)
			res := j.WaitResult()
			wg.Done()
			require.Error(t, res.Err)
		}()
	}
	wg.Wait()
	d.Stop()

	elapsed := time.Now().Sub(start).Seconds()
	t.Logf("%.4f; %.4f", limit, elapsed)
	require.Less(t, elapsed, limit)

}
func TestCanExecuteJobsAndReceiveOutput(t *testing.T) {

	logger, _ := zap.NewProduction()

	d := New(logger.Sugar(), maxWorkers)
	d.Start(true)

	for i := 0; i < 50; i++ {
		j := NewJob(func() *JobResult {
			buff := bytes.NewBuffer(nil)
			buff.WriteString("Hello world!")
			buff.WriteString("Hello world!")
			return NewJobResult(buff, nil)
		})
		d.AddJob(j)

		result := j.WaitResult()

		str := result.Out.(*bytes.Buffer).String()
		require.Equal(t, "Hello world!Hello world!", str)
		require.NoError(t, result.Err)
	}

	d.Stop()
}
