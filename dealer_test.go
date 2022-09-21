package dealer

import (
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

	f := func() error {
		for i := 0; i < 50; i++ {
		}
		return nil
	}

	j := NewJob(f)
	for i := 0; i < 50; i++ {
		d.AddJob(j)
		err := j.ReadErr()
		require.NoError(t, err)
	}

	d.Stop()
}
func TestCanExecuteOneJobAsync(t *testing.T) {

	logger, _ := zap.NewProduction()

	d := New(logger.Sugar(), maxWorkers)
	d.Start(true)

	f := func() error {
		for i := 0; i < 50; i++ {
		}
		return nil
	}

	j := NewJob(f)
	wg := new(sync.WaitGroup)

	for i := 0; i < 50; i++ {
		//async
		wg.Add(1)
		go func() {
			d.AddJob(j)
			err := j.ReadErr()
			wg.Done() //important order
			require.NoError(t, err)
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
			f := func() error {
				time.Sleep(time.Millisecond * 200)
				return errors.New("err")
			}
			j := NewJob(f)
			d.AddJob(j)
			err := j.ReadErr()
			wg.Done()
			require.Error(t, err)
		}()
	}
	wg.Wait()
	d.Stop()

	elapsed := time.Now().Sub(start).Seconds()
	t.Logf("%.4f; %.4f", limit, elapsed)
	require.Less(t, elapsed, limit)

}
