// Package worker_pool implements a worker pool pattern with a maximum rate
// of work. Consumption from the consumer is throttled by an intermediary
// which batches work and places it on a read channel at a pre-specified
// interval.
package worker_pool

import (
	"bytes"
	"fmt"
	"sync"
	"time"

	"github.com/inconshreveable/log15"
)

// TimedWorkerPool reads from the queue, batching work and sending it
// downstream to be processed by a series of workers only after a minimum
// duration BatchInterval.
//
// This is useful in cases where work involves things like network requests
// that can be batched together to minimize network congestion.
type TimedWorkerPool struct {
	cfg    Config
	logger log15.Logger

	queue chan *Payload
	done  chan struct{}
	wg    sync.WaitGroup

	closeOnce sync.Once
}

// Config defines configuration for TimedWorkerPool.
type Config struct {
	BatchInterval time.Duration
	WorkerCount   int
	Retries       int
	Debug         bool
}

// Payload defines a sample payload.
type Payload struct {
	// Sample payload
}

// NewTimedWorkerPool creates a new default TimedWorkerPool.
func NewTimedWorkerPool(cfg Config, logger log15.Logger, queue chan *Payload) (TimedWorkerPool, error) {
	wp := &TimedWorkerPool{
		cfg:    cfg,
		logger: logger,
		done:   make(chan struct{}),
		queue:  queue,
	}
	sendCh := wp.batcher()
	wp.startWorkers(cfg.WorkerCount, sendCh)

	return wp, nil
}

// Close closes the done chan and waits for all workers to complete. To ensure
// no items are placed on the queue after Close is called, the sender should
// close the queue chan before calling Close.
//
// Close can be called more than once.
func (wp *TimedWorkerPool) Close() error {
	wp.closeOnce.Do(func() {
		close(wp.done)
		wp.wg.Wait()
	})

	return nil
}

// batcher periodically flushes the queue and places a batch of payloads on
// sendCh. This ensures work is not done at a rate faster than BatchInterval.
//
// batcher owns sendCh and closes it on cancellation. In the case of panic,
// recovery occurs by closing sendCh, restarting batcher and starting a new
// set of workers.
func (wp *TimedWorkerPool) batcher() <-chan []*Payload {
	sendCh := make(chan []*Payload)
	flush := func() {
		var payloads []*Payload
		for i, j := 0, len(wp.queue); i < j; i++ {
			payloads = append(payloads, <-wp.queue)
		}

		if len(payloads) > 0 {
			sendCh <- payloads
		}
	}

	go func() {
		defer func() {
			if r := recover(); r != nil {
				wp.logger.Crit("Panic occurred in batcher. Shutting down workers and restarting", "err", r)

				close(sendCh)
				sendChRecv := wp.batcher()
				wp.startWorkers(wp.cfg.WorkerCount, sendChRecv)
			}
		}()

		ticker := time.NewTicker(wp.cfg.BatchInterval)
		for {
			select {
			case <-ticker.C:
				flush()
			case <-wp.done:
				flush()

				ticker.Stop()
				close(sendCh)
				return
			}
		}
	}()
	return sendCh
}

// startWorkers spins up workers which consume batches of work from sendCh.
// Workers synchronously call doWork().
func (wp *TimedWorkerPool) startWorkers(num int, sendCh <-chan []*Payload) {
	for i := 0; i < num; i++ {
		wp.wg.Add(1)
		go func() {
			defer func() {
				if r := recover(); r != nil {
					wp.logger.Crit("Panic occurred in worker. Restarting worker", "err", r)
					wp.startWorkers(1, sendCh)
				}
			}()
			defer wp.wg.Done()

			for payloads := range sendCh {
				wp.doWork(payloads)
			}
		}()
	}
}

// doWork does work and handles retries.
func (wp *TimedWorkerPool) doWork(payloads []*Payload) (err error) {
	if wp.cfg.Debug {
		var s bytes.Buffer
		s.WriteByte('\n')
		for i, p := range payloads {
			s.WriteString(fmt.Sprintf("Payload %d: %+v\n", i, *p))
		}
		wp.logger.Debug("Doing work for the next batch of payloads", "len_payloads", len(payloads), "dump", s.String())
	}

	for i := 0; i < wp.cfg.Retries; i++ {
		// Do some work that returns an error.

		if err == nil {
			return nil
		}
		wp.logger.Warn("Doing work failed, retrying...", "err", err)
	}

	wp.logger.Error("Doing work failed permanently", "err", err)
	return err
}
