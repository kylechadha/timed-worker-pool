package worker_pool

import (
	"testing"
	"time"

	"github.com/inconshreveable/log15"
)

func TestBatcher(t *testing.T) {
	t.Parallel()

	payloads := []*Payload{
		{text: "payload1"},
		{text: "payload2"},
	}

	// Define expectations.
	wantQueueLen := 0
	wantNumPayloads := 2

	// Define config.
	cfg := Config{
		BatchInterval: 100 * time.Millisecond,
	}

	// Construct TimedWorkerPool.
	wp := &TimedWorkerPool{
		cfg:    cfg,
		logger: log15.New("global", "worker_pool_test"),
		done:   make(chan struct{}),
		queue:  make(chan *Payload, 10),
	}

	// Test the results.
	wp.queue <- payloads[0]
	wp.queue <- payloads[1]
	sendCh := wp.batcher()
	time.Sleep(cfg.BatchInterval + time.Millisecond*10)
	batch := <-sendCh

	gotQueueLen := len(wp.queue)
	gotNumPayloads := len(batch)

	if gotNumPayloads != wantNumPayloads {
		t.Errorf("The correct number of payloads were not found in the batch. Want '%v', Got '%v'", wantNumPayloads, gotNumPayloads)
	}
	if gotQueueLen != wantQueueLen {
		t.Errorf("The incorrect number of items was in the queue. Want '%v', Got '%v'", wantQueueLen, gotQueueLen)
	}
	wp.Close()
}

func TestBatcherPanic(t *testing.T) {
	t.Parallel()

	payloads := []*Payload{
		{text: "payload1"},
		{text: "payload2"},
	}

	// Define expectations.
	wantQueueLen := 0

	// Define config.
	cfg := Config{
		BatchInterval: -100 * time.Millisecond,
	}

	// Construct TimedWorkerPool.
	wp := &TimedWorkerPool{
		cfg:    cfg,
		logger: log15.New("global", "worker_pool_test"),
		done:   make(chan struct{}),
		queue:  make(chan *Payload, 10),
	}

	// Test the results.
	wp.batcher()
	time.Sleep(1 * time.Millisecond)
	wp.cfg.BatchInterval = 100 * time.Millisecond

	wp.queue <- payloads[0]
	time.Sleep(wp.cfg.BatchInterval + time.Millisecond*10)
	gotQueueLen := len(wp.queue) // If batcher hadn't been restarted, this would be 1.

	if gotQueueLen != wantQueueLen {
		t.Errorf("The incorrect number of items were in the queue. Want '%v', Got '%v'", wantQueueLen, gotQueueLen)
	}
	wp.Close()
}

func TestWorkers(t *testing.T) {
	t.Parallel()

	payloads := []*Payload{
		{text: "payload1"},
		{text: "payload2"},
	}

	// Define expectations.
	/* See TODO below
	wantNumPayloads := 2
	*/

	// Define config.
	cfg := Config{
		BatchInterval: 100 * time.Millisecond,
		WorkerCount:   2,
	}

	// Construct TimedWorkerPool.
	wp := &TimedWorkerPool{
		cfg:    cfg,
		logger: log15.New("global", "timed_worker_test"),
		done:   make(chan struct{}),
		queue:  make(chan *Payload, 10),
	}

	// Test the results.
	sendCh := make(chan []*Payload)
	wp.startWorkers(cfg.WorkerCount, sendCh)
	sendCh <- payloads
	close(sendCh)
	wp.Close()

	// TODO: Replace doWork() with an interface that can be mocked and
	// provide validation that it was called with the correct num of
	// payloads.
}

func TestWorkersTwoRequests(t *testing.T) {
	t.Parallel()

	payloads := []*Payload{
		{text: "payload1"},
		{text: "payload2"},
		{text: "payload3"},
	}

	// Define expectations.
	wantQueueLen := 0
	/* See TODO below
	wantNumPayloads := 3
	wantNumRequests := 2
	*/

	// Define config.
	cfg := Config{
		BatchInterval: 100 * time.Millisecond,
		WorkerCount:   2,
	}

	// Construct TimedWorkerPool.
	wp := &TimedWorkerPool{
		cfg:    cfg,
		logger: log15.New("global", "timed_worker_test"),
		done:   make(chan struct{}),
		queue:  make(chan *Payload, 10),
	}

	// Test the results.
	sendCh := make(chan []*Payload)
	wp.startWorkers(cfg.WorkerCount, sendCh)
	sendCh <- payloads[0:2]
	sendCh <- payloads[2:]
	close(sendCh)
	wp.Close()

	gotQueueLen := len(wp.queue)

	// TODO: Replace doWork() with an interface that can be mocked and
	// provide validation that it was called the correct number of times
	// with the correct number of payloads.
	if gotQueueLen != wantQueueLen {
		t.Errorf("The incorrect number of items was in the queue. Want '%v', Got '%v'", wantQueueLen, gotQueueLen)
	}
	wp.Close()
}

func TestBatcherAndWorkers(t *testing.T) {
	t.Parallel()

	payloads := []*Payload{
		{text: "payload1"},
		{text: "payload2"},
	}

	// Define expectations.
	wantQueueLen := 0
	/* See TODO below
	wantNumPayloads := 2
	*/

	// Define config.
	cfg := Config{
		BatchInterval: 100 * time.Millisecond,
		WorkerCount:   2,
	}

	// Construct TimedWorkerPool.
	wp := &TimedWorkerPool{
		cfg:    cfg,
		logger: log15.New("global", "timed_worker_test"),
		done:   make(chan struct{}),
		queue:  make(chan *Payload, 10),
	}

	// Test the results.
	wp.queue <- payloads[0]
	wp.queue <- payloads[1]
	sendCh := wp.batcher()
	wp.startWorkers(cfg.WorkerCount, sendCh)
	wp.Close()

	gotQueueLen := len(wp.queue)

	// TODO: Replace doWork() with an interface that can be mocked and
	// provide validation that it was called with the correct num of
	// payloads.
	if gotQueueLen != wantQueueLen {
		t.Errorf("The incorrect number of items was in the queue. Want '%v', Got '%v'", wantQueueLen, gotQueueLen)
	}
}

func TestClose(t *testing.T) {
	t.Parallel()

	// Define expectations.
	wantErr := error(nil)

	// Define config.
	cfg := Config{
		BatchInterval: 100 * time.Millisecond,
	}

	// Construct TimedWorkerPool.
	wp := &TimedWorkerPool{
		cfg:    cfg,
		logger: log15.New("global", "worker_pool_test"),
		done:   make(chan struct{}),
		queue:  make(chan *Payload, 100),
	}

	// Test the results.
	gotErr := wp.Close()

	if gotErr != wantErr {
		t.Errorf("An unexpected error occurred. Want '%v', Got '%v'", wantErr, gotErr)
	}
}

func TestCloseTwice(t *testing.T) {
	t.Parallel()

	// Define expectations.
	wantErr := error(nil)

	// Define config.
	cfg := Config{
		BatchInterval: 100 * time.Millisecond,
	}

	// Construct TimedWorkerPool.
	wp := &TimedWorkerPool{
		cfg:    cfg,
		logger: log15.New("global", "worker_pool_test"),
		done:   make(chan struct{}),
		queue:  make(chan *Payload, 100),
	}

	// Test the results.
	gotErr := wp.Close()
	if gotErr != wantErr {
		t.Errorf("An unexpected error occurred. Want '%v', Got '%v'", wantErr, gotErr)
	}
	gotErr = wp.Close()
	if gotErr != wantErr {
		t.Errorf("An unexpected error occurred. Want '%v', Got '%v'", wantErr, gotErr)
	}
}

func TestDoWork(t *testing.T) {
	t.Parallel()

	payloads := []*Payload{
		{text: "payload1"},
		{text: "payload2"},
	}

	// Define expectations.
	wantErr := error(nil)

	// Define config.
	cfg := Config{
		BatchInterval: 100 * time.Millisecond,
		Debug:         true,
	}

	// Construct TimedWorkerPool.
	wp := &TimedWorkerPool{
		cfg:    cfg,
		logger: log15.New("global", "timed_worker_test"),
		done:   make(chan struct{}),
		queue:  make(chan *Payload, 10),
	}

	// Test the results.
	gotErr := wp.doWork(payloads)
	wp.Close()

	if gotErr != wantErr {
		t.Errorf("An unexpected error occurred. Want '%v', Got '%v'", wantErr, gotErr)
	}
}
