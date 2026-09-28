package rw

import (
	"bytes"
	"errors"
	"io"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type closeUnblocksReader struct {
	once   sync.Once
	closed chan struct{}
}

func (r *closeUnblocksReader) Read([]byte) (int, error) {
	<-r.closed
	return 0, io.ErrClosedPipe
}

func (r *closeUnblocksReader) Close() error {
	r.once.Do(func() { close(r.closed) })
	return nil
}

func TestIdleTimeoutReadCloser_ReadBlocksUntilClose(t *testing.T) {
	const timeout = 30 * time.Millisecond
	raw := &closeUnblocksReader{closed: make(chan struct{})}
	wrapped := NewIdleTimeoutReadCloser(raw, timeout)
	if _, ok := wrapped.(*IdleTimeoutReadCloser); !ok {
		t.Fatalf("expected IdleTimeoutReadCloser wrapper")
	}

	done := make(chan error, 1)
	go func() {
		_, err := wrapped.Read(make([]byte, 8))
		done <- err
	}()

	select {
	case err := <-done:
		if !errors.Is(err, ErrIdleTimeout) {
			t.Fatalf("expected ErrIdleTimeout, got %v", err)
		}
	case <-time.After(2 * timeout):
		t.Fatal("read did not return after idle timeout")
	}
}

type dataThenBlockReader struct {
	data      []byte
	closeFlag atomic.Bool
	block     chan struct{}
}

func (r *dataThenBlockReader) Read(p []byte) (int, error) {
	if len(r.data) > 0 {
		n := copy(p, r.data)
		r.data = r.data[n:]
		return n, nil
	}
	<-r.block
	return 0, io.ErrClosedPipe
}

func (r *dataThenBlockReader) Close() error {
	r.closeFlag.Store(true)
	close(r.block)
	return nil
}

func TestIdleTimeoutReadCloser_NoCloseAfterReadReturns(t *testing.T) {
	const timeout = 30 * time.Millisecond
	raw := &dataThenBlockReader{data: []byte("x"), block: make(chan struct{})}
	wrapped := NewIdleTimeoutReadCloser(raw, timeout)

	n, err := wrapped.Read(make([]byte, 8))
	if n != 1 || err != nil {
		t.Fatalf("unexpected first read: n=%d err=%v", n, err)
	}

	time.Sleep(3 * timeout)

	if raw.closeFlag.Load() {
		t.Fatal("timer should not close reader when Read already returned")
	}
}

func TestIdleTimeoutReadCloser_ZeroTimeoutPassthrough(t *testing.T) {
	raw := io.NopCloser(bytes.NewReader([]byte("x")))
	wrapped := NewIdleTimeoutReadCloser(raw, 0)
	if wrapped != raw {
		t.Fatal("expected same reader when timeout is zero")
	}
}
