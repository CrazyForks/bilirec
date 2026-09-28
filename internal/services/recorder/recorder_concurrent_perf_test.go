//go:build integration

package recorder_test

import (
	"github.com/bilirec/bilirec/internal/testutil/recording"
	"sort"
	"sync"
	"testing"
	"time"
)

func TestRecorder_ConcurrentStartBurst_Latency(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping burst latency test in short mode")
	}
	t.Setenv("MAX_CONCURRENT_RECORDINGS", "3")

	sess := recording.NewSession(t)
	rooms := recording.ResolveLiveTestRoomIDs(t, sess.Room, 4)

	burstPhase, err := sess.Monitor.BeginPhase("concurrent_start_burst")
	if err != nil {
		t.Fatalf("begin burst phase: %v", err)
	}

	startGate := make(chan struct{})
	latencies := make([]time.Duration, 0, len(rooms))
	var latMu sync.Mutex
	var wg sync.WaitGroup
	for _, roomID := range rooms {
		rid := roomID
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-startGate
			begin := time.Now()
			_ = sess.Recorder.Start(rid)
			lat := time.Since(begin)
			latMu.Lock()
			latencies = append(latencies, lat)
			latMu.Unlock()
		}()
	}
	close(startGate)
	wg.Wait()

	burstReport := burstPhase.End(t)
	recording.LogCPUPhase(t, burstReport)
	sess.Monitor.SnapshotGoroutines(t, "after_burst")
	sess.Monitor.SnapshotMemory(t, "after_burst", false)

	if len(latencies) == 0 {
		t.Fatal("no latency samples collected")
	}
	sort.Slice(latencies, func(i, j int) bool { return latencies[i] < latencies[j] })
	p95 := latencies[(len(latencies)-1)*95/100]
	t.Logf("concurrent start burst latency samples=%d p95=%s max=%s", len(latencies), p95, latencies[len(latencies)-1])

	for _, roomID := range rooms {
		sess.Recorder.Stop(roomID)
	}
	recording.WaitUntilNoActiveRecordings(t, sess.Recorder, 12*time.Second)
	sess.Monitor.LogAnalysisHints(t)
}
