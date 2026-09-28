package recorder_test

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/bilirec/bilirec/internal/modules/bilibili"
	"github.com/bilirec/bilirec/internal/services/recorder"
	"github.com/bilirec/bilirec/internal/testutil/recording"
)

// Live format record tests live in recorder_record_integration_test.go (integration build tag).

// ZZZ_Final_* long soak tests run in isolated go test processes so heap/cpu pprof are
// not polluted by other integration tests. CI runs each in a separate workflow step; locally:
//
//	go test ./internal/services/recorder -run TestZZZ_Final_Concurrent3WayFlvRecord -count=1 -timeout 30m
//	go test ./internal/services/recorder -run TestZZZ_Final_Concurrent3WayFmp4Record -count=1 -timeout 30m
//
// Danmaku ZZZ final: go test ./internal/services/danmaku -run TestZZZ_Final_DanmakuJsonlRecord ...
//
// The ZZZ prefix keeps lexicographic order last when the full recorder package is
// run in one invocation (e.g. go test ./internal/services/recorder without -run).
//
// Optional: RECORDER_RECORD_PROFILE_INTERVAL_SECS=60s
func TestZZZ_Final_Concurrent3WayFlvRecord(t *testing.T) {
	recording.RunZZZFinalConcurrentRecordTest(t, bilibili.ProfileHTTPFLV, "flv", 3)
}

func TestZZZ_Final_Concurrent3WayFmp4Record(t *testing.T) {
	recording.RunZZZFinalConcurrentRecordTest(t, bilibili.ProfileHLSFMP4, "fmp4", 3)
}

func TestChannelRangeReturnedWhileStreaming(t *testing.T) {
	ch := make(chan int, 10)
	send := func() {
		for i := 0; i < 10; i++ {
			ch <- i
			time.Sleep(1 * time.Second)
		}
		close(ch)
	}
	go send()
	for v := range ch {
		t.Logf("received: %d", v)
		if v == 5 {
			t.Log("stop early")
			break
		}
	}
	<-time.After(5 * time.Second)
	for v := range ch {
		t.Logf("received after first range stopped: %d", v)
	}
}

func TestInfoOutputPath_DefaultEmpty(t *testing.T) {
	info := &recorder.Info{}
	if got := info.OutputPath(); got != "" {
		t.Fatalf("expected empty output path, got %q", got)
	}
}

func TestInfoOutputPath_AtomicConcurrentReadWrite(t *testing.T) {
	info := &recorder.Info{}
	info.SetOutputPath("")

	const writers = 8
	const readers = 8
	const loops = 2000

	var wg sync.WaitGroup

	for i := 0; i < writers; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for n := 0; n < loops; n++ {
				info.SetOutputPath(fmt.Sprintf("seg-%d-%d.flv", id, n))
			}
		}(i)
	}

	for i := 0; i < readers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for n := 0; n < loops; n++ {
				_ = info.OutputPath()
			}
		}()
	}

	wg.Wait()

	if got := info.OutputPath(); got == "" {
		t.Fatal("expected non-empty output path after concurrent writes")
	}
}
