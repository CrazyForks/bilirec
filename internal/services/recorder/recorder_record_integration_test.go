//go:build integration

package recorder_test

import (
	"testing"
	"time"

	"github.com/bilirec/bilirec/internal/modules/bilibili"
	"github.com/bilirec/bilirec/internal/services/recorder"
	"github.com/bilirec/bilirec/internal/testutil/recording"
)

func TestFlvRecord(t *testing.T) {
	recording.RunFormatRecordTest(t, bilibili.ProfileHTTPFLV, "flv")
}

func TestTsRecord(t *testing.T) {
	recording.RunFormatRecordTest(t, bilibili.ProfileHLSTS, "ts")
}

func TestFmp4Record(t *testing.T) {
	recording.RunFormatRecordTest(t, bilibili.ProfileHLSFMP4, "fmp4")
}

func TestFlvFmp4ConcurrentRecord(t *testing.T) {
	recording.RunConcurrentFormatRecordTest(t,
		recording.ConcurrentFormatRecordSpec{Profile: bilibili.ProfileHTTPFLV, Format: "flv"},
		recording.ConcurrentFormatRecordSpec{Profile: bilibili.ProfileHLSFMP4, Format: "fmp4"},
	)
}

func TestFlvRecord_AutoStopAfterDuration(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping TestFlvRecord_AutoStopAfterDuration in short mode")
	}

	const recordDuration = 60 * time.Second
	const pollInterval = 2 * time.Second
	const tolerance = 20 * time.Second

	sess := recording.NewSession(t)
	room := recording.ResolveLiveTestRoomID(t, sess.Room)

	t.Logf("starting recording with duration limit: %v", recordDuration)
	startPhase, err := sess.Monitor.BeginPhase("auto_stop_start")
	if err != nil {
		t.Fatalf("begin start phase: %v", err)
	}
	startErr := sess.Recorder.Start(room, recorder.WithDuration(recordDuration))
	startReport := startPhase.End(t)
	recording.HandleRecordingStartErr(t, startErr)
	recording.LogCPUPhase(t, startReport)

	if status := sess.Recorder.GetStatus(room); status != recorder.Recording {
		t.Fatalf("expected status %q immediately after start, got %q", recorder.Recording, status)
	}

	deadline := time.Now().Add(recordDuration + tolerance)
	startTime := time.Now()
	for time.Now().Before(deadline) {
		<-time.After(pollInterval)
		status := sess.Recorder.GetStatus(room)
		t.Logf("elapsed: %v, status: %s", time.Since(startTime).Round(time.Second), status)
		if status == recorder.Idle {
			sess.Monitor.SnapshotMemory(t, "after_auto_stop", false)
			sess.Monitor.SnapshotGoroutines(t, "after_auto_stop")
			sess.Monitor.LogAnalysisHints(t)
			t.Logf("recording auto-stopped after ~%v as expected", recordDuration)
			return
		}
	}

	t.Errorf("recording did not auto-stop within %v (duration=%v + tolerance=%v)", recordDuration+tolerance, recordDuration, tolerance)
	sess.Recorder.Stop(room)
}
