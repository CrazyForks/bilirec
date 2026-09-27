package webhook

import (
	"sync/atomic"
	"testing"
	"time"

	"github.com/bilirec/bilirec/internal/modules/config"
	"go.uber.org/fx"
	"go.uber.org/fx/fxtest"
)

func waitWebhookDeliveriesInFlight(t *testing.T, inFlight *atomic.Int32, want int, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for inFlight.Load() < int32(want) {
		if time.Now().After(deadline) {
			t.Fatalf("in-flight webhook deliveries %d, want >= %d", inFlight.Load(), want)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func newWebhookService(t *testing.T, cfg *config.Config) *Service {
	t.Helper()
	var svc *Service
	app := fxtest.New(t,
		fx.Provide(func() *config.Config { return cfg }),
		fx.Provide(NewService),
		fx.Populate(&svc),
		fx.StartTimeout(10*time.Second),
		fx.StopTimeout(10*time.Second),
	)
	app.RequireStart()
	t.Cleanup(func() {
		app.RequireStop()
	})
	return svc
}
