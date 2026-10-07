package tests

import (
	"testing"
	"time"

	"github.com/distributed-task-scheduler/dts/pkg/cron"
)

func TestCronParseAndNext(t *testing.T) {
	// 1. Test @every macro
	sch, err := cron.Parse("@every 5s")
	if err != nil {
		t.Fatalf("failed to parse @every 5s: %v", err)
	}
	now := time.Now()
	next := sch.Next(now)
	if next.Sub(now) < 4900*time.Millisecond || next.Sub(now) > 5100*time.Millisecond {
		t.Errorf("expected ~5s interval, got %v", next.Sub(now))
	}

	// 2. Test 6-field cron with step seconds (every 10 seconds)
	sch6, err := cron.Parse("*/10 * * * * *")
	if err != nil {
		t.Fatalf("failed to parse 6-field cron: %v", err)
	}
	next6 := sch6.Next(now)
	if next6.Second()%10 != 0 {
		t.Errorf("expected second multiple of 10, got %d", next6.Second())
	}

	// 3. Test standard 5-field cron (every hour at minute 0)
	sch5, err := cron.Parse("0 * * * *")
	if err != nil {
		t.Fatalf("failed to parse 5-field cron: %v", err)
	}
	next5 := sch5.Next(now)
	if next5.Minute() != 0 {
		t.Errorf("expected minute 0, got %d", next5.Minute())
	}
}
