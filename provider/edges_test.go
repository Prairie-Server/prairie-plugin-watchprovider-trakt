package provider

import (
	"context"
	"encoding/json"
	"math"
	"net/http"
	"testing"
	"time"

	pluginv1 "github.com/prairie-server/prairie-plugin-sdk/pkg/pluginproto/prairie/plugin/v1"
)

// Every row decoder rejects a row that is not a JSON object instead of
// emitting a half-filled state.
func TestRowDecodersRejectMalformedRows(t *testing.T) {
	t.Parallel()
	bad := json.RawMessage(`"not an object"`)
	decoders := map[string]func(json.RawMessage) ([]*pluginv1.WatchSyncRemoteState, error){
		"ratedMovie":   ratedMovieStates,
		"ratedShow":    ratedShowStates,
		"watchedMovie": watchedMovieStates,
		"watchedShow":  watchedShowStates,
		"playback":     playbackStates,
		"listedMovie":  listedMovieStates(listKind(0)),
		"listedShow":   listedShowStates(listKind(0)),
	}
	for name, decode := range decoders {
		states, err := decode(bad)
		if err == nil || states != nil {
			t.Errorf("%s: states=%v err=%v, want a decode error", name, states, err)
		}
	}
}

func TestParseRetryAfterForms(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 10, 12, 12, 0, 0, 0, time.UTC)
	cases := []struct {
		value string
		want  time.Duration
		ok    bool
	}{
		{"", 0, false},
		{"  ", 0, false},
		{"-5", 0, false},
		{"7", 7 * time.Second, true},
		{"99999999999999999", time.Duration(math.MaxInt64), true},
		{now.Add(30 * time.Second).Format(http.TimeFormat), 30 * time.Second, true},
		{now.Add(-time.Hour).Format(http.TimeFormat), 0, true},
		{"soon", 0, false},
	}
	for _, tc := range cases {
		got, ok := parseRetryAfter(tc.value, now)
		if got != tc.want || ok != tc.ok {
			t.Errorf("parseRetryAfter(%q) = %v, %v; want %v, %v", tc.value, got, ok, tc.want, tc.ok)
		}
	}
}

func TestSleepContext(t *testing.T) {
	t.Parallel()
	if err := sleepContext(context.Background(), 0); err != nil {
		t.Fatalf("zero sleep: %v", err)
	}
	if err := sleepContext(context.Background(), time.Millisecond); err != nil {
		t.Fatalf("short sleep: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := sleepContext(ctx, 0); err == nil {
		t.Fatal("cancelled zero sleep returned nil")
	}
	if err := sleepContext(ctx, time.Hour); err == nil {
		t.Fatal("cancelled sleep returned nil")
	}
}

// An idle limiter with a full bucket is dropped on the next sweep, while one
// used recently stays.
func TestCredentialLimiterSweepsIdleEntries(t *testing.T) {
	t.Parallel()
	clock := time.Now()
	l := newCredentialLimiter(time.Millisecond, 0)
	l.now = func() time.Time { return clock }
	if err := l.Wait(context.Background(), "idle"); err != nil {
		t.Fatal(err)
	}
	if got := len(l.limiters); got != 1 {
		t.Fatalf("limiters = %d, want 1", got)
	}
	clock = clock.Add(credentialLimiterIdleTTL + time.Minute)
	if err := l.Wait(context.Background(), "fresh"); err != nil {
		t.Fatal(err)
	}
	if got := len(l.limiters); got != 1 {
		t.Fatalf("limiters after sweep = %d, want only the fresh one", got)
	}
}

func TestLimiterWaitFaultClassification(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if fault := limiterWaitFault(ctx, time.Second); fault.GetCode() != pluginv1.WatchSyncFaultCode_WATCH_SYNC_FAULT_CODE_TEMPORARY {
		t.Fatalf("done ctx fault = %v, want TEMPORARY", fault.GetCode())
	}
	fault := limiterWaitFault(context.Background(), 3*time.Second)
	if fault.GetCode() != pluginv1.WatchSyncFaultCode_WATCH_SYNC_FAULT_CODE_RATE_LIMITED || fault.GetRetryAfter().AsDuration() != 3*time.Second {
		t.Fatalf("live ctx fault = %v after %v", fault.GetCode(), fault.GetRetryAfter().AsDuration())
	}
}

func TestServerClockDefaultsToWallTime(t *testing.T) {
	t.Parallel()
	before := time.Now()
	got := (&Server{}).clock()
	if got.Before(before) || time.Since(got) > time.Minute {
		t.Fatalf("clock() = %v, want about now", got)
	}
}
