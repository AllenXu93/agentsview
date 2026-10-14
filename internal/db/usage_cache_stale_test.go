//go:build fts5

package db

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"go.kenn.io/agentsview/internal/money"
)

func staleUsageFixture(t *testing.T, daemon bool) (*DB, *usageCache) {
	t.Helper()
	database := testDB(t)
	seedLiveWriteSession(t, database, "session", "project", "2026-08-10T09:00:00Z", 0, 10)
	require.Equal(t, 10, liveWriteDailyInput(t, database, "project"))
	if daemon {
		require.NoError(t, database.StartUsageCacheBackfill(t.Context()))
		require.NoError(t, database.WaitUsageCacheBackfill(t.Context()))
	}
	_, _, cache := prepareUsageRollupTest(t, database)
	return database, cache
}

func changeStaleUsage(t *testing.T, database *DB, marker time.Time) {
	t.Helper()
	_, err := database.getWriter().Exec(t.Context(), `UPDATE messages SET token_usage = '{"input_tokens":17}' WHERE session_id = 'session'; UPDATE sessions SET transcript_revision = 'changed', created_at = '2026-08-10T09:00:00Z', local_modified_at = ? WHERE id = 'session'`, marker.UTC().Format(time.RFC3339Nano))
	require.NoError(t, err)
}

func TestUsageStaleAppendRefreshesWithoutWaiting(t *testing.T) {
	for _, heldWriter := range []bool{false, true} {
		t.Run(map[bool]string{false: "held fill", true: "held writer"}[heldWriter], func(t *testing.T) {
			database, cache := staleUsageFixture(t, true)
			started, release, batches := make(chan struct{}), make(chan struct{}), make(chan struct{}, 16)
			var first atomic.Bool
			cache.fill.observer.beforeExtract = func([]usageSourceVersion) {
				if !first.Swap(true) {
					close(started)
					<-release
				}
			}
			t.Cleanup(func() {
				select {
				case <-release:
				default:
					close(release)
				}
			})
			cache.fill.observer.afterNotificationBatch = func() { batches <- struct{}{} }
			appendLiveWriteMessage(t, database, "session", "2026-08-10T10:00:00Z", 1, 7)
			waitUsageNotification(t, started)
			held := openRawUsageCacheTestDB(t, cache.path)
			defer held.Close()
			if heldWriter {
				_, err := held.ExecContext(t.Context(), `BEGIN IMMEDIATE`)
				require.NoError(t, err)
				defer func() { _, _ = held.ExecContext(t.Context(), `ROLLBACK`) }()
			}
			ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
			defer cancel()
			for range 5 {
				daily, err := database.GetDailyUsage(ctx, UsageFilter{Timezone: "UTC"})
				require.NoError(t, err)
				assert.Equal(t, 10, daily.Totals.InputTokens)
				assert.Equal(t, 1, daily.SessionCounts.Total)
			}
			cache.fill.mu.Lock()
			queued := len(cache.fill.notify) + len(cache.fill.overflow)
			cache.fill.mu.Unlock()
			assert.Positive(t, queued, "reads must queue recovery while a batch is running")
			if heldWriter {
				_, err := held.ExecContext(t.Context(), `COMMIT`)
				require.NoError(t, err)
			}
			close(release)
			waitUsageNotification(t, batches)
			assert.Equal(t, 17, liveWriteDailyInput(t, database, "project"))
		})
	}
}

func TestUsageStaleRejectedStatesStayExact(t *testing.T) {
	for _, state := range []string{"no install", "no facts", "old marker", "future marker", "pending rollup", "pricing", "cursor", "standalone"} {
		t.Run(state, func(t *testing.T) {
			database, cache := staleUsageFixture(t, state != "standalone")
			changeStaleUsage(t, database, time.Now().Add(-time.Second))
			want := 17
			switch state {
			case "no install", "no facts":
				table := "usage_rollup_installs"
				if state == "no facts" {
					table = "usage_cached_sessions"
				}
				_, err := cache.db.ExecContext(t.Context(), `DELETE FROM `+table+` WHERE session_id = 'session'`)
				require.NoError(t, err)
			case "old marker":
				changeStaleUsage(t, database, time.Now().Add(-time.Minute))
			case "future marker":
				changeStaleUsage(t, database, time.Now().Add(time.Minute))
			case "pending rollup":
				prepareUsageRollupTest(t, database)
			case "pricing":
				require.NoError(t, database.UpsertModelPricing([]ModelPricing{{ModelPattern: "model-a", InputPerMTok: money.Money{Microdollars: 1_000_000}}}))
			case "cursor":
				require.NoError(t, database.InsertCursorUsageEvents(t.Context(), []CursorUsageEvent{{OccurredAt: "2026-08-10T09:00:00Z", Model: "cursor-model", InputTokens: 1, DedupKey: "cursor-one"}}))
				want = 18
			case "standalone":
			}
			daily, err := database.GetDailyUsage(t.Context(), UsageFilter{Timezone: "UTC"})
			require.NoError(t, err)
			assert.Equal(t, want, daily.Totals.InputTokens)
		})
	}
}

func TestUsageStaleDedupUsesCompatibleFacts(t *testing.T) {
	for _, refill := range []bool{false, true} {
		t.Run(map[bool]string{false: "facts pending with narrower read", true: "refilled invalidates peer"}[refill], func(t *testing.T) {
			database := testDB(t)
			for _, id := range []string{"a", "b"} {
				seedUsageSnapshotSession(t, database, id, id, "2026-08-10T09:00:00Z", 0, 10, "model-a")
			}
			filter := UsageFilter{Timezone: "UTC"}
			daily, err := database.GetDailyUsage(t.Context(), filter)
			require.NoError(t, err)
			require.Equal(t, 10, daily.Totals.OutputTokens)
			require.NoError(t, database.StartUsageCacheBackfill(t.Context()))
			require.NoError(t, database.WaitUsageCacheBackfill(t.Context()))
			_, _, cache := prepareUsageRollupTest(t, database)
			_, err = database.getWriter().Exec(t.Context(), `UPDATE messages SET token_usage = '{}' WHERE session_id = 'a'; UPDATE sessions SET transcript_revision = 'changed', local_modified_at = ? WHERE id = 'a'`, time.Now().Add(-time.Second).UTC().Format(time.RFC3339Nano))
			require.NoError(t, err)
			if refill {
				prepareUsageRollupTest(t, database)
			} else {
				_, err = cache.db.ExecContext(t.Context(), `DELETE FROM usage_rollup_installs WHERE session_id = 'b'`)
				require.NoError(t, err)
				_, err = database.GetDailyUsage(t.Context(), UsageFilter{Timezone: "UTC", Project: "b"})
				require.NoError(t, err)
			}
			var builds atomic.Int32
			cache.rollup.observer.beforeEnsure = func() { builds.Add(1) }
			daily, err = database.GetDailyUsage(t.Context(), filter)
			require.NoError(t, err)
			assert.Equal(t, 10, daily.Totals.OutputTokens)
			assert.Equal(t, refill, builds.Load() > 0)
		})
	}
}

func TestUsageNotificationContinuousArrivalsReachBatchCap(t *testing.T) {
	database, cache := staleUsageFixture(t, true)
	changeStaleUsage(t, database, time.Now().Add(-time.Second))
	batches := make(chan struct{}, 16)
	cache.fill.observer.afterNotificationBatch = func() { batches <- struct{}{} }
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	guard := time.NewTimer(3 * time.Second)
	defer guard.Stop()
	cache.fill.enqueueSessions([]string{"session"})
	for {
		select {
		case <-ticker.C:
			cache.fill.enqueueSessions([]string{"session"})
		case <-batches:
			assert.Equal(t, 17, liveWriteDailyInput(t, database, "project"))
			return
		case <-guard.C:
			require.FailNow(t, "continuous notifications postponed refresh")
		}
	}
}
