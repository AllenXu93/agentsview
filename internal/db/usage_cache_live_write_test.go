//go:build fts5

package db

import (
	"encoding/json"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// seedLiveWriteSession inserts one session whose message carries a unique
// Claude identity, so sessions in these tests never share a dedup group.
func seedLiveWriteSession(
	t *testing.T, database *DB, id, project, timestamp string,
	ordinal, input int,
) {
	t.Helper()
	started := timestamp
	insertSession(t, database, id, project, func(session *Session) {
		session.StartedAt = &started
		session.Agent = "claude"
	})
	appendLiveWriteMessage(t, database, id, timestamp, ordinal, input)
}

func appendLiveWriteMessage(
	t *testing.T, database *DB, id, timestamp string, ordinal, input int,
) {
	t.Helper()
	suffix := id + "-" + strconv.Itoa(ordinal)
	require.NoError(t, database.InsertMessages(t.Context(), []Message{{
		SessionID: id, Ordinal: ordinal, Role: "assistant",
		Timestamp: timestamp, Model: "model-a",
		TokenUsage: json.RawMessage(
			`{"input_tokens":` + strconv.Itoa(input) + `}`),
		ClaudeMessageID: "message-" + suffix,
		ClaudeRequestID: "request-" + suffix,
	}}))
}

func liveWriteDailyInput(t *testing.T, database *DB, project string) int {
	t.Helper()
	daily, err := database.GetDailyUsage(t.Context(), UsageFilter{
		From: "2026-08-10", To: "2026-08-10", Timezone: "UTC",
		Project: project, SkipSessionCounts: true,
	})
	require.NoError(t, err)
	return daily.Totals.InputTokens
}

// A rollup build reads only committed per-session facts, so an archive write
// landing while it runs can never abort it. The rollup coordinator's
// beforeEnsure seam runs on the detached build goroutine, after this request's
// facts are already filled, which is exactly the window that used to fail.
func TestUsageSummarySurvivesArchiveWriteDuringRollupBuild(t *testing.T) {
	for _, testCase := range []struct {
		name          string
		writtenID     string
		wantDuring    []int
		wantAfterFill int
	}{
		{
			// A session the summary does not report cannot influence it at
			// all, so the answer is unchanged in both directions.
			name: "write outside the filter", writtenID: "other-project",
			wantDuring: []int{10}, wantAfterFill: 10,
		},
		{
			// An in-scope append is either invisible to this request or
			// already part of it, but never an error.
			name: "append inside the filter", writtenID: "reported",
			wantDuring: []int{10, 17}, wantAfterFill: 17,
		},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			database := testDB(t)
			seedLiveWriteSession(t, database, "reported", "keep",
				"2026-08-10T09:00:00Z", 0, 10)
			seedLiveWriteSession(t, database, "other-project", "drop",
				"2026-08-10T09:30:00Z", 0, 4)
			require.Equal(t, 10, liveWriteDailyInput(t, database, "keep"))

			snapshot, err := database.captureUsageQuery(
				t.Context(), UsageFilter{}, usageQueryKindToken)
			require.NoError(t, err)
			cache, err := database.usageCache.Generation(
				t.Context(), snapshot.DatabaseID)
			require.NoError(t, err)
			var written atomic.Bool
			cache.rollup.observer.beforeEnsure = func() {
				if written.Swap(true) {
					return
				}
				appendLiveWriteMessage(t, database, testCase.writtenID,
					"2026-08-10T09:45:00Z", 1, 7)
			}
			assert.Contains(t, testCase.wantDuring,
				liveWriteDailyInput(t, database, "keep"))
			assert.True(t, written.Load(), "the build seam never ran")

			cache.rollup.observer.beforeEnsure = nil
			assert.Equal(t, testCase.wantAfterFill,
				liveWriteDailyInput(t, database, "keep"))
		})
	}
}

// The coverage pass used to restart itself, and then give up, whenever the
// archive moved underneath it. Facts now come from each session's own read
// snapshot, so a pass finishes against a continuously written archive.
func TestUsageCacheBackfillCompletesUnderConcurrentArchiveWrites(t *testing.T) {
	database := testDB(t)
	for index := range 4 {
		id := "churn-" + strconv.Itoa(index)
		seedLiveWriteSession(t, database, id, "project",
			"2026-08-10T09:00:00Z", 0, 1)
	}
	snapshot, err := database.captureUsageQuery(
		t.Context(), UsageFilter{}, usageQueryKindToken)
	require.NoError(t, err)
	cache, err := database.usageCache.Generation(
		t.Context(), snapshot.DatabaseID)
	require.NoError(t, err)
	// Write into the archive from both phases of the pass: while facts are
	// being extracted, and while the rollups are being aggregated.
	var appends atomic.Int32
	churn := func() {
		ordinal := int(appends.Add(1))
		suffix := "churn-0-" + strconv.Itoa(ordinal)
		_, err := database.getWriter().Exec(t.Context(), `INSERT INTO messages(session_id, ordinal, role, content, timestamp, model, token_usage, claude_message_id, claude_request_id) VALUES ('churn-0', ?, 'assistant', '', '2026-08-10T09:00:00Z', 'model-a', '{"input_tokens":1}', ?, ?); UPDATE sessions SET transcript_revision = ?, local_modified_at = ? WHERE id = 'churn-0'`, ordinal, "message-"+suffix, "request-"+suffix, suffix, time.Now().UTC().Format(time.RFC3339Nano))
		require.NoError(t, err)
	}
	cache.fill.observer.afterExtract = func([]usageSourceVersion) { churn() }
	cache.rollup.observer.beforeEnsure = churn

	require.NoError(t, database.StartUsageCacheBackfill(t.Context()))
	require.NoError(t, database.WaitUsageCacheBackfill(t.Context()))
	written := int(appends.Load())
	require.Positive(t, written)
	assert.Equal(t, 4, usageCacheCount(t, cache, "usage_cached_sessions"))
	assert.Equal(t, 4, usageCacheCount(t, cache, "usage_rollup_installs"))

	cache.fill.observer.afterExtract = nil
	cache.rollup.observer.beforeEnsure = nil
	daily, err := database.GetDailyUsage(t.Context(), UsageFilter{
		From: "2026-08-10", To: "2026-08-10", Timezone: "UTC",
		Project: "project", SkipSessionCounts: true, RequireFresh: true,
	})
	require.NoError(t, err)
	assert.Equal(t, 4+written, daily.Totals.InputTokens)
}

func waitUsageNotification(t *testing.T, batches <-chan struct{}) {
	t.Helper()
	select {
	case <-batches:
	case <-time.After(10 * time.Second):
		require.FailNow(t, "usage notification batch did not finish")
	}
}

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
	now := time.Now().Add(time.Hour)
	cache.fill.now = func() time.Time { return now }
	return database, cache
}

func changeStaleUsage(t *testing.T, database *DB, marker time.Time) {
	t.Helper()
	_, err := database.getWriter().Exec(t.Context(), `UPDATE messages SET token_usage = '{"input_tokens":17}' WHERE session_id = 'session'; UPDATE sessions SET transcript_revision = 'changed', created_at = '2026-08-10T09:00:00Z', local_modified_at = ? WHERE id = 'session'`, marker.UTC().Format(time.RFC3339Nano))
	require.NoError(t, err)
}

func TestUsageNotificationContinuousArrivalsReachBatchCap(t *testing.T) {
	database, cache := staleUsageFixture(t, true)
	changeStaleUsage(t, database, cache.fill.now().Add(-time.Second))
	batches := make(chan struct{}, 16)
	cache.fill.observer.afterNotificationBatch = func() { batches <- struct{}{} }
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	guard := time.NewTimer(10 * usageFillNotificationBatchCap)
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

func TestUsageNotificationRefreshesWrittenSession(t *testing.T) {
	for _, daemon := range []bool{false, true} {
		t.Run(strconv.FormatBool(daemon), func(t *testing.T) {
			database := testDB(t)
			seedLiveWriteSession(t, database, "session", "project", "2026-08-10T09:00:00Z", 0, 10)
			for _, zone := range []string{"UTC", "America/Chicago"} {
				_, err := database.GetDailyUsage(t.Context(), UsageFilter{Timezone: zone})
				require.NoError(t, err)
			}
			snapshot, err := database.captureUsageQuery(t.Context(), UsageFilter{}, usageQueryKindToken)
			require.NoError(t, err)
			cache, err := database.usageCache.Generation(t.Context(), snapshot.DatabaseID)
			require.NoError(t, err)
			batches := make(chan struct{}, 16)
			cache.fill.observer.afterNotificationBatch = func() { batches <- struct{}{} }
			if daemon {
				require.NoError(t, database.StartUsageCacheBackfill(t.Context()))
				require.NoError(t, database.WaitUsageCacheBackfill(t.Context()))
			}
			var installs atomic.Int32
			cache.rollup.observer.beforeInstall = func([]usageRollupBuild) { installs.Add(1) }
			appendLiveWriteMessage(t, database, "session", "2026-08-10T10:00:00Z", 1, 7)
			waitUsageNotification(t, batches)
			background := installs.Load()
			if daemon {
				assert.Positive(t, background)
			} else {
				assert.Zero(t, background)
			}
			for _, zone := range []string{"UTC", "America/Chicago"} {
				daily, err := database.GetDailyUsage(t.Context(), UsageFilter{Timezone: zone})
				require.NoError(t, err)
				assert.Equal(t, 17, daily.Totals.InputTokens)
			}
			if daemon {
				assert.Equal(t, background, installs.Load(), "requests rebuilt already warmed totals")
			}
		})
	}
}

func TestUsageNotificationLeavesCursorRollupForRequest(t *testing.T) {
	database := testDB(t)
	seedLiveWriteSession(t, database, "session", "project", "2026-08-10T09:00:00Z", 0, 10)
	_, cache := usageCursorBatchFailureFixture(t, database)
	database.usageBackfillMu.Lock()
	database.usageBackfillEnabled = true
	database.usageBackfillMu.Unlock()
	var rollupAttempts int
	cache.rollup.observer.beforeEnsure = func() {
		rollupAttempts++
		assert.Equal(t, usageCursorCopyBatchSize, usageCacheCount(t, cache, "cursor_usage_facts"), "rollup refresh delayed Cursor fact progress")
	}
	cache.fill.processNotificationBatch(map[string]bool{"session": true}, true)
	assert.Positive(t, rollupAttempts)
	cache.rollup.observer.beforeEnsure = nil
	var cursorInstalls int
	require.NoError(t, cache.db.QueryRowContext(t.Context(), `SELECT COUNT(*) FROM usage_rollup_installs WHERE session_id = ?`, usageRollupCursorSessionID).Scan(&cursorInstalls))
	assert.Zero(t, cursorInstalls, "session refresh stamped incomplete Cursor facts as current")
	_, err := cache.db.ExecContext(t.Context(), `DROP TRIGGER reject_late_cursor_fact`)
	require.NoError(t, err)
	daily, err := database.GetDailyUsage(t.Context(), UsageFilter{Timezone: "UTC"})
	require.NoError(t, err)
	assert.Equal(t, 1011, daily.Totals.InputTokens)
}

func TestUsageNotificationChannelOverflow(t *testing.T) {
	t.Run("peers", func(t *testing.T) {
		database := testDB(t)
		for _, id := range []string{"peer-a", "peer-b"} {
			seedUsageSnapshotSession(t, database, id, "project", "2026-08-10T09:00:00Z", 0, 10, "model-a")
		}
		_, err := database.GetDailyUsage(t.Context(), UsageFilter{Timezone: "UTC"})
		require.NoError(t, err)
		snapshot, err := database.captureUsageQuery(t.Context(), UsageFilter{}, usageQueryKindToken)
		require.NoError(t, err)
		cache, err := database.usageCache.Generation(t.Context(), snapshot.DatabaseID)
		require.NoError(t, err)
		blocked, release := make(chan struct{}), make(chan struct{})
		t.Cleanup(func() { close(release) })
		batches := make(chan struct{}, 16)
		var paused atomic.Bool
		cache.fill.observer.afterNotificationBatch = func() {
			if !paused.Swap(true) {
				close(blocked)
				<-release
			}
			batches <- struct{}{}
		}
		require.NoError(t, database.StartUsageCacheBackfill(t.Context()))
		require.NoError(t, database.WaitUsageCacheBackfill(t.Context()))
		database.usageCache.NotifySessions([]string{"missing"})
		waitUsageNotification(t, blocked)
		ids := make([]string, cap(cache.fill.notify))
		for i := range ids {
			ids[i] = "missing"
		}
		database.usageCache.NotifySessions(ids)
		seedUsageSnapshotSession(t, database, "sibling", "project", "2026-08-10T10:00:00Z", 0, 10, "model-a")
		snapshot, err = database.captureUsageQuery(t.Context(), UsageFilter{}, usageQueryKindToken)
		require.NoError(t, err)
		_, err = cache.fill.Ensure(t.Context(), snapshot.Versions, 0)
		require.NoError(t, err)
		var installs atomic.Int32
		cache.rollup.observer.beforeInstall = func([]usageRollupBuild) { installs.Add(1) }
		release <- struct{}{}
		for {
			waitUsageNotification(t, batches)
			var warmed int
			require.NoError(t, cache.db.QueryRowContext(t.Context(), `SELECT COUNT(*)
				FROM usage_rollup_installs i JOIN usage_rollup_timezones z ON z.id = i.timezone_id
				WHERE z.timezone_name = 'UTC' AND i.session_id IN ('peer-a', 'peer-b', 'sibling')`).Scan(&warmed))
			if warmed == 3 {
				break
			}
		}
		background := installs.Load()
		daily, err := database.GetDailyUsage(t.Context(), UsageFilter{Timezone: "UTC"})
		require.NoError(t, err)
		assert.Equal(t, 10, daily.Totals.OutputTokens)
		assert.Equal(t, background, installs.Load(), "request rebuilt peers lost from the full queue")
	})
	t.Run("large burst", func(t *testing.T) {
		database := testDB(t)
		seedLiveWriteSession(t, database, "session", "project", "2026-08-10T09:00:00Z", 0, 10)
		require.Equal(t, 10, liveWriteDailyInput(t, database, "project"))
		snapshot, err := database.captureUsageQuery(t.Context(), UsageFilter{}, usageQueryKindToken)
		require.NoError(t, err)
		cache, err := database.usageCache.Generation(t.Context(), snapshot.DatabaseID)
		require.NoError(t, err)
		blocked, release, drained := make(chan struct{}), make(chan struct{}), make(chan struct{})
		defer close(release)
		var leases atomic.Int32
		database.SetUsageCacheNotificationWork(func() (func(), bool) {
			leases.Add(1)
			return func() { close(drained) }, true
		})
		var paused atomic.Bool
		cache.fill.observer.afterNotificationBatch = func() {
			if !paused.Swap(true) {
				close(blocked)
				<-release
			}
		}
		database.usageCache.NotifySessions([]string{"missing"})
		waitUsageNotification(t, blocked)
		ids := make([]string, 20000)
		for i := range ids {
			ids[i] = "missing-" + strconv.Itoa(i)
		}
		database.usageCache.NotifySessions(ids)
		appendLiveWriteMessage(t, database, "session", "2026-08-10T10:00:00Z", 1, 7)
		cache.fill.mu.Lock()
		assert.Len(t, cache.fill.overflow, 1024, "burst retained an unbounded overflow")
		cache.fill.mu.Unlock()
		assert.EqualValues(t, 1, leases.Load(), "overflow acquired a second lease while a batch was in flight")
		release <- struct{}{}
		waitUsageNotification(t, drained)
		assert.Equal(t, 17, liveWriteDailyInput(t, database, "project"), "request lost usage dropped beyond the overflow cap")
	})
}

func TestUsageNotificationRefreshChunks(t *testing.T) {
	for _, testCase := range []struct {
		name         string
		sessions     int
		failFirst    bool
		wantSizes    []int
		wantInstalls int
	}{
		{name: "chunks", sessions: 600, wantSizes: []int{256, 256, 88}, wantInstalls: 600},
		{name: "continues after chunk failure", sessions: 257, failFirst: true, wantSizes: []int{256, 1}, wantInstalls: 1},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			database := testDB(t)
			for index := range testCase.sessions {
				seedLiveWriteSession(t, database, "session-"+strconv.Itoa(index), "project", "2026-08-10T09:00:00Z", 0, 1)
			}
			snapshot, err := database.captureUsageQuery(t.Context(), UsageFilter{}, usageQueryKindToken)
			require.NoError(t, err)
			cache, err := database.usageCache.Generation(t.Context(), snapshot.DatabaseID)
			require.NoError(t, err)
			fills, err := cache.fill.Ensure(t.Context(), snapshot.Versions, 0)
			require.NoError(t, err)
			database.usageBackfillMu.Lock()
			database.usageBackfillEnabled = true
			database.usageBackfillMu.Unlock()
			var sizes []int
			cache.rollup.observer.beforeInstall = func(builds []usageRollupBuild) {
				sizes = append(sizes, len(builds))
				if testCase.failFirst {
					switch len(sizes) {
					case 1:
						_, err := cache.db.ExecContext(t.Context(), `CREATE TRIGGER reject_rollup_chunk
							BEFORE INSERT ON usage_rollup_installs BEGIN SELECT RAISE(ABORT, 'rollup install failed'); END`)
						require.NoError(t, err)
					case 2:
						_, err := cache.db.ExecContext(t.Context(), `DROP TRIGGER reject_rollup_chunk`)
						require.NoError(t, err)
					}
				}
			}
			var fillSizes []int
			cache.rollup.observer.beforeAttempt = func(fills map[string]usageFillResult) { fillSizes = append(fillSizes, len(fills)) }
			database.refreshUsageRollups(t.Context(), cache, fills)
			assert.Len(t, sizes, len(testCase.wantSizes), "failed chunk prevented the later chunk from running")
			assert.Equal(t, testCase.wantSizes, fillSizes, "rollup attempts received fills outside their notification chunk")
			assert.Equal(t, testCase.wantInstalls, usageCacheCount(t, cache, "usage_rollup_installs"))
		})
	}
}
