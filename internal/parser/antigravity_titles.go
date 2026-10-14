package parser

import (
	"context"
	"database/sql"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

// AntigravitySummariesDBName is the per-root store where Antigravity keeps
// conversation titles, apart from each conversation's transcript.
const AntigravitySummariesDBName = "conversation_summaries.db"

type antigravityTitleSnapshot struct {
	state  SQLiteContainerState
	titles map[string]string
	err    error
	readAt time.Time
}

var antigravityTitleNow = time.Now

var antigravityTitleCache = struct {
	sync.Mutex
	stores map[string]antigravityTitleSnapshot
}{stores: make(map[string]antigravityTitleSnapshot)}

func InvalidateAntigravityTitles(path string) {
	antigravityTitleCache.Lock()
	defer antigravityTitleCache.Unlock()
	delete(antigravityTitleCache.stores, path)
}

// Matching markers can hide writes, so cached titles expire.
func AntigravityStoreTitles(ctx context.Context, path string) (map[string]string, SQLiteContainerState, error) {
	state, ok := StatSQLiteContainerState(path)
	antigravityTitleCache.Lock()
	defer antigravityTitleCache.Unlock()
	now := antigravityTitleNow()
	if prior, cached := antigravityTitleCache.stores[path]; ok && cached && prior.state == state && now.Sub(prior.readAt) < SQLiteContainerDigestVerifyInterval {
		return prior.titles, prior.state, prior.err
	}
	titles, err := readAntigravityTitles(ctx, path)
	if ctx.Err() != nil {
		return nil, state, ctx.Err()
	}
	if ok {
		// Remote imports extract stores into fresh temp roots; bound what stays cached.
		if _, cached := antigravityTitleCache.stores[path]; !cached && len(antigravityTitleCache.stores) >= 16 {
			clear(antigravityTitleCache.stores)
		}
		antigravityTitleCache.stores[path] = antigravityTitleSnapshot{state: state, titles: titles, err: err, readAt: now}
	} else {
		delete(antigravityTitleCache.stores, path)
	}
	return titles, state, err
}

func readAntigravityTitles(ctx context.Context, path string) (map[string]string, error) {
	if !IsRegularFile(path) {
		return nil, nil
	}
	observeSharedContainerScan(ctx)
	store, err := openSQLiteReadOnly(path, sqliteReadOptions{busyTimeoutMS: 3000})
	if err != nil {
		return nil, err
	}
	defer store.Close()
	rows, err := store.QueryContext(ctx, "SELECT conversation_id, title FROM conversation_summaries")
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	titles := make(map[string]string)
	for rows.Next() {
		var id string
		var title sql.NullString
		if err := rows.Scan(&id, &title); err != nil {
			return nil, err
		}
		if title.Valid {
			titles[id] = strings.TrimSpace(title.String)
		}
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return titles, nil
}

// AntigravitySessionRoot returns the Antigravity root that owns a stored
// conversation database or brain transcript path, even after it was deleted.
func AntigravitySessionRoot(path string) (string, bool) {
	path = filepath.Clean(path)
	if root, _, ok := antigravityBrainTranscriptConversation(path); ok {
		return root, true
	}
	root := filepath.Dir(filepath.Dir(path))
	if _, _, ok := antigravityConversationDBForPath(root, path); ok {
		return root, true
	}
	return "", false
}

// applyAntigravityTitle sets the session's title from its root's store. An
// unreadable store or missing entry leaves no title signal, so the stored
// name survives the write.
func applyAntigravityTitle(ctx context.Context, sess *ParsedSession, src antigravitySource) {
	titles, _, err := AntigravityStoreTitles(ctx, filepath.Join(src.Root, AntigravitySummariesDBName))
	if err != nil {
		return
	}
	if title, ok := titles[src.ID]; ok {
		sess.SessionName = title
		sess.SessionNamePresent = true
	}
}

// PreservesStoredSessionName reports whether a parse carries no title signal
// for an agent whose titles live outside the transcript.
func PreservesStoredSessionName(sess ParsedSession) bool {
	return (sess.Agent == AgentCodex || sess.Agent == AgentAntigravity) && !sess.SessionNamePresent
}
