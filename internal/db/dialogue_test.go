package db

import (
	"fmt"
	"path/filepath"
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"go.kenn.io/agentsview/internal/config"
)

func TestDialogueFTSUpgradePreservesMessages(t *testing.T) {
	const total, needle = 21, 12
	path := filepath.Join(t.TempDir(), "archive.db")
	d := testDBAtPath(t, path, "source")
	insertSession(t, d, "legacy", "project", func(s *Session) { s.Agent = "pi"; s.UserMessageCount = 2 })
	require.NoError(t, d.DropFTS(t.Context()))
	_, err := d.getWriter().Exec(t.Context(), fmt.Sprintf(`DROP VIEW message_search_content;
 ALTER TABLE messages DROP COLUMN dialogue_text;
 WITH RECURSIVE n(i) AS (SELECT 0 UNION ALL SELECT i + 1 FROM n WHERE i + 1 < %d)
 INSERT INTO messages (session_id, ordinal, role, content, content_length)
 SELECT 'legacy', i, CASE i %% 2 WHEN 0 THEN 'user' ELSE 'assistant' END,
  CASE i WHEN %d THEN 'answerneedle [Thinking] reasoningneedle' ELSE 'filler ' || i END, 0 FROM n;
 DELETE FROM stats WHERE key = 'message_search_corpus_version';
 CREATE VIRTUAL TABLE messages_fts USING fts5(content, content='messages', content_rowid='id', tokenize='porter unicode61');
 UPDATE messages SET thinking_text = 'reasoningneedle', has_thinking = 1 WHERE ordinal = %d;
 INSERT INTO messages_fts(messages_fts) VALUES('rebuild');
 PRAGMA user_version=%d; UPDATE sessions SET data_version=%d;`, total, needle, needle, CurrentDataVersion()-1, CurrentDataVersion()-1))
	require.NoError(t, err)
	var beforeID int64
	require.NoError(t, d.getReader().QueryRow(t.Context(),
		"SELECT id FROM messages WHERE session_id = 'legacy' AND ordinal = ?", needle).Scan(&beforeID))
	require.NoError(t, d.Close())

	var details []string
	d, err = OpenWithProgress(t.Context(), path, config.ArchiveContentFull, func(progress OpenProgress) {
		details = append(details, progress.Detail)
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, d.Close()) })

	stored, err := d.GetAllMessages(t.Context(), "legacy")
	require.NoError(t, err)
	require.Len(t, stored, total)
	assert.Equal(t, "answerneedle [Thinking] reasoningneedle", stored[needle].Content)
	assert.Nil(t, stored[needle].DialogueText)
	assert.Equal(t, beforeID, stored[needle].ID)
	assert.Equal(t, "reasoningneedle", stored[needle].ThinkingText)
	for _, term := range []string{"answerneedle", "reasoningneedle"} {
		ordinary, err := d.Search(t.Context(), SearchFilter{Query: term})
		require.NoError(t, err)
		assert.Len(t, ordinary.Results, 1, term)
	}

	assert.Contains(t, details, "Updating full-text search")
	var marker string
	require.NoError(t, d.getReader().QueryRow(t.Context(), "SELECT value FROM stats WHERE key = ?", searchCorpusVersionStatsKey).Scan(&marker))
	assert.Equal(t, searchCorpusVersion, marker)
	require.NoError(t, d.Close())
	details = nil
	d, err = OpenWithProgress(t.Context(), path, config.ArchiveContentFull, func(progress OpenProgress) {
		details = append(details, progress.Detail)
	})
	require.NoError(t, err)
	assert.NotContains(t, details, "Updating full-text search")
	_, err = d.getWriter().Exec(t.Context(), "INSERT INTO messages_fts(messages_fts, rank) VALUES('integrity-check', 1)")
	require.NoError(t, err)
}

func TestDialogueProjectionSeparatesMessagesAndThinking(t *testing.T) {
	for _, classified := range []bool{true, false} {
		t.Run(strconv.FormatBool(classified), func(t *testing.T) {
			d := testDB(t)
			insertSession(t, d, "search", "project", func(s *Session) { s.UserMessageCount = 2 })
			message := Message{
				SessionID: "search", Ordinal: 1, Role: "assistant",
				Content:      "answerneedle [Thinking] reasoningneedle [/Thinking] [Read: file.txt]",
				ThinkingText: "reasoningneedle",
			}
			if classified {
				message.DialogueText = new("answerneedle")
			}
			insertMessages(t, d, Message{SessionID: "search", Role: "user", Content: "Question"}, message)
			for _, mode := range []string{"substring", "regex", "fts", "terms"} {
				for _, term := range []string{"answerneedle", "reasoningneedle", "file.txt"} {
					page, err := d.SearchContent(t.Context(), ContentSearchFilter{Pattern: term, Mode: mode, Sources: []string{"messages"}})
					require.NoError(t, err)
					if classified && term != "answerneedle" {
						assert.Empty(t, page.Matches, "%s %s", mode, term)
					} else {
						require.Len(t, page.Matches, 1, "%s %s", mode, term)
						assert.Equal(t, "message", page.Matches[0].Location)
					}
				}
			}
			for _, mode := range []string{"substring", "regex"} {
				for _, sources := range [][]string{nil, {"thinking"}} {
					page, err := d.SearchContent(t.Context(), ContentSearchFilter{Pattern: "reasoningneedle", Mode: mode, Sources: sources})
					require.NoError(t, err)
					require.Len(t, page.Matches, 1)
					location := "thinking"
					if !classified && sources == nil {
						location = "message"
					}
					assert.Equal(t, location, page.Matches[0].Location)
				}
			}
			page, err := d.SearchContent(t.Context(), ContentSearchFilter{Pattern: `"answerneedle" NOT reasoningneedle`, Mode: "fts", Sources: []string{"messages"}})
			require.NoError(t, err)
			if classified {
				assert.Len(t, page.Matches, 1)
			} else {
				assert.Empty(t, page.Matches)
			}
		})
	}
	d := testDB(t)
	for _, mode := range []string{"terms", "semantic", "hybrid"} {
		_, err := d.SearchContent(t.Context(), ContentSearchFilter{Pattern: "reasoningneedle", Mode: mode, Sources: []string{"thinking"}})
		var inputErr *SearchInputError
		require.ErrorAs(t, err, &inputErr)
	}
}

func TestSearchRetainsRenderedContentAlongsideDialogue(t *testing.T) {
	for _, tc := range []struct{ name, dialogue, hidden, replacement string }{
		{"porter", "answerneedle", "reasoningneedle", "replacementneedle"},
		{"cjk", "中文搜索", "工具输出", "秘密推理"},
		{"empty", "", "reasoningneedle", "replacementneedle"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if tc.name == "cjk" && !simpleFTSRuntimeConfig.available() {
				t.Skip("simple FTS5 runtime is not installed for this test process")
			}
			d := testDB(t)
			insertSession(t, d, "search", "project", func(s *Session) { s.UserMessageCount = 2 })
			messages := []Message{
				{SessionID: "search", Ordinal: 0, Role: "user", Content: "Question"},
				{SessionID: "search", Ordinal: 1, Role: "assistant", Content: tc.dialogue + " [Thinking] " + tc.hidden, DialogueText: new(tc.dialogue)},
				{SessionID: "search", Ordinal: 2, Role: "assistant", Content: "Done"},
			}
			insertMessages(t, d, messages...)
			if tc.name == "cjk" {
				page, err := d.SearchContent(t.Context(), ContentSearchFilter{Pattern: tc.dialogue, Mode: "terms", Sources: []string{"messages"}})
				require.NoError(t, err)
				assert.Len(t, page.Matches, 1)
			}
			for _, phase := range []string{"insert", "update", "delete"} {
				t.Run(phase, func(t *testing.T) {
					switch phase {
					case "update":
						messages[1].Content = tc.dialogue + " [Thinking] " + tc.replacement
						require.NoError(t, d.ReplaceSessionMessages(t.Context(), "search", messages))
					case "delete":
						require.NoError(t, d.ReplaceSessionMessages(t.Context(), "search", nil))
					}
					for _, term := range []string{tc.dialogue, tc.hidden, tc.replacement} {
						if term == "" {
							continue
						}
						ordinary, err := d.Search(t.Context(), SearchFilter{Query: term})
						require.NoError(t, err)
						wantOrdinary := 0
						if phase != "delete" && (term == tc.dialogue || phase == "insert" && term == tc.hidden || phase == "update" && term == tc.replacement) {
							wantOrdinary = 1
						}
						assert.Len(t, ordinary.Results, wantOrdinary, term)
						if phase == "insert" {
							continue
						}
						page, err := d.SearchContent(t.Context(), ContentSearchFilter{Pattern: term, Mode: "fts", Sources: []string{"messages"}})
						require.NoError(t, err)
						if phase != "delete" && term == tc.dialogue {
							require.Len(t, page.Matches, 1)
							assert.Equal(t, tc.dialogue, page.Matches[0].Snippet)
						} else {
							assert.Empty(t, page.Matches, term)
						}
					}
				})
			}
		})
	}
}

func TestProjectionOnlyReparsePreservesTranscriptFreshness(t *testing.T) {
	d := testDB(t)
	insertSession(t, d, "projection", "project")
	messages := []Message{{SessionID: "projection", Role: "assistant", Content: "Answer [Thinking] reasoning"}}
	insertMessages(t, d, messages...)
	_, err := d.getWriter().Exec(t.Context(), `UPDATE sessions SET
		secrets_rules_version = 'secrets-current', friction_rules_version = 'friction-current',
		local_modified_at = '2026-05-01T10:00:00Z' WHERE id = 'projection'`)
	require.NoError(t, err)
	storedBefore, err := d.GetAllMessages(t.Context(), "projection")
	require.NoError(t, err)
	require.Len(t, storedBefore, 1)
	before, err := d.GetSession(t.Context(), "projection")
	require.NoError(t, err)
	for _, dialogue := range []string{"Answer", ""} {
		messages[0].DialogueText = new(dialogue)
		require.NoError(t, d.ReplaceSessionMessages(t.Context(), "projection", messages))
		stored, err := d.GetAllMessages(t.Context(), "projection")
		require.NoError(t, err)
		require.Len(t, stored, 1)
		assert.Equal(t, storedBefore[0].ID, stored[0].ID)
		assert.Equal(t, new(dialogue), stored[0].DialogueText)
		after, err := d.GetSession(t.Context(), "projection")
		require.NoError(t, err)
		assert.Equal(t, before.TranscriptRevision, after.TranscriptRevision)
		var secretVersion, frictionVersion, modified string
		require.NoError(t, d.getReader().QueryRow(t.Context(), `SELECT secrets_rules_version,
			friction_rules_version, local_modified_at FROM sessions WHERE id = 'projection'`).Scan(&secretVersion, &frictionVersion, &modified))
		assert.Equal(t, "secrets-current", secretVersion)
		assert.Equal(t, "friction-current", frictionVersion)
		assert.Equal(t, "2026-05-01T10:00:00Z", modified)
	}
}
