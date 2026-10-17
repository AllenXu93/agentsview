package parser

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/tidwall/gjson"
)

func TestStructuredParsersRetainDialogueProjection(t *testing.T) {
	const blocks = `[{"type":"text","text":"  Literal [Read: quokka.txt]  "},{"type":"thinking","thinking":"reasoningneedle"},{"type":"tool_use","id":"call_a","name":"Read","input":{"file_path":"quokka.txt"}}]`
	for _, agent := range []AgentType{AgentOpenClaw, AgentQClaw, AgentDevin, AgentOpenHands, AgentCursor} {
		t.Run(string(agent), func(t *testing.T) {
			var message ParsedMessage
			switch agent {
			case AgentOpenClaw:
				b := newOpenClawRecordBuilder()
				require.NoError(t, b.consume(`{"type":"message","message":{"role":"assistant","content":`+blocks+`}}`, false))
				require.Len(t, b.messages, 1)
				message = b.messages[0]
			case AgentQClaw:
				path, _ := writeQClawTestFile(t, "agent", `{"type":"session","id":"dialogue"}`, `{"type":"message","message":{"role":"assistant","content":`+blocks+`}}`)
				_, messages, err := parseQClawSessionForTest(t, path, "project", "local")
				require.NoError(t, err)
				require.Len(t, messages, 1)
				message = messages[0]
			case AgentDevin:
				var ok bool
				message, ok = parseDevinStep("dialogue", gjson.Parse(`{"source":"agent","message":`+blocks+`}`), 0, "")
				require.True(t, ok)
				stored, ok, err := parseDevinDBMessageNode("dialogue", devinMessageNodeRow{ChatMessage: `{"role":"assistant","content":` + blocks + `}`}, 0, "")
				require.NoError(t, err)
				require.True(t, ok)
				assert.Equal(t, message.DialogueText, stored.DialogueText)
			case AgentOpenHands:
				var ok bool
				message, ok, _ = parseOpenHandsMessageEvent(gjson.Parse(`{"llm_message":{"role":"assistant","content":`+blocks+`}}`), 0, "", time.Time{})
				require.True(t, ok)
			case AgentCursor:
				messages := parseCursorJSONL(`{"role":"assistant","message":{"content":` + blocks + `}}`)
				require.Len(t, messages, 1)
				message = messages[0]
			default:
				require.FailNow(t, "unexpected provider")
			}
			want := "Literal [Read: quokka.txt]"
			if agent == AgentDevin {
				want = "  Literal [Read: quokka.txt]  "
			}
			assert.Equal(t, new(want), message.DialogueText)
			assert.Equal(t, "reasoningneedle", message.ThinkingText)
			assert.Contains(t, message.Content, "reasoningneedle")
			require.Len(t, message.ToolCalls, 1)
		})
	}
}
