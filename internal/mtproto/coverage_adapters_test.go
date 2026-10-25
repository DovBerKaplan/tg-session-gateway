package mtproto

// Direct adapter tests: each thin adapter's coerce+validate+dispatch
// path, driven with the existing fake client. The HTTP surface for
// these is covered by the proxy tests; media-bearing adapters need a
// populated media cache, which only gotdClient has — so their
// parameter paths are exercised here at the adapter layer.

import (
	"context"
	"errors"
	"testing"
)

func TestAdaptersValidationAndDispatch(t *testing.T) {
	fake := &fakeMTClient{}
	ctx := context.Background()

	t.Run("edit markup requires message_id", func(t *testing.T) {
		if _, err := adaptEditMarkup(ctx, fake, map[string]any{"chat_id": 5.0}); err == nil {
			t.Fatal("missing message_id must error")
		}
		if _, err := adaptEditMarkup(ctx, fake, map[string]any{"chat_id": 5.0, "message_id": 1.0}); err != nil {
			t.Fatalf("valid: %v", err)
		}
	})

	t.Run("chat action", func(t *testing.T) {
		if _, err := adaptChatAction(ctx, fake, map[string]any{}); err == nil {
			t.Fatal("missing chat_id must error")
		}
		if _, err := adaptChatAction(ctx, fake, map[string]any{"chat_id": 5.0, "action": "typing"}); err != nil {
			t.Fatalf("valid: %v", err)
		}
	})

	t.Run("delete messages accepts message_ids or ids", func(t *testing.T) {
		if _, err := adaptDeleteMessages(ctx, fake, map[string]any{}); err == nil {
			t.Fatal("missing ids must error")
		}
		if _, err := adaptDeleteMessages(ctx, fake, map[string]any{"message_ids": []any{1.0, 2.0}}); err != nil {
			t.Fatalf("message_ids: %v", err)
		}
		if _, err := adaptDeleteMessages(ctx, fake, map[string]any{"ids": []any{3.0}}); err != nil {
			t.Fatalf("ids: %v", err)
		}
		if _, err := adaptDeleteMessages(ctx, fake, map[string]any{"message_ids": "nope"}); err == nil {
			t.Fatal("non-list ids must error")
		}
	})

	t.Run("forward messages validations", func(t *testing.T) {
		if _, err := adaptForwardMessages(ctx, fake, map[string]any{}); err == nil {
			t.Fatal("missing from_chat_id must error")
		}
		if _, err := adaptForwardMessages(ctx, fake, map[string]any{"from_chat_id": 5.0}); err == nil {
			t.Fatal("missing chat_id must error")
		}
		if _, err := adaptForwardMessages(ctx, fake, map[string]any{"from_chat_id": 5.0, "chat_id": 6.0}); err == nil {
			t.Fatal("missing message_ids must error")
		}
		if _, err := adaptForwardMessages(ctx, fake, map[string]any{
			"from_chat_id": 5.0, "chat_id": 6.0, "message_ids": []any{1.0}, "copy": true,
		}); err != nil {
			t.Fatalf("valid: %v", err)
		}
	})

	t.Run("get chat", func(t *testing.T) {
		if _, err := adaptGetChat(ctx, fake, map[string]any{}); err == nil {
			t.Fatal("missing chat_id must error")
		}
		if _, err := adaptGetChat(ctx, fake, map[string]any{"chat_id": 5.0}); err != nil {
			t.Fatalf("valid: %v", err)
		}
	})

	t.Run("resolve username", func(t *testing.T) {
		if _, err := adaptResolveUsername(ctx, fake, map[string]any{}); err == nil {
			t.Fatal("missing username must error")
		}
		out, err := adaptResolveUsername(ctx, fake, map[string]any{"username": "@durov"})
		if err != nil {
			t.Fatalf("valid: %v", err)
		}
		if out.(map[string]any)["username"] != "durov" {
			t.Fatalf("@ prefix must be trimmed: %v", out)
		}
		failing := &failingResolve{fakeMTClient: fake}
		if _, err := adaptResolveUsername(ctx, failing, map[string]any{"username": "x"}); err == nil {
			t.Fatal("client error must propagate")
		}
	})

	t.Run("answer callback id variants", func(t *testing.T) {
		if _, err := adaptAnswerCallback(ctx, fake, map[string]any{}); err == nil {
			t.Fatal("missing callback_query_id must error")
		}
		if _, err := adaptAnswerCallback(ctx, fake, map[string]any{"callback_query_id": "cb"}); err != nil {
			t.Fatalf("callback_query_id: %v", err)
		}
		if _, err := adaptAnswerCallback(ctx, fake, map[string]any{"id": "cb2"}); err != nil {
			t.Fatalf("id variant: %v", err)
		}
	})

	t.Run("get me", func(t *testing.T) {
		if _, err := adaptGetMe(ctx, fake, map[string]any{}); err != nil {
			t.Fatalf("get me: %v", err)
		}
	})
}

func TestArgIntSliceShapes(t *testing.T) {
	if ids, ok := argIntSlice([]any{float64(1), int64(2), int(3)}); !ok || len(ids) != 3 {
		t.Fatalf("mixed numeric list: %v %v", ids, ok)
	}
	if _, ok := argIntSlice([]any{"x"}); ok {
		t.Fatal("non-numeric item must fail")
	}
	if _, ok := argIntSlice("nope"); ok {
		t.Fatal("non-list must fail")
	}
}

// failingResolve overrides one method to fail, for error-propagation.
type failingResolve struct {
	*fakeMTClient
}

func (f *failingResolve) ResolveUsername(ctx context.Context, username string) (int64, error) {
	return 0, errors.New("username not found")
}
