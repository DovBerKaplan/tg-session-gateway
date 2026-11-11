package mtproto

// fakeMTClient extensions for the scanner surface: canned dicts the
// adapter/dispatch tests assert against.

import (
	"context"
	"fmt"
)

func (f *fakeMTClient) GetHistory(ctx context.Context, chatID int64, offsetID, addOffset, minID, limit int) ([]map[string]any, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := []map[string]any{}
	for i := 1; i <= limit; i++ {
		id := int64(1000 + i)
		out = append(out, map[string]any{
			"_": "Message", "id": id, "message": fmt.Sprintf("msg %d", id), "date": id,
		})
	}
	return out, nil
}

func (f *fakeMTClient) GetMessages(ctx context.Context, chatID int64, ids []int64) ([]map[string]any, error) {
	out := []map[string]any{}
	for _, id := range ids {
		out = append(out, map[string]any{"_": "Message", "id": id, "message": "fetched"})
	}
	return out, nil
}

func (f *fakeMTClient) GetDialogs(ctx context.Context, limit int) ([]map[string]any, error) {
	return []map[string]any{
		{"chat_id": int64(111), "access_hash": int64(11), "kind": "channel", "title": "one"},
		{"chat_id": int64(222), "access_hash": int64(22), "kind": "channel", "title": "two"},
		{"kind": "stats", "channels": 2, "users": 1, "legacy_chats": 0},
	}, nil
}

func (f *fakeMTClient) GetEntity(ctx context.Context, chatID int64) (map[string]any, error) {
	if chatID > 0 {
		return map[string]any{"_": "User", "id": chatID, "username": "u" + fmt.Sprint(chatID)}, nil
	}
	return map[string]any{"_": "Channel", "id": -chatID, "title": "chan", "access_hash": int64(7)}, nil
}

func (f *fakeMTClient) GetFullChannel(ctx context.Context, chatID int64) (map[string]any, error) {
	return map[string]any{"_": "ChannelFull", "id": chatID, "about": "about text", "participants_count": int64(42)}, nil
}

func (f *fakeMTClient) GetParticipant(ctx context.Context, chatID, userID int64) (map[string]any, error) {
	return map[string]any{"_": "ChannelParticipantAdmin", "user_id": userID, "rank": "scanner"}, nil
}

func (f *fakeMTClient) GetParticipants(ctx context.Context, p ParticipantsParams) ([]map[string]any, error) {
	return []map[string]any{
		{"_": "ChannelParticipantCreator", "user_id": int64(1)},
		{"_": "ChannelParticipantAdmin", "user_id": int64(2), "rank": "scanner"},
	}, nil
}

func (f *fakeMTClient) SendDocument(ctx context.Context, chatID int64, doc DocRef, caption, parseMode string) (SentMessage, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.sentDocs = append(f.sentDocs, doc)
	return SentMessage{ID: 5001, ChatID: chatID}, nil
}

func (f *fakeMTClient) CreateChannel(ctx context.Context, title, about string, megagroup bool) (map[string]any, error) {
	return map[string]any{"_": "Channel", "id": int64(999), "title": title, "access_hash": int64(5)}, nil
}

func (f *fakeMTClient) EditChannel(ctx context.Context, p EditChannelParams) (EditResult, error) {
	return EditResult{Ok: true}, nil
}

func (f *fakeMTClient) DeleteChannel(ctx context.Context, chatID int64) (EditResult, error) {
	return EditResult{Ok: true}, nil
}

func (f *fakeMTClient) EditAdmin(ctx context.Context, p EditAdminParams) (EditResult, error) {
	if len(p.Rights) == 0 {
		return EditResult{}, fmt.Errorf("rights required")
	}
	return EditResult{Ok: true}, nil
}

func (f *fakeMTClient) JoinChannel(ctx context.Context, chatID int64) (EditResult, error) {
	return EditResult{Ok: true}, nil
}

func (f *fakeMTClient) LeaveChannel(ctx context.Context, chatID int64) (EditResult, error) {
	return EditResult{Ok: true}, nil
}

func (f *fakeMTClient) ReadHistory(ctx context.Context, chatID, maxID int64) (EditResult, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.readAcks = append(f.readAcks, [2]int64{chatID, maxID})
	return EditResult{Ok: true}, nil
}

func (f *fakeMTClient) SeedPeers(_ context.Context, peers []PeerSeed) (int, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.seededPeers = append(f.seededPeers, peers...)
	return len(peers), nil
}
