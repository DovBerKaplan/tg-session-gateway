package mtproto

// gotd implementations of the scanner surface. Channel-scoped methods
// resolve the channel (with access hash) once via channels.getChannels —
// the same hash-less resolution get_chat uses — and reuse the hash for
// the actual call, mirroring cine-scanner's client.

import (
	"context"
	"fmt"
	"strings"

	"github.com/gotd/td/telegram/message"
	"github.com/gotd/td/telegram/query/dialogs"
	"github.com/gotd/td/tg"
	"github.com/gotd/td/tgerr"

	"github.com/DovBerKaplan/tg-session-gateway/internal/store"
)

const scanChunk = 200 // channels.getMessages / getChannels / getParticipants page

// scanAPI is the RPC subset the scanner surface uses — *tg.Client
// satisfies it; tests fake it to cover the request-assembly logic
// (chunking, hash propagation, filter/rights mapping) honestly.
type scanAPI interface {
	UsersGetUsers(ctx context.Context, id []tg.InputUserClass) ([]tg.UserClass, error)
	MessagesGetHistory(ctx context.Context, req *tg.MessagesGetHistoryRequest) (tg.MessagesMessagesClass, error)
	ChannelsGetChannels(ctx context.Context, id []tg.InputChannelClass) (tg.MessagesChatsClass, error)
	ChannelsGetMessages(ctx context.Context, req *tg.ChannelsGetMessagesRequest) (tg.MessagesMessagesClass, error)
	ChannelsGetFullChannel(ctx context.Context, channel tg.InputChannelClass) (*tg.MessagesChatFull, error)
	ChannelsGetParticipant(ctx context.Context, req *tg.ChannelsGetParticipantRequest) (*tg.ChannelsChannelParticipant, error)
	ChannelsGetParticipants(ctx context.Context, req *tg.ChannelsGetParticipantsRequest) (tg.ChannelsChannelParticipantsClass, error)
	MessagesSendMessage(ctx context.Context, req *tg.MessagesSendMessageRequest) (tg.UpdatesClass, error)
	MessagesEditChatAbout(ctx context.Context, req *tg.MessagesEditChatAboutRequest) (bool, error)
	ChannelsEditTitle(ctx context.Context, req *tg.ChannelsEditTitleRequest) (tg.UpdatesClass, error)
	ChannelsToggleSignatures(ctx context.Context, req *tg.ChannelsToggleSignaturesRequest) (tg.UpdatesClass, error)
	ChannelsTogglePreHistoryHidden(ctx context.Context, req *tg.ChannelsTogglePreHistoryHiddenRequest) (tg.UpdatesClass, error)
	MessagesToggleNoForwards(ctx context.Context, req *tg.MessagesToggleNoForwardsRequest) (tg.UpdatesClass, error)
	ChannelsCreateChannel(ctx context.Context, req *tg.ChannelsCreateChannelRequest) (tg.UpdatesClass, error)
	ChannelsDeleteChannel(ctx context.Context, channel tg.InputChannelClass) (tg.UpdatesClass, error)
	ChannelsEditAdmin(ctx context.Context, req *tg.ChannelsEditAdminRequest) (tg.UpdatesClass, error)
	ChannelsJoinChannel(ctx context.Context, channel tg.InputChannelClass) (tg.MessagesChatInviteJoinResultClass, error)
	ChannelsLeaveChannel(ctx context.Context, channel tg.InputChannelClass) (tg.UpdatesClass, error)
	ChannelsReadHistory(ctx context.Context, req *tg.ChannelsReadHistoryRequest) (bool, error)
	MessagesReadHistory(ctx context.Context, req *tg.MessagesReadHistoryRequest) (*tg.MessagesAffectedMessages, error)
}

// compile-time: the production raw client satisfies scanAPI.
var _ scanAPI = (*tg.Client)(nil)

// scan returns the RPC surface (real or test-injected).
func (g *gotdClient) scan() scanAPI {
	if g.scanFake != nil {
		return g.scanFake
	}
	return g.raw
}

// bareChannelID accepts Bot API -100… form or a bare positive channel id
// (the scanner's convention). Legacy -xxxxxxx group chats are refused —
// they are not channels.
func bareChannelID(chatID int64) (int64, error) {
	switch {
	case chatID <= -1000000000000:
		return -(chatID + 1000000000000), nil
	case chatID > 0:
		return chatID, nil
	default:
		return 0, fmt.Errorf("%d is a legacy group chat, not a channel", chatID)
	}
}

// resolveChannel fetches the channel entity (access hash included) for
// a Bot API or bare channel id. The shared peer cache is checked first
// (one resolve per session, not per client); misses resolve once and
// land in the cache.
func (g *gotdClient) resolveChannel(ctx context.Context, chatID int64) (*tg.Channel, error) {
	bare, err := bareChannelID(chatID)
	if err != nil {
		return nil, err
	}
	if g.storeDB != nil {
		if e, found := g.storeDB.LookupPeer(g.alias, bare); found && e.Kind == "channel" && e.AccessHash != 0 {
			return &tg.Channel{ID: bare, AccessHash: e.AccessHash,
				Title: e.Title, Username: e.Username}, nil
		}
	}
	resp, err := g.scan().ChannelsGetChannels(ctx, []tg.InputChannelClass{
		&tg.InputChannel{ChannelID: bare},
	})
	if err != nil {
		return nil, err
	}
	for _, chat := range chatsOfMessagesChats(resp) {
		if ch, ok := chat.(*tg.Channel); ok && ch.ID == bare {
			g.cacheChannel(ch)
			return ch, nil
		}
	}
	return nil, fmt.Errorf("channel %d not found", bare)
}

// cacheChannel / cacheUser land entities in the shared peer cache.
// Best-effort, and READ-BEFORE-WRITE: history pages repeat the same
// chats constantly — an unchanged entity skips the write entirely (the
// store serializes on one connection; redundant writes are what starved
// facade calls next to the gap-recovery state ticks).
func (g *gotdClient) cacheChannel(ch *tg.Channel) {
	if g.storeDB == nil || ch.ID == 0 {
		return
	}
	e := store.PeerEntry{
		ID: ch.ID, Kind: "channel", AccessHash: ch.AccessHash,
		Username: ch.Username, Title: ch.Title,
	}
	if cur, found := g.storeDB.LookupPeer(g.alias, ch.ID); found &&
		cur.AccessHash == e.AccessHash && cur.Username == e.Username && cur.Title == e.Title {
		return // unchanged — no write
	}
	_ = g.storeDB.UpsertPeer(g.alias, e)
}

func (g *gotdClient) cacheUser(u *tg.User) {
	if g.storeDB == nil || u.AccessHash == 0 {
		return
	}
	e := store.PeerEntry{
		ID: u.ID, Kind: "user", AccessHash: u.AccessHash, Username: u.Username,
		Title: strings.TrimSpace(u.FirstName + " " + u.LastName),
	}
	if cur, found := g.storeDB.LookupPeer(g.alias, u.ID); found &&
		cur.Kind == "user" && cur.AccessHash == e.AccessHash && cur.Username == e.Username {
		return // unchanged — no write
	}
	_ = g.storeDB.UpsertPeer(g.alias, e)
}

// cacheResponse fills the peer cache from the chats/users riding any
// messages response (the scanner's cacheChats/cacheUsers port).
func (g *gotdClient) cacheResponse(chats []tg.ChatClass, users []tg.UserClass) {
	for _, chat := range chats {
		if ch, ok := chat.(*tg.Channel); ok {
			g.cacheChannel(ch)
		}
	}
	for _, user := range users {
		if u, ok := user.(*tg.User); ok {
			g.cacheUser(u)
		}
	}
}

func chatsOfMessagesChats(resp tg.MessagesChatsClass) []tg.ChatClass {
	switch cc := resp.(type) {
	case *tg.MessagesChats:
		return cc.Chats
	case *tg.MessagesChatsSlice:
		return cc.Chats
	}
	return nil
}

// unpackMessagesClass splits a messages response into its parts.
func unpackMessagesClass(res tg.MessagesMessagesClass) (msgs []tg.MessageClass, chats []tg.ChatClass, users []tg.UserClass) {
	switch m := res.(type) {
	case *tg.MessagesMessages:
		return m.Messages, m.Chats, m.Users
	case *tg.MessagesMessagesSlice:
		return m.Messages, m.Chats, m.Users
	case *tg.MessagesChannelMessages:
		return m.Messages, m.Chats, m.Users
	}
	return nil, nil, nil
}

func serializeMessages(msgs []tg.MessageClass) []map[string]any {
	out := make([]map[string]any, 0, len(msgs))
	for _, mc := range msgs {
		if d := SerializeMessage(mc); d != nil {
			out = append(out, d)
		}
	}
	return out
}

// GetHistory is ONE messages.getHistory window (offset_id/add_offset/
// min_id/limit, Telegram semantics — the walk stays with the caller).
// It never marks messages as read.
func (g *gotdClient) GetHistory(ctx context.Context, chatID int64, offsetID, addOffset, minID, limit int) ([]map[string]any, error) {
	if limit <= 0 || limit > 100 {
		limit = 100
	}
	res, err := g.scan().MessagesGetHistory(ctx, &tg.MessagesGetHistoryRequest{
		Peer:      g.peerFor(ctx, chatID),
		OffsetID:  offsetID,
		AddOffset: addOffset,
		MinID:     minID,
		Limit:     limit,
	})
	if err != nil {
		return nil, err
	}
	msgs, chats, users := unpackMessagesClass(res)
	g.cacheResponse(chats, users)
	return serializeMessages(msgs), nil
}

// GetMessages fetches specific message ids from a channel in the
// requested order (channels.getMessages, chunked; missing ids omitted).
func (g *gotdClient) GetMessages(ctx context.Context, chatID int64, ids []int64) ([]map[string]any, error) {
	ch, err := g.resolveChannel(ctx, chatID)
	if err != nil {
		return nil, err
	}
	input := &tg.InputChannel{ChannelID: ch.ID, AccessHash: ch.AccessHash}
	byID := map[int64]map[string]any{}
	var order []int64
	for i := 0; i < len(ids); i += scanChunk {
		end := min(i+scanChunk, len(ids))
		inputs := make([]tg.InputMessageClass, 0, end-i)
		for _, id := range ids[i:end] {
			inputs = append(inputs, &tg.InputMessageID{ID: int(id)})
			order = append(order, id)
		}
		res, err := g.scan().ChannelsGetMessages(ctx, &tg.ChannelsGetMessagesRequest{
			Channel: input, ID: inputs,
		})
		if err != nil {
			return nil, err
		}
		msgs, chats, users := unpackMessagesClass(res)
		g.cacheResponse(chats, users)
		for _, d := range serializeMessages(msgs) {
			if id, ok := anyToInt64(d["id"]); ok {
				byID[id] = d
			}
		}
	}
	out := make([]map[string]any, 0, len(order))
	for _, id := range order {
		if d, ok := byID[id]; ok {
			out = append(out, d)
		}
	}
	return out, nil
}

// dialogDict is one get_dialogs entry: enough to read the channel (the
// access hash is the scarce part) plus whatever metadata Telegram sent.
func dialogDict(kind string, id, accessHash int64, chat map[string]any) map[string]any {
	d := map[string]any{
		"chat_id":     id,
		"access_hash": accessHash,
		"kind":        kind,
	}
	for k, v := range chat {
		d[k] = v
	}
	return d
}

// GetDialogs walks every dialog newest-first (query builder iterator),
// returning channels/supergroups with their access hashes; channel
// metadata is filled in chunks of channels.getChannels exactly like the
// scanner's Dialogs (missing metadata is not fatal — the hash alone
// reads the channel).
func (g *gotdClient) GetDialogs(ctx context.Context, limit int) ([]map[string]any, error) {
	if limit <= 0 {
		limit = 500
	}
	type seen struct {
		id   int64
		hash int64
	}
	var chans []seen
	users := 0
	chats := 0

	iter := dialogs.NewQueryBuilder(g.raw).GetDialogs().BatchSize(100).Iter()
	for iter.Next(ctx) {
		switch p := iter.Value().Peer.(type) {
		case *tg.InputPeerChannel:
			chans = append(chans, seen{id: p.ChannelID, hash: p.AccessHash})
		case *tg.InputPeerChat:
			chats++
		case *tg.InputPeerUser:
			users++
		}
		if len(chans) >= limit {
			break
		}
	}
	if err := iter.Err(); err != nil {
		return nil, err
	}

	out := make([]map[string]any, 0, len(chans))
	meta := map[int64]map[string]any{}
	for i := 0; i < len(chans); i += scanChunk {
		end := min(i+scanChunk, len(chans))
		inputs := make([]tg.InputChannelClass, 0, end-i)
		for _, ch := range chans[i:end] {
			inputs = append(inputs, &tg.InputChannel{ChannelID: ch.id, AccessHash: ch.hash})
		}
		res, err := g.scan().ChannelsGetChannels(ctx, inputs)
		if err != nil {
			// metadata is best-effort; the hash alone is enough to read
			continue
		}
		for _, chat := range chatsOfMessagesChats(res) {
			if ch, ok := chat.(*tg.Channel); ok {
				meta[ch.ID] = channelDict(ch)
			}
		}
	}
	for _, ch := range chans {
		d := dialogDict("channel", ch.id, ch.hash, meta[ch.id])
		out = append(out, d)
		g.cacheChannel(&tg.Channel{ID: ch.id, AccessHash: ch.hash, Title: d["title"].(string)})
	}
	out = append(out,
		map[string]any{"kind": "stats", "channels": len(chans), "users": users, "legacy_chats": chats},
	)
	return out, nil
}

// channelDict is the Pyrogram-ish channel view.
func channelDict(ch *tg.Channel) map[string]any {
	d := map[string]any{
		"_":        "Channel",
		"id":       ch.ID,
		"title":    ch.Title,
		"username": ch.Username,
	}
	if ch.Megagroup {
		d["type"] = "supergroup"
	} else if ch.Broadcast {
		d["type"] = "channel"
	}
	return d
}

// GetEntity resolves one peer to its full entity dict (cache-flush:
// users.getUsers / channels.getChannels).
func (g *gotdClient) GetEntity(ctx context.Context, chatID int64) (map[string]any, error) {
	if chatID > 0 {
		users, err := g.scan().UsersGetUsers(ctx, []tg.InputUserClass{&tg.InputUser{UserID: chatID}})
		if err != nil {
			return nil, err
		}
		if len(users) > 0 {
			if u, ok := users[0].(*tg.User); ok {
				return map[string]any{
					"_": "User", "id": u.ID, "username": u.Username,
					"first_name": u.FirstName, "last_name": u.LastName,
					"access_hash": u.AccessHash, "is_bot": u.Bot,
				}, nil
			}
		}
		return nil, fmt.Errorf("user %d not found", chatID)
	}
	ch, err := g.resolveChannel(ctx, chatID)
	if err != nil {
		return nil, err
	}
	d := channelDict(ch)
	d["access_hash"] = ch.AccessHash
	return d, nil
}

// GetFullChannel is channels.getFullChannel (about, participants_count…).
func (g *gotdClient) GetFullChannel(ctx context.Context, chatID int64) (map[string]any, error) {
	ch, err := g.resolveChannel(ctx, chatID)
	if err != nil {
		return nil, err
	}
	res, err := g.scan().ChannelsGetFullChannel(ctx, &tg.InputChannel{
		ChannelID: ch.ID, AccessHash: ch.AccessHash,
	})
	if err != nil {
		return nil, err
	}
	out := map[string]any{
		"_": "ChannelFull", "id": ch.ID, "title": ch.Title,
	}
	if len(res.Chats) > 0 {
		if c, ok := res.Chats[0].(*tg.Channel); ok {
			out["title"] = c.Title
			out["username"] = c.Username
			out["megagroup"] = c.Megagroup
			out["signatures"] = c.Signatures
			out["noforwards"] = c.Noforwards
			out["access_hash"] = c.AccessHash
		}
	}
	if full, ok := res.FullChat.(*tg.ChannelFull); ok {
		out["about"] = full.About
		if n, ok := full.GetParticipantsCount(); ok {
			out["participants_count"] = int64(n)
		}
		if n, ok := full.GetOnlineCount(); ok {
			out["online_count"] = int64(n)
		}
	}
	return out, nil
}

// GetParticipant is channels.getParticipant for one user.
func (g *gotdClient) GetParticipant(ctx context.Context, chatID, userID int64) (map[string]any, error) {
	ch, err := g.resolveChannel(ctx, chatID)
	if err != nil {
		return nil, err
	}
	res, err := g.scan().ChannelsGetParticipant(ctx, &tg.ChannelsGetParticipantRequest{
		Channel:     &tg.InputChannel{ChannelID: ch.ID, AccessHash: ch.AccessHash},
		Participant: &tg.InputPeerUser{UserID: userID},
	})
	if err != nil {
		return nil, err
	}
	return channelParticipantDict(res.Participant), nil
}

// channelParticipantDict renders the channel (super)participant
// hierarchy the gateway's get_participant(s) return. Banned/left carry
// the user inside a Peer.
func channelParticipantDict(p tg.ChannelParticipantClass) map[string]any {
	peerUserID := func(peer tg.PeerClass) int64 {
		if u, ok := peer.(*tg.PeerUser); ok {
			return u.UserID
		}
		return 0
	}
	base := map[string]any{"_": "ChannelParticipant"}
	switch v := p.(type) {
	case *tg.ChannelParticipantSelf:
		base = map[string]any{"_": "ChannelParticipantSelf", "user_id": v.UserID,
			"inviter_id": v.InviterID, "date": int64(v.Date)}
	case *tg.ChannelParticipantCreator:
		base = map[string]any{"_": "ChannelParticipantCreator", "user_id": v.UserID, "rank": v.Rank}
		base["admin_rights"] = adminRightsDict(&v.AdminRights)
	case *tg.ChannelParticipantAdmin:
		base = map[string]any{"_": "ChannelParticipantAdmin", "user_id": v.UserID,
			"promoted_by": v.PromotedBy, "date": int64(v.Date), "rank": v.Rank}
		base["admin_rights"] = adminRightsDict(&v.AdminRights)
	case *tg.ChannelParticipantBanned:
		base = map[string]any{"_": "ChannelParticipantBanned", "user_id": peerUserID(v.Peer),
			"left": v.Left, "kicked_by": v.KickedBy, "date": int64(v.Date)}
	case *tg.ChannelParticipantLeft:
		base = map[string]any{"_": "ChannelParticipantLeft", "user_id": peerUserID(v.Peer)}
	}
	return base
}

func adminRightsDict(r *tg.ChatAdminRights) []string {
	var out []string
	add := func(on bool, name string) {
		if on {
			out = append(out, name)
		}
	}
	add(r.ChangeInfo, "change_info")
	add(r.PostMessages, "post_messages")
	add(r.EditMessages, "edit_messages")
	add(r.DeleteMessages, "delete_messages")
	add(r.BanUsers, "ban_users")
	add(r.InviteUsers, "invite_users")
	add(r.PinMessages, "pin_messages")
	add(r.AddAdmins, "add_admins")
	add(r.Anonymous, "anonymous")
	add(r.ManageCall, "manage_call")
	add(r.ManageTopics, "manage_topics")
	add(r.Other, "other")
	return out
}

func adminRightsFromNames(names []string) tg.ChatAdminRights {
	var r tg.ChatAdminRights
	for _, name := range names {
		switch name {
		case "change_info":
			r.ChangeInfo = true
		case "post_messages":
			r.PostMessages = true
		case "edit_messages":
			r.EditMessages = true
		case "delete_messages":
			r.DeleteMessages = true
		case "ban_users":
			r.BanUsers = true
		case "invite_users":
			r.InviteUsers = true
		case "pin_messages":
			r.PinMessages = true
		case "add_admins":
			r.AddAdmins = true
		case "anonymous":
			r.Anonymous = true
		case "manage_call":
			r.ManageCall = true
		case "other":
			r.Other = true
		}
	}
	return r
}

// GetParticipants pages channels.getParticipants with a filter
// (recent|admins|kicked|banned|search).
func (g *gotdClient) GetParticipants(ctx context.Context, p ParticipantsParams) ([]map[string]any, error) {
	ch, err := g.resolveChannel(ctx, p.ChatID)
	if err != nil {
		return nil, err
	}
	if p.Limit <= 0 || p.Limit > scanChunk {
		p.Limit = scanChunk
	}
	var filter tg.ChannelParticipantsFilterClass
	switch p.Filter {
	case "", "recent":
		filter = &tg.ChannelParticipantsRecent{}
	case "admins":
		filter = &tg.ChannelParticipantsAdmins{}
	case "kicked":
		filter = &tg.ChannelParticipantsKicked{Q: p.Query}
	case "banned":
		filter = &tg.ChannelParticipantsBanned{Q: p.Query}
	case "search":
		filter = &tg.ChannelParticipantsSearch{Q: p.Query}
	default:
		return nil, fmt.Errorf("unknown filter %q (recent|admins|kicked|banned|search)", p.Filter)
	}
	res, err := g.scan().ChannelsGetParticipants(ctx, &tg.ChannelsGetParticipantsRequest{
		Channel: &tg.InputChannel{ChannelID: ch.ID, AccessHash: ch.AccessHash},
		Filter:  filter,
		Offset:  p.Offset,
		Limit:   p.Limit,
	})
	if err != nil {
		return nil, err
	}
	list, _ := res.(*tg.ChannelsChannelParticipants)
	parts := []tg.ChannelParticipantClass{}
	if list != nil {
		parts = list.Participants
	}
	out := make([]map[string]any, 0, len(parts))
	for _, part := range parts {
		out = append(out, channelParticipantDict(part))
	}
	// the users riding the response (access hashes for SetAdmin) — a
	// separate array, additive for existing consumers
	if list != nil && len(list.Users) > 0 {
		users := make([]map[string]any, 0, len(list.Users))
		for _, u := range list.Users {
			if user, ok := u.(*tg.User); ok {
				g.cacheUser(user)
				users = append(users, map[string]any{
					"_": "User", "id": user.ID, "access_hash": user.AccessHash,
					"username": user.Username, "is_bot": user.Bot,
					"first_name": user.FirstName,
				})
			}
		}
		if len(users) > 0 {
			out = append(out, map[string]any{"_": "Users", "users": users})
		}
	}
	return out, nil
}

// SendDocument sends an existing document by reference (server-side
// copy — messages.sendMedia with InputMediaDocument; no upload/down-
// load). When the reference expired and the caller named the source
// message (ChatID+MessageID), it is refetched ONCE for a fresh
// reference and the send retried — the central refresh point (ported
// from the scanner's CopyMedia).
func (g *gotdClient) SendDocument(ctx context.Context, chatID int64, doc DocRef, caption, parseMode string) (SentMessage, error) {
	updates, err := g.sendDocRef(ctx, chatID, doc, caption, parseMode)
	if err == nil {
		return extractSentMessage(updates, chatID), nil
	}
	if !tgerr.Is(err, "FILE_REFERENCE_EXPIRED") || doc.ChatID == 0 || doc.MessageID == 0 {
		return SentMessage{}, err
	}
	fresh, rerr := g.refreshDocRef(ctx, doc)
	if rerr != nil {
		return SentMessage{}, err // surface the original expiry error
	}
	updates, err = g.sendDocRef(ctx, chatID, fresh, caption, parseMode)
	if err != nil {
		return SentMessage{}, err
	}
	return extractSentMessage(updates, chatID), nil
}

func (g *gotdClient) sendDocRef(ctx context.Context, chatID int64, doc DocRef, caption, parseMode string) (tg.UpdatesClass, error) {
	media := &tg.InputMediaDocument{
		ID: &tg.InputDocument{
			ID:            doc.ID,
			AccessHash:    doc.AccessHash,
			FileReference: doc.FileReference,
		},
	}
	sender := message.NewSender(g.raw)
	rb := sender.To(g.peerFor(ctx, chatID))
	builder := &rb.Builder
	var opts []message.StyledTextOption
	if caption != "" {
		opts = captionOption(g, SendParams{Text: caption, ParseMode: parseMode})
	}
	return builder.Media(ctx, message.Media(media, opts...))
}

// refreshDocRef re-reads the source message and extracts a live
// document reference.
func (g *gotdClient) refreshDocRef(ctx context.Context, doc DocRef) (DocRef, error) {
	ch, err := g.resolveChannel(ctx, doc.ChatID)
	if err != nil {
		return DocRef{}, err
	}
	res, err := g.scan().ChannelsGetMessages(ctx, &tg.ChannelsGetMessagesRequest{
		Channel: &tg.InputChannel{ChannelID: ch.ID, AccessHash: ch.AccessHash},
		ID:      []tg.InputMessageClass{&tg.InputMessageID{ID: int(doc.MessageID)}},
	})
	if err != nil {
		return DocRef{}, err
	}
	msgs, _, _ := unpackMessagesClass(res)
	for _, mc := range msgs {
		m, ok := mc.(*tg.Message)
		if !ok {
			continue
		}
		md, ok := m.Media.(*tg.MessageMediaDocument)
		if !ok {
			continue
		}
		if d, ok := md.Document.(*tg.Document); ok && d.ID == doc.ID {
			return DocRef{ID: d.ID, AccessHash: d.AccessHash, FileReference: d.FileReference,
				ChatID: doc.ChatID, MessageID: doc.MessageID}, nil
		}
	}
	return DocRef{}, fmt.Errorf("source message %d no longer carries document %d", doc.MessageID, doc.ID)
}

// CreateChannel is channels.createChannel (broadcast by default,
// megagroup for group-forums).
func (g *gotdClient) CreateChannel(ctx context.Context, title, about string, megagroup bool) (map[string]any, error) {
	res, err := g.scan().ChannelsCreateChannel(ctx, &tg.ChannelsCreateChannelRequest{
		Title: title, About: about, Megagroup: megagroup,
	})
	if err != nil {
		return nil, err
	}
	var chats []tg.ChatClass
	switch u := res.(type) {
	case *tg.Updates:
		chats = u.Chats
	}
	for _, chat := range chats {
		if ch, ok := chat.(*tg.Channel); ok {
			d := channelDict(ch)
			d["access_hash"] = ch.AccessHash
			return d, nil
		}
	}
	return nil, fmt.Errorf("channel created but not returned in updates")
}

// EditChannel folds the settings toggles: title via channels.editTitle,
// about via messages.editChatAbout, plus toggleSignatures,
// togglePreHistoryHidden, and noforwards. Nil pointers keep the value.
func (g *gotdClient) EditChannel(ctx context.Context, p EditChannelParams) (EditResult, error) {
	ch, err := g.resolveChannel(ctx, p.ChatID)
	if err != nil {
		return EditResult{}, err
	}
	input := &tg.InputChannel{ChannelID: ch.ID, AccessHash: ch.AccessHash}
	if p.Title != nil {
		if _, err := g.scan().ChannelsEditTitle(ctx, &tg.ChannelsEditTitleRequest{
			Channel: input, Title: *p.Title,
		}); err != nil {
			return EditResult{}, err
		}
	}
	if p.About != nil {
		if _, err := g.scan().MessagesEditChatAbout(ctx, &tg.MessagesEditChatAboutRequest{
			Peer:  &tg.InputPeerChannel{ChannelID: ch.ID, AccessHash: ch.AccessHash},
			About: *p.About,
		}); err != nil {
			return EditResult{}, err
		}
	}
	if p.Signatures != nil {
		if _, err := g.scan().ChannelsToggleSignatures(ctx, &tg.ChannelsToggleSignaturesRequest{
			Channel: input, SignaturesEnabled: *p.Signatures,
		}); err != nil {
			return EditResult{}, err
		}
	}
	if p.PreHistoryHidden != nil {
		if _, err := g.scan().ChannelsTogglePreHistoryHidden(ctx, &tg.ChannelsTogglePreHistoryHiddenRequest{
			Channel: input, Enabled: *p.PreHistoryHidden,
		}); err != nil {
			return EditResult{}, err
		}
	}
	if p.NoForwards != nil {
		if _, err := g.scan().MessagesToggleNoForwards(ctx, &tg.MessagesToggleNoForwardsRequest{
			Peer:    &tg.InputPeerChannel{ChannelID: ch.ID, AccessHash: ch.AccessHash},
			Enabled: *p.NoForwards,
		}); err != nil {
			return EditResult{}, err
		}
	}
	return EditResult{Ok: true}, nil
}

// DeleteChannel is channels.deleteChannel (creator only).
func (g *gotdClient) DeleteChannel(ctx context.Context, chatID int64) (EditResult, error) {
	ch, err := g.resolveChannel(ctx, chatID)
	if err != nil {
		return EditResult{}, err
	}
	_, err = g.scan().ChannelsDeleteChannel(ctx, &tg.InputChannel{
		ChannelID: ch.ID, AccessHash: ch.AccessHash,
	})
	return EditResult{Ok: err == nil}, err
}

// scanResolveUser resolves a user id (with access hash) through the
// scan RPC layer — fakeable in tests, unlike resolveUser's raw path.
func (g *gotdClient) scanResolveUser(ctx context.Context, userID int64) (tg.InputUserClass, error) {
	users, err := g.scan().UsersGetUsers(ctx, []tg.InputUserClass{&tg.InputUser{UserID: userID}})
	if err != nil {
		return nil, err
	}
	if len(users) == 0 {
		return nil, fmt.Errorf("user %d not found", userID)
	}
	if u, ok := users[0].(*tg.User); ok {
		return &tg.InputUser{UserID: u.ID, AccessHash: u.AccessHash}, nil
	}
	return nil, fmt.Errorf("user %d resolved to a non-user object", userID)
}

// EditAdmin is channels.editAdmin with named rights ("anonymous" is the
// anonymous-admin flag the scanner's provisioner sets on itself).
func (g *gotdClient) EditAdmin(ctx context.Context, p EditAdminParams) (EditResult, error) {
	ch, err := g.resolveChannel(ctx, p.ChatID)
	if err != nil {
		return EditResult{}, err
	}
	user, err := g.scanResolveUser(ctx, p.UserID)
	if err != nil {
		return EditResult{}, err
	}
	_, err = g.scan().ChannelsEditAdmin(ctx, &tg.ChannelsEditAdminRequest{
		Channel:     &tg.InputChannel{ChannelID: ch.ID, AccessHash: ch.AccessHash},
		UserID:      user,
		AdminRights: adminRightsFromNames(p.Rights),
		Rank:        p.Rank,
	})
	return EditResult{Ok: err == nil}, err
}

// JoinChannel is channels.joinChannel.
func (g *gotdClient) JoinChannel(ctx context.Context, chatID int64) (EditResult, error) {
	ch, err := g.resolveChannel(ctx, chatID)
	if err != nil {
		return EditResult{}, err
	}
	_, err = g.scan().ChannelsJoinChannel(ctx, &tg.InputChannel{
		ChannelID: ch.ID, AccessHash: ch.AccessHash,
	})
	return EditResult{Ok: err == nil}, err
}

// LeaveChannel is channels.leaveChannel.
func (g *gotdClient) LeaveChannel(ctx context.Context, chatID int64) (EditResult, error) {
	ch, err := g.resolveChannel(ctx, chatID)
	if err != nil {
		return EditResult{}, err
	}
	_, err = g.scan().ChannelsLeaveChannel(ctx, &tg.InputChannel{
		ChannelID: ch.ID, AccessHash: ch.AccessHash,
	})
	return EditResult{Ok: err == nil}, err
}

// ReadHistory marks messages as read (channels.readHistory for
// channels, messages.readHistory otherwise). This is the ONLY read
// path — get_history never acknowledges.
func (g *gotdClient) ReadHistory(ctx context.Context, chatID, maxID int64) (EditResult, error) {
	var err error
	if chatID <= -1000000000000 {
		var ch *tg.Channel
		ch, err = g.resolveChannel(ctx, chatID)
		if err != nil {
			return EditResult{}, err
		}
		_, err = g.scan().ChannelsReadHistory(ctx, &tg.ChannelsReadHistoryRequest{
			Channel: &tg.InputChannel{ChannelID: ch.ID, AccessHash: ch.AccessHash},
			MaxID:   int(maxID),
		})
	} else {
		_, err = g.scan().MessagesReadHistory(ctx, &tg.MessagesReadHistoryRequest{
			Peer: g.peerFor(ctx, chatID), MaxID: int(maxID),
		})
	}
	return EditResult{Ok: err == nil}, err
}

// SeedPeers bulk-loads known peers into the shared cache (the scanner
// seeds from its scan.channels table on boot).
func (g *gotdClient) SeedPeers(_ context.Context, peers []PeerSeed) (int, error) {
	if g.storeDB == nil {
		return 0, fmt.Errorf("no store attached")
	}
	n := 0
	for _, p := range peers {
		if p.AccessHash == 0 {
			continue // a zero hash would poison lookups
		}
		if err := g.storeDB.UpsertPeer(g.alias, store.PeerEntry{
			ID: p.ID, Kind: p.Kind, AccessHash: p.AccessHash,
			Username: p.Username, Title: p.Title,
		}); err != nil {
			return n, err
		}
		n++
	}
	return n, nil
}
