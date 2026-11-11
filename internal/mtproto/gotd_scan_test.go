package mtproto

// Tests for the gotd scan layer through a fake scanAPI: chunking,
// access-hash propagation, filter/rights mapping, serialization — the
// request-assembly logic, without Telegram.

import (
	"context"
	"testing"

	"github.com/gotd/td/tg"
)

// fakeScanAPI records requests and answers with canned objects.
type fakeScanAPI struct {
	channels     map[int64]*tg.Channel // resolveChannel answers
	users        map[int64]*tg.User
	historyReqs  []*tg.MessagesGetHistoryRequest
	getMsgReqs   []*tg.ChannelsGetMessagesRequest
	participants []tg.ChannelParticipantClass
	partsReqs    []*tg.ChannelsGetParticipantsRequest
	editAdmin    []*tg.ChannelsEditAdminRequest
	editTitle    []*tg.ChannelsEditTitleRequest
	abouts       []*tg.MessagesEditChatAboutRequest
	signatures   []*tg.ChannelsToggleSignaturesRequest
	preHistory   []*tg.ChannelsTogglePreHistoryHiddenRequest
	noForwards   []*tg.MessagesToggleNoForwardsRequest
	readChan     []*tg.ChannelsReadHistoryRequest
	readDM       []*tg.MessagesReadHistoryRequest
	messages     []tg.MessageClass // get_history/get_messages payload
	joinCalls    []int64
	leaveCalls   []int64
	deleteCalls  []int64
}

func newFakeScanAPI() *fakeScanAPI {
	return &fakeScanAPI{
		channels: map[int64]*tg.Channel{},
		users:    map[int64]*tg.User{},
	}
}

func (f *fakeScanAPI) UsersGetUsers(_ context.Context, ids []tg.InputUserClass) ([]tg.UserClass, error) {
	out := []tg.UserClass{}
	for _, id := range ids {
		if u, ok := id.(*tg.InputUser); ok {
			if user, found := f.users[u.UserID]; found {
				out = append(out, user)
			}
		}
	}
	return out, nil
}

func (f *fakeScanAPI) MessagesGetHistory(_ context.Context, req *tg.MessagesGetHistoryRequest) (tg.MessagesMessagesClass, error) {
	f.historyReqs = append(f.historyReqs, req)
	return &tg.MessagesMessages{Messages: f.messages}, nil
}

func (f *fakeScanAPI) ChannelsGetChannels(_ context.Context, ids []tg.InputChannelClass) (tg.MessagesChatsClass, error) {
	chats := []tg.ChatClass{}
	for _, id := range ids {
		if c, ok := id.(*tg.InputChannel); ok {
			if ch, found := f.channels[c.ChannelID]; found {
				chats = append(chats, ch)
			}
		}
	}
	return &tg.MessagesChats{Chats: chats}, nil
}

func (f *fakeScanAPI) ChannelsGetMessages(_ context.Context, req *tg.ChannelsGetMessagesRequest) (tg.MessagesMessagesClass, error) {
	f.getMsgReqs = append(f.getMsgReqs, req)
	// echo one message per requested id (the chunking test relies on it)
	msgs := make([]tg.MessageClass, 0, len(req.ID))
	for _, id := range req.ID {
		if m, ok := id.(*tg.InputMessageID); ok {
			msgs = append(msgs, &tg.Message{ID: m.ID, PeerID: &tg.PeerChannel{ChannelID: 42}})
		}
	}
	return &tg.MessagesMessages{Messages: msgs}, nil
}

func (f *fakeScanAPI) ChannelsGetFullChannel(ctx context.Context, channel tg.InputChannelClass) (*tg.MessagesChatFull, error) {
	n := 420
	onl := 12
	full := &tg.ChannelFull{About: "the about"}
	full.SetParticipantsCount(n)
	full.SetOnlineCount(onl)
	return &tg.MessagesChatFull{FullChat: full}, nil
}

func (f *fakeScanAPI) ChannelsGetParticipant(_ context.Context, req *tg.ChannelsGetParticipantRequest) (*tg.ChannelsChannelParticipant, error) {
	part := &tg.ChannelParticipantAdmin{UserID: 7, Rank: "scanner"}
	return &tg.ChannelsChannelParticipant{Participant: part}, nil
}

func (f *fakeScanAPI) ChannelsGetParticipants(_ context.Context, req *tg.ChannelsGetParticipantsRequest) (tg.ChannelsChannelParticipantsClass, error) {
	f.partsReqs = append(f.partsReqs, req)
	return &tg.ChannelsChannelParticipants{Participants: f.participants}, nil
}

func (f *fakeScanAPI) MessagesSendMessage(ctx context.Context, req *tg.MessagesSendMessageRequest) (tg.UpdatesClass, error) {
	return &tg.Updates{}, nil
}

func (f *fakeScanAPI) MessagesEditChatAbout(_ context.Context, req *tg.MessagesEditChatAboutRequest) (bool, error) {
	f.abouts = append(f.abouts, req)
	return true, nil
}

func (f *fakeScanAPI) ChannelsEditTitle(_ context.Context, req *tg.ChannelsEditTitleRequest) (tg.UpdatesClass, error) {
	f.editTitle = append(f.editTitle, req)
	return &tg.Updates{}, nil
}

func (f *fakeScanAPI) ChannelsToggleSignatures(_ context.Context, req *tg.ChannelsToggleSignaturesRequest) (tg.UpdatesClass, error) {
	f.signatures = append(f.signatures, req)
	return &tg.Updates{}, nil
}

func (f *fakeScanAPI) ChannelsTogglePreHistoryHidden(_ context.Context, req *tg.ChannelsTogglePreHistoryHiddenRequest) (tg.UpdatesClass, error) {
	f.preHistory = append(f.preHistory, req)
	return &tg.Updates{}, nil
}

func (f *fakeScanAPI) MessagesToggleNoForwards(_ context.Context, req *tg.MessagesToggleNoForwardsRequest) (tg.UpdatesClass, error) {
	f.noForwards = append(f.noForwards, req)
	return &tg.Updates{}, nil
}

func (f *fakeScanAPI) ChannelsCreateChannel(_ context.Context, req *tg.ChannelsCreateChannelRequest) (tg.UpdatesClass, error) {
	ch := &tg.Channel{ID: 999, Title: req.Title, AccessHash: 55, Broadcast: true}
	return &tg.Updates{Chats: []tg.ChatClass{ch}}, nil
}

func (f *fakeScanAPI) ChannelsDeleteChannel(_ context.Context, channel tg.InputChannelClass) (tg.UpdatesClass, error) {
	if c, ok := channel.(*tg.InputChannel); ok {
		f.deleteCalls = append(f.deleteCalls, c.ChannelID)
	}
	return &tg.Updates{}, nil
}

func (f *fakeScanAPI) ChannelsEditAdmin(_ context.Context, req *tg.ChannelsEditAdminRequest) (tg.UpdatesClass, error) {
	f.editAdmin = append(f.editAdmin, req)
	return &tg.Updates{}, nil
}

func (f *fakeScanAPI) ChannelsJoinChannel(_ context.Context, channel tg.InputChannelClass) (tg.MessagesChatInviteJoinResultClass, error) {
	if c, ok := channel.(*tg.InputChannel); ok {
		f.joinCalls = append(f.joinCalls, c.ChannelID)
	}
	return &tg.MessagesChatInviteJoinResultOk{}, nil
}

func (f *fakeScanAPI) ChannelsLeaveChannel(_ context.Context, channel tg.InputChannelClass) (tg.UpdatesClass, error) {
	if c, ok := channel.(*tg.InputChannel); ok {
		f.leaveCalls = append(f.leaveCalls, c.ChannelID)
	}
	return &tg.Updates{}, nil
}

func (f *fakeScanAPI) ChannelsReadHistory(_ context.Context, req *tg.ChannelsReadHistoryRequest) (bool, error) {
	f.readChan = append(f.readChan, req)
	return true, nil
}

func (f *fakeScanAPI) MessagesReadHistory(_ context.Context, req *tg.MessagesReadHistoryRequest) (*tg.MessagesAffectedMessages, error) {
	f.readDM = append(f.readDM, req)
	return &tg.MessagesAffectedMessages{}, nil
}

// newScanClient wires a gotdClient with the fake RPC layer.
func newScanClient(fake *fakeScanAPI) *gotdClient {
	g := &gotdClient{alias: "test"}
	g.scanFake = fake
	return g
}

func TestScanBareChannelID(t *testing.T) {
	if id, err := bareChannelID(-1001234567890); err != nil || id != 1234567890 {
		t.Fatalf("-100 form: %d %v", id, err)
	}
	if id, err := bareChannelID(1234567890); err != nil || id != 1234567890 {
		t.Fatalf("bare: %d %v", id, err)
	}
	if _, err := bareChannelID(-12345); err == nil {
		t.Fatal("legacy chat must be refused")
	}
}

func TestScanResolveChannelPropagatesHash(t *testing.T) {
	fake := newFakeScanAPI()
	fake.channels[42] = &tg.Channel{ID: 42, AccessHash: 4242, Title: "c", Broadcast: true}
	g := newScanClient(fake)

	// Bot API form and bare form resolve to the same channel
	for _, id := range []int64{-1000000000042, 42} {
		ch, err := g.resolveChannel(context.Background(), id)
		if err != nil || ch.ID != 42 || ch.AccessHash != 4242 {
			t.Fatalf("resolve %d: %+v %v", id, ch, err)
		}
	}
	if _, err := g.resolveChannel(context.Background(), -7); err == nil {
		t.Fatal("legacy chat refused")
	}
	if _, err := g.resolveChannel(context.Background(), -1000000000099); err == nil {
		t.Fatal("unknown channel refused")
	}
}

func TestScanGetHistoryWindow(t *testing.T) {
	fake := newFakeScanAPI()
	fake.messages = []tg.MessageClass{
		&tg.Message{ID: 11, Message: "b", PeerID: &tg.PeerChannel{ChannelID: 42}},
		&tg.Message{ID: 10, Message: "a", PeerID: &tg.PeerChannel{ChannelID: 42}},
		&tg.MessageEmpty{ID: 9}, // skipped by the serializer
	}
	g := newScanClient(fake)
	msgs, err := g.GetHistory(context.Background(), -1000000000042, 15, -2, 5, 2)
	if err != nil {
		t.Fatal(err)
	}
	if len(fake.historyReqs) != 1 {
		t.Fatalf("expected one window call, got %d", len(fake.historyReqs))
	}
	req := fake.historyReqs[0]
	if req.OffsetID != 15 || req.AddOffset != -2 || req.MinID != 5 || req.Limit != 2 {
		t.Fatalf("window params: %+v", req)
	}
	if len(msgs) != 2 || msgs[0]["id"] != int64(11) {
		t.Fatalf("msgs=%v", msgs)
	}
}

func TestScanGetMessagesChunkingAndHash(t *testing.T) {
	fake := newFakeScanAPI()
	fake.channels[42] = &tg.Channel{ID: 42, AccessHash: 4242}
	fake.messages = []tg.MessageClass{&tg.Message{ID: 5, PeerID: &tg.PeerChannel{ChannelID: 42}}}
	g := newScanClient(fake)

	ids := make([]int64, 450) // > 2 chunks of 200
	for i := range ids {
		ids[i] = int64(i + 1)
	}
	msgs, err := g.GetMessages(context.Background(), 42, ids)
	if err != nil {
		t.Fatal(err)
	}
	if len(fake.getMsgReqs) != 3 { // 200 + 200 + 50
		t.Fatalf("chunks=%d", len(fake.getMsgReqs))
	}
	if fake.getMsgReqs[0].Channel.(*tg.InputChannel).AccessHash != 4242 {
		t.Fatal("access hash must ride from resolveChannel")
	}
	if len(fake.getMsgReqs[2].ID) != 50 {
		t.Fatalf("last chunk=%d", len(fake.getMsgReqs[2].ID))
	}
	// every requested id returns (fake returns all); order preserved
	if len(msgs) != 450 {
		t.Fatalf("msgs=%d", len(msgs))
	}
}

func TestScanGetFullChannelAndParticipants(t *testing.T) {
	fake := newFakeScanAPI()
	fake.channels[42] = &tg.Channel{ID: 42, AccessHash: 4242, Title: "c"}
	g := newScanClient(fake)

	full, err := g.GetFullChannel(context.Background(), 42)
	if err != nil || full["about"] != "the about" || full["participants_count"] != int64(420) {
		t.Fatalf("full=%v err=%v", full, err)
	}

	fake.participants = []tg.ChannelParticipantClass{
		&tg.ChannelParticipantCreator{UserID: 1, AdminRights: tg.ChatAdminRights{ChangeInfo: true}},
		&tg.ChannelParticipantBanned{Peer: &tg.PeerUser{UserID: 9}, KickedBy: 3},
		&tg.ChannelParticipantLeft{Peer: &tg.PeerUser{UserID: 8}},
	}
	parts, err := g.GetParticipants(context.Background(), ParticipantsParams{ChatID: 42, Filter: "admins"})
	if err != nil {
		t.Fatal(err)
	}
	if len(parts) != 3 {
		t.Fatalf("parts=%v", parts)
	}
	if parts[0]["_"] != "ChannelParticipantCreator" {
		t.Fatalf("creator=%v", parts[0])
	}
	if rights := parts[0]["admin_rights"]; len(rights.([]string)) != 1 || rights.([]string)[0] != "change_info" {
		t.Fatalf("rights=%v", rights)
	}
	if parts[1]["user_id"] != int64(9) || parts[1]["kicked_by"] != int64(3) {
		t.Fatalf("banned=%v", parts[1])
	}
	if parts[2]["user_id"] != int64(8) {
		t.Fatalf("left=%v", parts[2])
	}
	// filter mapping rides through
	if _, ok := fake.partsReqs[0].Filter.(*tg.ChannelParticipantsAdmins); !ok {
		t.Fatalf("filter=%T", fake.partsReqs[0].Filter)
	}

	part, err := g.GetParticipant(context.Background(), 42, 7)
	if err != nil || part["rank"] != "scanner" {
		t.Fatalf("participant=%v err=%v", part, err)
	}
}

func TestScanAdminRightsRoundtrip(t *testing.T) {
	fake := newFakeScanAPI()
	fake.channels[42] = &tg.Channel{ID: 42, AccessHash: 4242}
	fake.users[7] = &tg.User{ID: 7, AccessHash: 77}
	g := newScanClient(fake)

	names := []string{"post_messages", "delete_messages", "anonymous"}
	res, err := g.EditAdmin(context.Background(), EditAdminParams{
		ChatID: 42, UserID: 7, Rights: names, Rank: "forwarder",
	})
	if err != nil || !res.Ok {
		t.Fatalf("edit admin=%v %v", res, err)
	}
	req := fake.editAdmin[0]
	if !req.AdminRights.PostMessages || !req.AdminRights.DeleteMessages ||
		!req.AdminRights.Anonymous || req.AdminRights.AddAdmins {
		t.Fatalf("rights mapping: %+v", req.AdminRights)
	}
	if req.Rank != "forwarder" {
		t.Fatalf("rank=%q", req.Rank)
	}
	// dict roundtrip
	back := adminRightsDict(&req.AdminRights)
	if len(back) != len(names) {
		t.Fatalf("dict=%v", back)
	}
}

func TestScanEditChannelCallsAndReadHistory(t *testing.T) {
	fake := newFakeScanAPI()
	fake.channels[42] = &tg.Channel{ID: 42, AccessHash: 4242, Title: "old"}
	g := newScanClient(fake)
	ctx := context.Background()

	title := "new"
	about := "about text"
	signatures, hidden, nofwd := true, false, true
	if _, err := g.EditChannel(ctx, EditChannelParams{
		ChatID: 42, Title: &title, About: &about,
		Signatures: &signatures, PreHistoryHidden: &hidden, NoForwards: &nofwd,
	}); err != nil {
		t.Fatal(err)
	}
	if fake.editTitle[0].Title != "new" {
		t.Fatalf("title=%v", fake.editTitle[0])
	}
	if fake.abouts[0].About != "about text" {
		t.Fatalf("about=%v", fake.abouts[0])
	}
	if !fake.signatures[0].SignaturesEnabled {
		t.Fatal("signatures toggle")
	}
	if fake.preHistory[0].Enabled {
		t.Fatal("pre-history toggle")
	}
	if !fake.noForwards[0].Enabled {
		t.Fatal("noforwards toggle")
	}

	// read: channel path vs DM path
	if _, err := g.ReadHistory(ctx, -1000000000042, 55); err != nil {
		t.Fatal(err)
	}
	if len(fake.readChan) != 1 || fake.readChan[0].MaxID != 55 {
		t.Fatalf("readChan=%v", fake.readChan)
	}
	if _, err := g.ReadHistory(ctx, 77, 9); err != nil {
		t.Fatal(err)
	}
	if len(fake.readDM) != 1 || fake.readDM[0].Peer.(*tg.InputPeerUser).UserID != 77 {
		t.Fatalf("readDM=%v", fake.readDM)
	}
}

func TestScanCreateJoinLeaveDelete(t *testing.T) {
	fake := newFakeScanAPI()
	fake.channels[42] = &tg.Channel{ID: 42, AccessHash: 4242}
	g := newScanClient(fake)
	ctx := context.Background()

	created, err := g.CreateChannel(ctx, "storage", "about", false)
	if err != nil || created["id"] != int64(999) || created["access_hash"] != int64(55) {
		t.Fatalf("created=%v err=%v", created, err)
	}
	if _, err := g.JoinChannel(ctx, 42); err != nil {
		t.Fatal(err)
	}
	if _, err := g.LeaveChannel(ctx, 42); err != nil {
		t.Fatal(err)
	}
	if _, err := g.DeleteChannel(ctx, 42); err != nil {
		t.Fatal(err)
	}
	if len(fake.joinCalls) != 1 || fake.joinCalls[0] != 42 ||
		len(fake.leaveCalls) != 1 || fake.leaveCalls[0] != 42 ||
		len(fake.deleteCalls) != 1 || fake.deleteCalls[0] != 42 {
		t.Fatalf("calls join=%v leave=%v delete=%v", fake.joinCalls, fake.leaveCalls, fake.deleteCalls)
	}
}

func TestScanGetEntity(t *testing.T) {
	fake := newFakeScanAPI()
	fake.channels[42] = &tg.Channel{ID: 42, AccessHash: 4242, Title: "c", Username: "cname"}
	fake.users[7] = &tg.User{ID: 7, AccessHash: 77, Username: "uname"}
	g := newScanClient(fake)

	u, err := g.GetEntity(context.Background(), 7)
	if err != nil || u["_"] != "User" || u["access_hash"] != int64(77) {
		t.Fatalf("user=%v err=%v", u, err)
	}
	c, err := g.GetEntity(context.Background(), -1000000000042)
	if err != nil || c["_"] != "Channel" || c["username"] != "cname" || c["access_hash"] != int64(4242) {
		t.Fatalf("channel=%v err=%v", c, err)
	}
}
