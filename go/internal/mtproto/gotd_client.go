package mtproto

import (
	"bytes"
	"context"
	"database/sql"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gotd/td/session"
	"github.com/gotd/td/telegram"
	"github.com/gotd/td/telegram/downloader"
	"github.com/gotd/td/telegram/message"
	"github.com/gotd/td/telegram/message/entity"
	"github.com/gotd/td/telegram/message/html"
	"github.com/gotd/td/telegram/message/styling"
	"github.com/gotd/td/telegram/uploader"
	"github.com/gotd/td/tg"
	"github.com/gotd/td/tgerr"

	"github.com/DovBerKaplan/tg-session-gateway/go/internal/config"
	"github.com/DovBerKaplan/tg-session-gateway/go/internal/store"
)

// uploadCounter serializes upload handles across clients in-process.
var uploadCounter atomic.Int64

// gotdClient is the production Client: a gotd telegram.Client per bot
// token with the session persisted in our SQLite — a migrated pyrofork
// auth_key opens here without a new login. HTML parse_mode goes through
// gotd's html.String, which supports <blockquote expandable> (the
// pyrofork migration reason).
type gotdClient struct {
	cfg     config.MTProto
	alias   string
	storeDB *store.MT

	client        *telegram.Client
	raw           *tg.Client
	runCancel     context.CancelFunc
	updateHandler func(map[string]any)

	// uploads maps this-session upload handles to their InputFile
	// (InputFileBig is reusable until consumed server-side).
	upMu    sync.Mutex
	uploads map[string]tg.InputFileClass

	// media locations seen in updates (id → photo/document with
	// access_hash + file_reference) so "photo:<id>" refs can download.
	mediaMu   sync.Mutex
	photoLocs map[int64]*tg.Photo
	docLocs   map[int64]*tg.Document
}

// sqliteSessionStorage persists gotd session blobs in mtgateway.db —
// the auth_key never sits on the container layer.
type sqliteSessionStorage struct {
	db    *sql.DB
	alias string
}

func (s sqliteSessionStorage) LoadSession(ctx context.Context) ([]byte, error) {
	if _, err := s.db.Exec(
		"CREATE TABLE IF NOT EXISTS gotd_sessions (alias TEXT PRIMARY KEY, session_blob BLOB NOT NULL)"); err != nil {
		return nil, err
	}
	var data []byte
	err := s.db.QueryRow("SELECT session_blob FROM gotd_sessions WHERE alias = ?", s.alias).Scan(&data)
	if err == sql.ErrNoRows {
		return nil, session.ErrNotFound
	}
	return data, err
}

func (s sqliteSessionStorage) StoreSession(ctx context.Context, data []byte) error {
	if _, err := s.db.Exec(
		"CREATE TABLE IF NOT EXISTS gotd_sessions (alias TEXT PRIMARY KEY, session_blob BLOB NOT NULL)"); err != nil {
		return err
	}
	_, err := s.db.Exec(
		"INSERT INTO gotd_sessions (alias, session_blob) VALUES (?, ?) "+
			"ON CONFLICT(alias) DO UPDATE SET session_blob = excluded.session_blob",
		s.alias, data)
	return err
}

func newGotdClient(cfg config.MTProto, alias string, st *store.MT) *gotdClient {
	g := &gotdClient{
		cfg:       cfg,
		alias:     alias,
		storeDB:   st,
		uploads:   map[string]tg.InputFileClass{},
		photoLocs: map[int64]*tg.Photo{},
		docLocs:   map[int64]*tg.Document{},
	}
	storage := sqliteSessionStorage{db: st.DB, alias: alias}
	g.client = telegram.NewClient(cfg.APIID, cfg.APIHash, telegram.Options{
		SessionStorage: &storage,
		UpdateHandler:  telegram.UpdateHandlerFunc(g.onUpdatesClass),
	})
	g.raw = g.client.API()
	return g
}

// onUpdatesClass fans a Telegram updates batch out into serialized
// Pyrogram-shaped single updates, and remembers media locations so
// "photo:<id>" / "document:<id>" refs can download later.
func (g *gotdClient) onUpdatesClass(ctx context.Context, class tg.UpdatesClass) error {
	for _, u := range flattenUpdates(class) {
		g.cacheMediaLocations(u)
		if g.updateHandler != nil {
			if d := SerializeUpdate(u); d != nil {
				g.updateHandler(d)
			}
		}
	}
	return nil
}

// cacheMediaLocations stores photos/documents carried by message
// updates (access_hash + file_reference are needed to download).
func (g *gotdClient) cacheMediaLocations(u tg.UpdateClass) {
	var msg *tg.Message
	switch up := u.(type) {
	case *tg.UpdateNewMessage:
		msg, _ = up.Message.(*tg.Message)
	case *tg.UpdateNewChannelMessage:
		msg, _ = up.Message.(*tg.Message)
	case *tg.UpdateEditMessage:
		msg, _ = up.Message.(*tg.Message)
	case *tg.UpdateEditChannelMessage:
		msg, _ = up.Message.(*tg.Message)
	}
	if msg == nil || msg.Media == nil {
		return
	}
	g.mediaMu.Lock()
	defer g.mediaMu.Unlock()
	switch media := msg.Media.(type) {
	case *tg.MessageMediaPhoto:
		if photo, ok := media.Photo.(*tg.Photo); ok {
			g.photoLocs[photo.ID] = photo
		}
	case *tg.MessageMediaDocument:
		if doc, ok := media.Document.(*tg.Document); ok {
			g.docLocs[doc.ID] = doc
		}
	}
}

// flattenUpdates expands an UpdatesClass batch into individual updates.
func flattenUpdates(class tg.UpdatesClass) []tg.UpdateClass {
	var out []tg.UpdateClass
	switch ups := class.(type) {
	case *tg.Updates:
		out = append(out, ups.Updates...)
	case *tg.UpdateShort:
		out = append(out, ups.Update)
	case *tg.UpdateShortMessage:
		// short private message — synthesize the UpdateNewMessage shape
		out = append(out, &tg.UpdateNewMessage{
			Message: &tg.Message{
				ID:        ups.ID,
				Out:       ups.Out,
				Mentioned: ups.Mentioned,
				Date:      ups.Date,
				PeerID:    &tg.PeerUser{UserID: ups.UserID},
				Message:   ups.Message,
			},
			Pts: ups.Pts,
		})
	case *tg.UpdatesCombined:
		out = append(out, ups.Updates...)
	}
	return out
}

func (g *gotdClient) Connect(ctx context.Context) error {
	runCtx, cancel := context.WithCancel(context.Background())
	g.runCancel = cancel
	ready := make(chan error, 1)
	go func() {
		_ = g.client.Run(runCtx, func(ctx context.Context) error {
			// an already-valid (migrated) session skips auth; a fresh
			// one logs in with the bot token — once per token lifetime
			authCtx, authCancel := context.WithTimeout(ctx, 60*time.Second)
			defer authCancel()
			status, err := g.client.Auth().Status(authCtx)
			if err != nil {
				ready <- err
				return err
			}
			if !status.Authorized {
				if _, err := g.client.Auth().Bot(authCtx, g.botToken()); err != nil {
					err = fmt.Errorf("bot auth: %w", err)
					ready <- err
					return err
				}
			}
			ready <- nil
			<-ctx.Done()
			return ctx.Err()
		})
	}()
	select {
	case err := <-ready:
		if err != nil {
			cancel()
			g.runCancel = nil
			return err
		}
		return nil
	case <-time.After(75 * time.Second):
		cancel()
		g.runCancel = nil
		return fmt.Errorf("connect timeout")
	}
}

// botToken reads the token through the store (registered, encrypted).
func (g *gotdClient) botToken() string {
	tok, _ := g.storeDB.GetToken(g.alias)
	return tok
}

func (g *gotdClient) Disconnect(ctx context.Context) error {
	if g.runCancel != nil {
		g.runCancel()
		g.runCancel = nil
	}
	return nil
}

func (g *gotdClient) IsConnected() bool { return g.runCancel != nil }

func (g *gotdClient) Ping(ctx context.Context) error {
	_, err := g.raw.UsersGetUsers(ctx, []tg.InputUserClass{&tg.InputUserSelf{}})
	return err
}

func (g *gotdClient) Me(ctx context.Context) (BotInfo, error) {
	user, err := g.raw.UsersGetUsers(ctx, []tg.InputUserClass{&tg.InputUserSelf{}})
	if err != nil {
		return BotInfo{}, err
	}
	if len(user) == 0 {
		return BotInfo{}, fmt.Errorf("empty self")
	}
	if me, ok := user[0].(*tg.User); ok {
		return BotInfo{Username: me.Username, ID: me.ID}, nil
	}
	return BotInfo{}, nil
}

func (g *gotdClient) WarmPeers(ctx context.Context) error {
	// walk dialogs once so access hashes are known for first sends
	_, err := g.raw.MessagesGetDialogs(ctx, &tg.MessagesGetDialogsRequest{
		OffsetPeer: &tg.InputPeerEmpty{},
		Limit:      500,
	})
	return err
}

func (g *gotdClient) SendMessage(ctx context.Context, p SendParams) (SentMessage, error) {
	sender := message.NewSender(g.raw)
	var markup tg.ReplyMarkupClass
	if p.ReplyMarkup != nil {
		markup = CoerceReplyMarkup(p.ReplyMarkup)
	}
	rb := sender.To(inputPeerOf(p.ChatID))
	builder := &rb.Builder
	if markup != nil {
		builder = rb.Markup(markup)
	}
	if p.ReplyToMsgID != 0 {
		builder = builder.Reply(int(p.ReplyToMsgID))
	}
	if p.DisableNotify {
		builder = builder.Silent()
	}
	var updates tg.UpdatesClass
	var err error
	if p.Media != nil {
		input, mErr := g.buildInputMedia(ctx, p.Media)
		if mErr != nil {
			return SentMessage{}, mErr
		}
		updates, err = builder.Media(ctx, message.Media(input, captionOptions(p)...))
	} else if strings.EqualFold(p.ParseMode, "html") {
		updates, err = builder.StyledText(ctx, html.String(nil, p.Text))
	} else {
		updates, err = builder.Text(ctx, p.Text)
	}
	if err != nil {
		return SentMessage{}, err
	}
	return extractSentMessage(updates, p.ChatID), nil
}

// captionOptions styles the media caption the same way message text is
// styled (HTML parse mode rides through).
func captionOptions(p SendParams) []message.StyledTextOption {
	if strings.EqualFold(p.ParseMode, "html") && p.Text != "" {
		return []message.StyledTextOption{html.String(nil, p.Text)}
	}
	if p.Text != "" {
		return []message.StyledTextOption{styling.Plain(p.Text)}
	}
	return nil
}

var mediaMimeTypes = map[string]string{
	"photo":    "image/jpeg",
	"video":    "video/mp4",
	"audio":    "audio/mpeg",
	"document": "application/octet-stream",
}

// buildInputMedia resolves a MediaAttach into the gotd input media.
func (g *gotdClient) buildInputMedia(ctx context.Context, m *MediaAttach) (tg.InputMediaClass, error) {
	mime := m.MimeType
	if mime == "" {
		mime = mediaMimeTypes[m.Kind]
	}
	name := m.FileName
	if name == "" {
		name = "upload." + m.Kind
	}

	if m.URL != "" {
		// Telegram fetches the URL itself
		if m.Kind == "photo" {
			return &tg.InputMediaPhotoExternal{URL: m.URL}, nil
		}
		return &tg.InputMediaDocumentExternal{URL: m.URL}, nil
	}

	g.upMu.Lock()
	file, ok := g.uploads[m.Handle]
	g.upMu.Unlock()
	if !ok {
		return nil, fmt.Errorf("unknown upload handle %q (uploads live per session; re-POST /upload after a restart)", m.Handle)
	}

	if m.Kind == "photo" {
		return &tg.InputMediaUploadedPhoto{File: file}, nil
	}
	attrs := []tg.DocumentAttributeClass{&tg.DocumentAttributeFilename{FileName: name}}
	switch m.Kind {
	case "video":
		attrs = append(attrs, &tg.DocumentAttributeVideo{SupportsStreaming: true})
	case "audio":
		attrs = append(attrs, &tg.DocumentAttributeAudio{})
	}
	return &tg.InputMediaUploadedDocument{
		File:       file,
		MimeType:   mime,
		Attributes: attrs,
	}, nil
}

// UploadMedia uploads bytes to Telegram and returns a session-scoped
// handle ("upload:<id>") that send_photo/video/document/audio accept.
func (g *gotdClient) UploadMedia(ctx context.Context, kind, filename string, data []byte) (string, error) {
	if filename == "" {
		filename = "upload." + kind
	}
	if g.raw == nil {
		return "", fmt.Errorf("client not connected")
	}
	file, err := uploader.NewUploader(g.raw).FromBytes(ctx, filename, data)
	if err != nil {
		return "", err
	}
	handle := fmt.Sprintf("upload:%d", uploadCounter.Add(1))
	g.upMu.Lock()
	g.uploads[handle] = file
	g.upMu.Unlock()
	return handle, nil
}

// DownloadMedia streams a "photo:<id>" / "document:<id>" reference
// (as SerializeMedia renders) to bytes. Locations come from updates
// this session observed — Telegram needs access_hash + file_reference,
// which a bare id does not carry.
func (g *gotdClient) DownloadMedia(ctx context.Context, fileID string) ([]byte, error) {
	kind, idStr, ok := strings.Cut(fileID, ":")
	if !ok {
		return nil, fmt.Errorf("file id must be photo:<id> or document:<id>")
	}
	id, err := strconv.ParseInt(idStr, 10, 64)
	if err != nil {
		return nil, fmt.Errorf("bad file id %q", fileID)
	}

	var loc tg.InputFileLocationClass
	g.mediaMu.Lock()
	switch kind {
	case "photo":
		photo, ok := g.photoLocs[id]
		if !ok {
			g.mediaMu.Unlock()
			return nil, fmt.Errorf("photo %d not seen in this session's updates (no access_hash to download it)", id)
		}
		thumb := largestThumbSize(photo.Sizes)
		loc = &tg.InputPhotoFileLocation{
			ID:            photo.ID,
			AccessHash:    photo.AccessHash,
			FileReference: photo.FileReference,
			ThumbSize:     thumb,
		}
	case "document":
		doc, ok := g.docLocs[id]
		if !ok {
			g.mediaMu.Unlock()
			return nil, fmt.Errorf("document %d not seen in this session's updates (no access_hash to download it)", id)
		}
		loc = &tg.InputDocumentFileLocation{
			ID:            doc.ID,
			AccessHash:    doc.AccessHash,
			FileReference: doc.FileReference,
		}
	default:
		g.mediaMu.Unlock()
		return nil, fmt.Errorf("unknown media kind %q", kind)
	}
	g.mediaMu.Unlock()

	if g.raw == nil {
		return nil, fmt.Errorf("client not connected")
	}
	var buf bytes.Buffer
	if _, err := downloader.NewDownloader().Download(g.raw, loc).Stream(ctx, &buf); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// largestThumbSize picks the biggest photo size type ("y" beats "x"…).
func largestThumbSize(sizes []tg.PhotoSizeClass) string {
	best, bestArea := "", -1
	for _, s := range sizes {
		if psz, ok := s.(*tg.PhotoSize); ok {
			if area := psz.W * psz.H; area > bestArea {
				bestArea, best = area, psz.Type
			}
		}
	}
	if best == "" {
		return "m" // sane default Telegram always has
	}
	return best
}

func inputPeerOf(chatID int64) tg.InputPeerClass {
	if chatID > 0 {
		return &tg.InputPeerUser{UserID: chatID}
	}
	// Bot API style: -100xxxxxxxxxx channels, -xxxxxxx chats
	if chatID <= -1000000000000 {
		return &tg.InputPeerChannel{ChannelID: -(chatID + 1000000000000)}
	}
	return &tg.InputPeerChat{ChatID: -chatID}
}

func extractSentMessage(updates tg.UpdatesClass, chatID int64) SentMessage {
	var found SentMessage
	walkMessages(updates, func(m *tg.Message) bool {
		if found.ID == 0 {
			found = SentMessage{ID: int64(m.ID), ChatID: chatID}
		}
		return false
	})
	return found
}

func walkMessages(u tg.UpdatesClass, fn func(*tg.Message) bool) {
	switch ups := u.(type) {
	case *tg.Updates:
		for _, upd := range ups.Updates {
			switch m := upd.(type) {
			case *tg.UpdateNewMessage:
				if msg, ok := m.Message.(*tg.Message); ok && fn(msg) {
					return
				}
			case *tg.UpdateNewChannelMessage:
				if msg, ok := m.Message.(*tg.Message); ok && fn(msg) {
					return
				}
			}
		}
	case *tg.UpdateShort:
		if msg, ok := ups.Update.(*tg.UpdateNewMessage); ok {
			if m, ok := msg.Message.(*tg.Message); ok {
				fn(m)
			}
		}
	}
}

func (g *gotdClient) EditMessageText(ctx context.Context, p EditTextParams) (EditResult, error) {
	req := tg.MessagesEditMessageRequest{
		Peer:      inputPeerOf(p.ChatID),
		ID:        int(p.MessageID),
		NoWebpage: p.NoWebpage,
		Message:   p.Text,
	}
	if strings.EqualFold(p.ParseMode, "html") {
		b := entity.Builder{}
		if err := styling.Perform(&b, html.String(nil, p.Text)); err == nil {
			if text, entities := b.Complete(); text != "" || len(entities) > 0 {
				req.Message = text
				req.Entities = entities
			}
		}
	}
	if p.ReplyMarkup != nil {
		if rk := CoerceReplyMarkup(p.ReplyMarkup); rk != nil {
			req.SetReplyMarkup(rk)
		}
	}
	_, err := g.raw.MessagesEditMessage(ctx, &req)
	return EditResult{Ok: err == nil}, err
}

func (g *gotdClient) EditMessageReplyMarkup(ctx context.Context, p EditMarkupParams) (EditResult, error) {
	req := tg.MessagesEditMessageRequest{
		Peer: inputPeerOf(p.ChatID),
		ID:   int(p.MessageID),
	}
	if p.ReplyMarkup != nil {
		if rk := CoerceReplyMarkup(p.ReplyMarkup); rk != nil {
			req.SetReplyMarkup(rk)
		}
	}
	_, err := g.raw.MessagesEditMessage(ctx, &req)
	return EditResult{Ok: err == nil}, err
}

var chatActions = map[string]tg.SendMessageActionClass{
	"typing":          &tg.SendMessageTypingAction{},
	"upload_photo":    &tg.SendMessageUploadPhotoAction{},
	"record_video":    &tg.SendMessageRecordVideoAction{},
	"upload_video":    &tg.SendMessageUploadVideoAction{},
	"record_audio":    &tg.SendMessageRecordAudioAction{},
	"upload_audio":    &tg.SendMessageUploadAudioAction{},
	"upload_document": &tg.SendMessageUploadDocumentAction{},
	"find_location":   &tg.SendMessageGeoLocationAction{},
	"cancel":          &tg.SendMessageCancelAction{},
}

func (g *gotdClient) SendChatAction(ctx context.Context, chatID int64, action string) (EditResult, error) {
	a, ok := chatActions[strings.ToLower(action)]
	if !ok {
		a = &tg.SendMessageTypingAction{}
	}
	_, err := g.raw.MessagesSetTyping(ctx, &tg.MessagesSetTypingRequest{
		Peer:   inputPeerOf(chatID),
		Action: a,
	})
	return EditResult{Ok: err == nil}, err
}

func (g *gotdClient) DeleteMessages(ctx context.Context, chatID int64, ids []int64, revoke bool) (EditResult, error) {
	var err error
	if chatID > 0 {
		_, err = g.raw.MessagesDeleteMessages(ctx, &tg.MessagesDeleteMessagesRequest{ID: intIDs(ids), Revoke: revoke})
	} else {
		_, err = g.raw.ChannelsDeleteMessages(ctx, &tg.ChannelsDeleteMessagesRequest{
			Channel: &tg.InputChannel{ChannelID: -chatID},
			ID:      intIDs(ids),
		})
	}
	return EditResult{Ok: err == nil}, err
}

func intIDs(ids []int64) []int {
	out := make([]int, len(ids))
	for i, v := range ids {
		out[i] = int(v)
	}
	return out
}

func (g *gotdClient) ForwardMessages(ctx context.Context, p ForwardParams) ([]SentMessage, error) {
	resp, err := g.raw.MessagesForwardMessages(ctx, &tg.MessagesForwardMessagesRequest{
		FromPeer:   inputPeerOf(p.FromChatID),
		ToPeer:     inputPeerOf(p.ToChatID),
		ID:         intIDs(p.IDs),
		RandomID:   []int64{randomID()},
		DropAuthor: p.NoQuote,
	})
	if err != nil {
		return nil, err
	}
	var out []SentMessage
	walkMessages(resp, func(m *tg.Message) bool {
		out = append(out, SentMessage{ID: int64(m.ID), ChatID: p.ToChatID})
		return false
	})
	return out, nil
}

func (g *gotdClient) GetChat(ctx context.Context, chatID int64) (map[string]any, error) {
	peer := inputPeerOf(chatID)
	switch p := peer.(type) {
	case *tg.InputPeerUser:
		users, err := g.raw.UsersGetUsers(ctx, []tg.InputUserClass{&tg.InputUser{UserID: p.UserID}})
		if err != nil {
			return nil, err
		}
		if len(users) == 0 {
			return nil, fmt.Errorf("chat not found: %d", chatID)
		}
		if u, ok := users[0].(*tg.User); ok {
			return dict("_", "Chat", "id", u.ID, "type", "private", "username", u.Username,
				"first_name", u.FirstName, "last_name", u.LastName), nil
		}
	case *tg.InputPeerChat:
		full, err := g.raw.MessagesGetFullChat(ctx, p.ChatID)
		if err != nil {
			return nil, err
		}
		chatList := full.Chats
		if len(chatList) > 0 {
			if c, ok := chatList[0].(*tg.Chat); ok {
				return dict("_", "Chat", "id", c.ID, "type", "chat", "title", c.Title), nil
			}
		}
	case *tg.InputPeerChannel:
		resp, err := g.raw.ChannelsGetChannels(ctx, []tg.InputChannelClass{
			&tg.InputChannel{ChannelID: p.ChannelID},
		})
		if err != nil {
			return nil, err
		}
		var chatList []tg.ChatClass
		switch cc := resp.(type) {
		case *tg.MessagesChats:
			chatList = cc.Chats
		}
		if len(chatList) > 0 {
			if c, ok := chatList[0].(*tg.Channel); ok {
				return dict("_", "Chat", "id", c.ID, "type", "channel", "title", c.Title, "username", c.Username), nil
			}
		}
	}
	return nil, fmt.Errorf("cannot resolve chat %d", chatID)
}

func (g *gotdClient) ResolveUsername(ctx context.Context, username string) (int64, error) {
	resp, err := g.raw.ContactsResolveUsername(ctx, &tg.ContactsResolveUsernameRequest{
		Username: strings.TrimPrefix(username, "@"),
	})
	if err != nil {
		return 0, err
	}
	if len(resp.Users) > 0 {
		if u, ok := resp.Users[0].(*tg.User); ok {
			return u.ID, nil
		}
	}
	if len(resp.Chats) > 0 {
		switch c := resp.Chats[0].(type) {
		case *tg.Chat:
			return c.ID, nil
		case *tg.Channel:
			return c.ID, nil
		}
	}
	return 0, fmt.Errorf("username not found: %s", username)
}

func (g *gotdClient) AnswerCallbackQuery(ctx context.Context, id, text string, showAlert bool, url string, cacheTime int) (EditResult, error) {
	var qid int64
	_, _ = fmt.Sscanf(id, "%d", &qid)
	_, err := g.raw.MessagesSetBotCallbackAnswer(ctx, &tg.MessagesSetBotCallbackAnswerRequest{
		QueryID:   qid,
		Message:   text,
		Alert:     showAlert,
		URL:       url,
		CacheTime: cacheTime,
	})
	return EditResult{Ok: err == nil}, err
}

// UpdateState reports the tracked pts/qts (polled by PersistStates).
// The pts/qts ride gotd's own session state; the durable snapshot stays
// in mtgateway.db via the persist loop.
func (g *gotdClient) UpdateState() store.MTState {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	state, err := g.raw.UpdatesGetState(ctx)
	if err != nil {
		return store.MTState{}
	}
	return store.MTState{
		PTS: int64(state.Pts), QTS: int64(state.Qts),
		Date: int64(state.Date), Seq: int64(state.Seq),
	}
}

// randomID produces a client-side message nonce (counter + nanos mix).
var randomSrc = time.Now().UnixNano()

func randomID() int64 {
	randomSrc += 0x9E3779B9
	return randomSrc ^ time.Now().UnixNano()
}

var _ = tgerr.Is
