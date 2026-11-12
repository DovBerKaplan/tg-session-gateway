package mtproto

// gotd updates.Manager wiring: gap recovery (getDifference /
// getChannelDifference) runs inside the gateway — the connection lives
// here, so the "no lost updates" guarantee must too. The manager's
// StateStorage persists into mtgateway.db (the same pts/qts/date/seq
// columns RestoreAll already reads, plus per-channel pts), surviving
// restarts.

import (
	"context"

	"github.com/gotd/td/telegram"
	"github.com/gotd/td/telegram/updates"

	"github.com/DovBerKaplan/tg-session-gateway/internal/store"
)

// gapStateStorage adapts store.MT to updates.StateStorage. userID is
// ignored — one account per alias (the session IS the user).
type gapStateStorage struct {
	st    *store.MT
	alias string
}

// compile-time check.
var _ updates.StateStorage = (*gapStateStorage)(nil)

func (s *gapStateStorage) GetState(_ context.Context, _ int64) (updates.State, bool, error) {
	st := s.st.GetState(s.alias)
	if st.PTS == 0 {
		return updates.State{}, false, nil
	}
	return updates.State{
		Pts:  int(st.PTS),
		Qts:  int(st.QTS),
		Date: int(st.Date),
		Seq:  int(st.Seq),
	}, true, nil
}

func (s *gapStateStorage) SetState(_ context.Context, _ int64, state updates.State) error {
	return s.st.SaveState(s.alias, store.MTState{
		PTS: int64(state.Pts), QTS: int64(state.Qts),
		Date: int64(state.Date), Seq: int64(state.Seq),
	})
}

// partial state writes are SINGLE UPDATE statements — the manager ticks
// pts/qts on every update batch, and a SELECT+UPDATE round-trip per
// tick doubled the write pressure on the store's single connection
// (the production choke).
func (s *gapStateStorage) SetPts(_ context.Context, _ int64, pts int) error {
	return s.st.UpdateSessionColumn(s.alias, "pts", int64(pts))
}

func (s *gapStateStorage) SetQts(_ context.Context, _ int64, qts int) error {
	return s.st.UpdateSessionColumn(s.alias, "qts", int64(qts))
}

func (s *gapStateStorage) SetDate(_ context.Context, _ int64, date int) error {
	return s.st.UpdateSessionColumn(s.alias, "date", int64(date))
}

func (s *gapStateStorage) SetSeq(_ context.Context, _ int64, seq int) error {
	return s.st.UpdateSessionColumn(s.alias, "seq", int64(seq))
}

func (s *gapStateStorage) SetDateSeq(_ context.Context, _ int64, date, seq int) error {
	return s.st.UpdateSessionColumns(s.alias, map[string]int64{"date": int64(date), "seq": int64(seq)})
}

func (s *gapStateStorage) GetChannelPts(_ context.Context, _ int64, channelID int64) (int, bool, error) {
	pts, found := s.st.GetChannelPts(s.alias, channelID)
	return pts, found, nil
}

func (s *gapStateStorage) SetChannelPts(_ context.Context, _ int64, channelID int64, pts int) error {
	return s.st.SetChannelPts(s.alias, channelID, pts)
}

func (s *gapStateStorage) ForEachChannels(_ context.Context, _ int64, f func(ctx context.Context, channelID int64, pts int) error) error {
	return s.st.ForEachChannelPts(s.alias, func(channelID int64, pts int) error {
		return f(context.Background(), channelID, pts)
	})
}

// peerStorage serves gotd's update manager the shared access-hash
// cache (the same peers table the facade reads) — channel gap recovery
// needs channel access hashes, and they resolve once per session now,
// not per client.
type peerStorage struct {
	st    *store.MT
	alias string
}

var (
	_ updates.ChannelAccessHasher = (*peerStorage)(nil)
	_ updates.UserAccessHasher    = (*peerStorage)(nil)
)

func (p *peerStorage) SetChannelAccessHash(_ context.Context, _, channelID, accessHash int64) error {
	return p.st.UpsertPeer(p.alias, store.PeerEntry{ID: channelID, Kind: "channel", AccessHash: accessHash})
}

func (p *peerStorage) GetChannelAccessHash(_ context.Context, _, channelID int64) (int64, bool, error) {
	e, found := p.st.LookupPeer(p.alias, channelID)
	if !found || e.AccessHash == 0 {
		return 0, false, nil
	}
	return e.AccessHash, true, nil
}

func (p *peerStorage) SetUserAccessHash(_ context.Context, _, targetUserID, accessHash int64) error {
	return p.st.UpsertPeer(p.alias, store.PeerEntry{ID: targetUserID, Kind: "user", AccessHash: accessHash})
}

func (p *peerStorage) GetUserAccessHash(_ context.Context, _, targetUserID int64) (int64, bool, error) {
	e, found := p.st.LookupPeer(p.alias, targetUserID)
	if !found || e.AccessHash == 0 {
		return 0, false, nil
	}
	return e.AccessHash, true, nil
}

// newUpdatesManager builds the gap-recovering handler that wraps the
// session's update fan-out; state AND access hashes persist in the
// shared tables.
func newUpdatesManager(st *store.MT, alias string, handler telegram.UpdateHandler) *updates.Manager {
	peers := &peerStorage{st: st, alias: alias}
	return updates.New(updates.Config{
		Handler:        handler,
		Storage:        &gapStateStorage{st: st, alias: alias},
		AccessHasher:   peers,
		UserAccessHasher: peers,
	})
}
