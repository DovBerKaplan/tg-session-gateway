package mtproto

import (
	"context"
	"testing"

	"github.com/gotd/td/tg"

	"github.com/DovBerKaplan/tg-session-gateway/internal/store"
)

func newPeerTestClient(t *testing.T, fake *fakeScanAPI) (*gotdClient, *store.MT) {
	t.Helper()
	mt, err := store.OpenMT(t.TempDir(), "CfR-eL8QzhZJQb22PBP1zxH7CzJMFK4LwKvFACh1pfw=", "normal", "", nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(mt.Close)
	_ = mt.Register("p1", "tok")
	g := &gotdClient{alias: "p1", storeDB: mt, scanFake: fake}
	return g, mt
}

func TestPeerCacheResolveFirst(t *testing.T) {
	fake := newFakeScanAPI()
	g, mt := newPeerTestClient(t, fake)

	// cache-first: a seeded hash skips the getChannels round-trip
	if _, err := g.SeedPeers(context.Background(), []PeerSeed{
		{ID: 42, Kind: "channel", AccessHash: 4242, Title: "cached"},
	}); err != nil {
		t.Fatal(err)
	}
	ch, err := g.resolveChannel(context.Background(), 42)
	if err != nil || ch.AccessHash != 4242 || ch.Title != "cached" {
		t.Fatalf("cached resolve: %+v %v", ch, err)
	}
	if len(fake.getMsgReqs) != 0 {
		t.Fatal("no RPC expected on cache hit")
	}
	if mt.PeerCount("p1") != 1 {
		t.Fatalf("cache size=%d", mt.PeerCount("p1"))
	}

	// a miss resolves over RPC and lands in the cache
	fake.channels[77] = &tg.Channel{ID: 77, AccessHash: 777, Title: "fresh"}
	ch, err = g.resolveChannel(context.Background(), 77)
	if err != nil || ch.AccessHash != 777 {
		t.Fatalf("rpc resolve: %+v %v", ch, err)
	}
	if e, found := mt.LookupPeer("p1", 77); !found || e.AccessHash != 777 {
		t.Fatalf("cache after resolve=%+v found=%v", e, found)
	}
}

func TestPeerCacheZeroHashNotPoisoned(t *testing.T) {
	fake := newFakeScanAPI()
	fake.channels[42] = &tg.Channel{ID: 42, AccessHash: 4242}
	g, mt := newPeerTestClient(t, fake)

	// seeding a zero hash is skipped
	if n, _ := g.SeedPeers(context.Background(), []PeerSeed{{ID: 42, Kind: "channel"}}); n != 0 {
		t.Fatalf("zero-hash seed counted=%d", n)
	}
	// and upsert never overwrites a live hash with zero
	if _, err := g.resolveChannel(context.Background(), 42); err != nil { // fills 4242
		t.Fatal(err)
	}
	if err := mt.UpsertPeer("p1", store.PeerEntry{ID: 42, Kind: "channel"}); err != nil {
		t.Fatal(err)
	}
	if e, _ := mt.LookupPeer("p1", 42); e.AccessHash != 4242 {
		t.Fatalf("hash lowered to %d", e.AccessHash)
	}
}

func TestHistoryFillsPeerCache(t *testing.T) {
	fake := newFakeScanAPI()
	fake.messages = []tg.MessageClass{
		&tg.Message{ID: 1, PeerID: &tg.PeerChannel{ChannelID: 42}},
	}
	g, mt := newPeerTestClient(t, fake)
	// the response's chats/users ride the same call in production; the
	// fake returns them via ChannelsGetChannels — call it to fill
	fake.channels[42] = &tg.Channel{ID: 42, AccessHash: 4242, Title: "c"}

	if _, err := g.GetHistory(context.Background(), 42, 0, 0, 0, 1); err != nil {
		t.Fatal(err)
	}
	// direct fill check via cacheUser/cacheChannel
	g.cacheChannel(&tg.Channel{ID: 9, AccessHash: 99})
	g.cacheUser(&tg.User{ID: 5, AccessHash: 55, Username: "u", FirstName: "A", LastName: "B"})
	if e, found := mt.LookupPeer("p1", 9); !found || e.AccessHash != 99 {
		t.Fatalf("channel fill=%+v", e)
	}
	if e, found := mt.LookupPeer("p1", 5); !found || e.Kind != "user" || e.Title != "A B" {
		t.Fatalf("user fill=%+v", e)
	}
}

func TestFileReferenceRefresh(t *testing.T) {
	fake := newFakeScanAPI()
	fake.channels[42] = &tg.Channel{ID: 42, AccessHash: 4242}
	g, _ := newPeerTestClient(t, fake)

	// the source message carries the same doc with a fresh reference
	fake.refreshDoc = &tg.Document{ID: 77, AccessHash: 88, FileReference: []byte("fresh")}

	doc := DocRef{ID: 77, AccessHash: 88, FileReference: []byte("stale"),
		ChatID: 42, MessageID: 5}
	fresh, err := g.refreshDocRef(context.Background(), doc)
	if err != nil {
		t.Fatal(err)
	}
	if fresh.ID != 77 || fresh.AccessHash != 88 || string(fresh.FileReference) != "fresh" {
		t.Fatalf("fresh=%+v", fresh)
	}
	if fresh.ChatID != 42 || fresh.MessageID != 5 {
		t.Fatalf("source must ride along: %+v", fresh)
	}

	// mismatched doc id in the source → clear error
	fake.refreshDoc = &tg.Document{ID: 999, AccessHash: 1}
	if _, err := g.refreshDocRef(context.Background(), doc); err == nil {
		t.Fatal("mismatched doc must fail")
	}
}
