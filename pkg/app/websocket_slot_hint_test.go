package app

import (
	"testing"
	"time"
)

func newSlotHintClient(w *WebsocketServer, uid string) *WebsocketClient {
	c := &WebsocketClient{Server: w}
	if uid != "" {
		c.User = &UserEntity{}
		c.User.ID = uid
	}
	return c
}

func TestBeginSyncRound_FirstRoundNoHints(t *testing.T) {
	w := &WebsocketServer{}
	c := newSlotHintClient(w, "1")
	if c.BeginSyncRound("v", "ctx1") {
		t.Fatal("first round on a fresh connection must not need hints")
	}
	// The same round seen again (NoteSync after FileSync) stays a first round
	// 同一轮次再次出现（FileSync 之后的 NoteSync）仍属于首轮
	if c.BeginSyncRound("v", "ctx1") {
		t.Fatal("repeat of the first round must not need hints")
	}
	if c.BeginSyncRound("v", "") {
		t.Fatal("empty context must not need hints")
	}
}

func TestBeginSyncRound_LaterRoundNeedsHints(t *testing.T) {
	c := newSlotHintClient(&WebsocketServer{}, "1")
	c.BeginSyncRound("v", "ctx1")
	if !c.BeginSyncRound("v", "ctx2") {
		t.Fatal("second round on the same connection must need hints")
	}
	if !c.BeginSyncRound("v", "ctx2") {
		t.Fatal("repeat of a later round must still need hints")
	}
}

func TestBeginSyncRound_ReconnectNeedsHints(t *testing.T) {
	w := &WebsocketServer{}
	old := newSlotHintClient(w, "1")
	old.BeginSyncRound("v", "ctx1")
	w.markVaultClosed(old)

	if !newSlotHintClient(w, "1").BeginSyncRound("v", "ctx2") {
		t.Fatal("first round after a recent close of the same user+vault must need hints")
	}
	if newSlotHintClient(w, "1").BeginSyncRound("other", "ctx3") {
		t.Fatal("a different vault must not need hints")
	}
	if newSlotHintClient(w, "2").BeginSyncRound("v", "ctx4") {
		t.Fatal("a different user must not need hints")
	}

	// Outside the reconnect window the close no longer counts
	// 超出重连窗口后关闭记录不再生效
	w.slotHint.mu.Lock()
	w.slotHint.vaultClosed["1|v"] = time.Now().Add(-slotHintReconnectWindow - time.Minute)
	w.slotHint.mu.Unlock()
	if newSlotHintClient(w, "1").BeginSyncRound("v", "ctx5") {
		t.Fatal("close older than the reconnect window must not need hints")
	}
}

func TestMarkVaultClosed_IgnoresConnectionsWithoutRound(t *testing.T) {
	w := &WebsocketServer{}
	w.markVaultClosed(newSlotHintClient(w, "1"))
	w.markVaultClosed(newSlotHintClient(w, ""))
	if len(w.slotHint.vaultClosed) != 0 {
		t.Fatalf("expected no close records, got %v", w.slotHint.vaultClosed)
	}
}

func TestStaleRoundContext(t *testing.T) {
	c := newSlotHintClient(&WebsocketServer{}, "1")
	if _, stale := c.StaleRoundContext("ctx1"); stale {
		t.Fatal("unknown context must not be stale")
	}
	c.BeginSyncRound("v", "ctx1")
	if _, stale := c.StaleRoundContext("ctx1"); stale {
		t.Fatal("latest round context must not be stale")
	}
	c.BeginSyncRound("v", "ctx2")
	latest, stale := c.StaleRoundContext("ctx1")
	if !stale || latest != "ctx2" {
		t.Fatalf("ctx1 must be stale with latest ctx2, got %q %v", latest, stale)
	}
	if _, stale := c.StaleRoundContext("user-edit-ctx"); stale {
		t.Fatal("context never seen as a round must not be stale")
	}
	// Returning to an earlier context makes it latest again
	// 回到更早的上下文时它重新成为最新
	c.BeginSyncRound("v", "ctx1")
	if latest, stale := c.StaleRoundContext("ctx2"); !stale || latest != "ctx1" {
		t.Fatalf("ctx2 must be stale with latest ctx1, got %q %v", latest, stale)
	}
	if _, stale := c.StaleRoundContext("ctx1"); stale {
		t.Fatal("ctx1 is latest again and must not be stale")
	}
	if got := c.LatestRoundContext("fb"); got != "ctx1" {
		t.Fatalf("LatestRoundContext = %q, want ctx1", got)
	}
	if got := newSlotHintClient(nil, "").LatestRoundContext("fb"); got != "fb" {
		t.Fatalf("LatestRoundContext without rounds = %q, want fb", got)
	}
}

func TestStaleRoundContext_Bounded(t *testing.T) {
	c := newSlotHintClient(&WebsocketServer{}, "1")
	for i := 0; i < 3*maxStaleRoundContexts; i++ {
		c.BeginSyncRound("v", time.Duration(i).String())
	}
	if n := len(c.slotHint.staleCtx); n > maxStaleRoundContexts {
		t.Fatalf("stale contexts not bounded: %d", n)
	}
}

func TestUploadTombstone(t *testing.T) {
	w := &WebsocketServer{}
	if _, found, _ := w.TakeUploadTombstone("1", "s1"); found {
		t.Fatal("unknown session must not be found")
	}
	w.AddUploadTombstone("1", "s1", UploadTombstone{Vault: "v", Path: "a.png", PathHash: "h"})

	ts, found, first := w.TakeUploadTombstone("1", "s1")
	if !found || !first || ts.Path != "a.png" || ts.Vault != "v" {
		t.Fatalf("first lookup: found=%v first=%v ts=%+v", found, first, ts)
	}
	if _, found, first := w.TakeUploadTombstone("1", "s1"); !found || first {
		t.Fatalf("second lookup must be found but not first: found=%v first=%v", found, first)
	}
	if _, found, _ := w.TakeUploadTombstone("2", "s1"); found {
		t.Fatal("tombstones must be per user")
	}

	// Expired tombstones are not returned
	// 过期的墓碑记录不再返回
	w.slotHint.mu.Lock()
	w.slotHint.tombstones["1"]["s1"].expires = time.Now().Add(-time.Second)
	w.slotHint.mu.Unlock()
	if _, found, _ := w.TakeUploadTombstone("1", "s1"); found {
		t.Fatal("expired tombstone must not be found")
	}
}

func TestUploadTombstone_PrunesExpired(t *testing.T) {
	w := &WebsocketServer{}
	for i := 0; i < 1024; i++ {
		w.AddUploadTombstone("1", time.Duration(i).String(), UploadTombstone{Path: "p"})
	}
	w.slotHint.mu.Lock()
	for _, ts := range w.slotHint.tombstones["1"] {
		ts.expires = time.Now().Add(-time.Second)
	}
	w.slotHint.mu.Unlock()
	w.AddUploadTombstone("1", "fresh", UploadTombstone{Path: "p"})
	if n := len(w.slotHint.tombstones["1"]); n != 1 {
		t.Fatalf("expired tombstones not pruned, %d left", n)
	}
	if _, found, _ := w.TakeUploadTombstone("1", "fresh"); !found {
		t.Fatal("fresh tombstone must survive pruning")
	}
}
