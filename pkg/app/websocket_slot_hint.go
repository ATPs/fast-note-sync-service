package app

import (
	"sync"
	"time"
)

// Slot-release hint support.
// 并发槽位释放提示支持。
//
// Obsidian plugin <= 2.4.0 can leak its upload concurrency slots (keyed by path) after a reconnect
// or when an ack carries a stale sync context, which stalls sync while the server is idle. The
// router works around this by sending no-op acks that make the plugin release the slot for a key.
// The state below tells the router when such hints are useful.
// Obsidian 插件 <= 2.4.0 在重连或 ack 携带过期同步上下文时会泄漏按路径记录的上传并发槽位，导致服务端空闲
// 而同步卡死。路由层通过发送无副作用的 ack 让插件释放对应槽位，以下状态用于判断何时需要发送这类提示。

const (
	// slotHintReconnectWindow how long after a connection of the same user+vault closes a new round still gets hints
	// slotHintReconnectWindow 同一用户+仓库连接关闭后，多长时间内的新同步轮次仍需发送提示
	slotHintReconnectWindow = 30 * time.Minute
	// uploadTombstoneTTL how long a completed upload session is remembered for late chunks
	// uploadTombstoneTTL 已完成上传会话为迟到分片保留的时长
	uploadTombstoneTTL = time.Hour
	// maxStaleRoundContexts upper bound of earlier round contexts kept per connection
	// maxStaleRoundContexts 每个连接保留的历史同步轮次上下文数量上限
	maxStaleRoundContexts = 64
)

// UploadTombstone remembers a completed upload session so late chunks can be answered
// UploadTombstone 记录已完成的上传会话，以便应答迟到的分片
type UploadTombstone struct {
	Vault    string
	Path     string
	PathHash string
	expires  time.Time
	hinted   bool
}

// slotHintState per-connection sync round tracking
// slotHintState 每个连接的同步轮次跟踪
type slotHintState struct {
	mu        sync.Mutex
	vault     string
	latestCtx string
	staleCtx  map[string]struct{}
	rounds    int
}

// slotHintServerState server-wide state shared by all connections
// slotHintServerState 所有连接共享的服务端状态
type slotHintServerState struct {
	mu          sync.Mutex
	vaultClosed map[string]time.Time                   // uid|vault -> last close time
	tombstones  map[string]map[string]*UploadTombstone // uid -> sessionID -> tombstone
	tombCount   int
	pruneAt     int
}

// BeginSyncRound records the context of a sync round request (FileSync/NoteSync/...) on this connection.
// It returns true when slot-release hints are useful for this round: this connection already ran an
// earlier round, or a connection of the same user and vault closed recently (the plugin reconnected).
// BeginSyncRound 记录本连接上同步轮次请求的上下文。当本连接已执行过更早的轮次，或同一用户+仓库的连接
// 最近关闭过（插件重连）时返回 true，表示本轮需要发送槽位释放提示。
func (c *WebsocketClient) BeginSyncRound(vault, syncCtx string) bool {
	if syncCtx == "" {
		return false
	}
	s := &c.slotHint
	s.mu.Lock()
	if syncCtx != s.latestCtx {
		if s.latestCtx != "" {
			if s.staleCtx == nil || len(s.staleCtx) >= maxStaleRoundContexts {
				s.staleCtx = make(map[string]struct{})
			}
			s.staleCtx[s.latestCtx] = struct{}{}
		}
		delete(s.staleCtx, syncCtx)
		s.latestCtx = syncCtx
		s.rounds++
	}
	s.vault = vault
	earlierRound := s.rounds > 1
	s.mu.Unlock()

	if earlierRound {
		return true
	}
	if c.User == nil || c.Server == nil {
		return false
	}
	return c.Server.vaultClosedRecently(c.User.ID, vault)
}

// StaleRoundContext reports whether syncCtx belongs to an earlier sync round of this connection,
// and returns the latest round context to use instead.
// StaleRoundContext 判断 syncCtx 是否属于本连接更早的同步轮次，并返回应改用的最新轮次上下文。
func (c *WebsocketClient) StaleRoundContext(syncCtx string) (string, bool) {
	s := &c.slotHint
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.staleCtx[syncCtx]; ok && s.latestCtx != "" {
		return s.latestCtx, true
	}
	return "", false
}

// LatestRoundContext returns the context of the most recent sync round on this connection, or fallback
// LatestRoundContext 返回本连接最近一次同步轮次的上下文，没有时返回 fallback
func (c *WebsocketClient) LatestRoundContext(fallback string) string {
	s := &c.slotHint
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.latestCtx != "" {
		return s.latestCtx
	}
	return fallback
}

// markVaultClosed remembers that a connection which ran a sync round for a vault has closed
// markVaultClosed 记录某个执行过同步轮次的仓库连接已关闭
func (w *WebsocketServer) markVaultClosed(c *WebsocketClient) {
	if c.User == nil {
		return
	}
	c.slotHint.mu.Lock()
	vault := c.slotHint.vault
	c.slotHint.mu.Unlock()
	if vault == "" {
		return
	}
	now := time.Now()
	h := &w.slotHint
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.vaultClosed == nil {
		h.vaultClosed = make(map[string]time.Time)
	}
	for k, t := range h.vaultClosed {
		if now.Sub(t) > slotHintReconnectWindow {
			delete(h.vaultClosed, k)
		}
	}
	h.vaultClosed[c.User.ID+"|"+vault] = now
}

func (w *WebsocketServer) vaultClosedRecently(uid, vault string) bool {
	h := &w.slotHint
	h.mu.Lock()
	defer h.mu.Unlock()
	t, ok := h.vaultClosed[uid+"|"+vault]
	return ok && time.Since(t) <= slotHintReconnectWindow
}

// AddUploadTombstone remembers a completed upload session for uploadTombstoneTTL
// AddUploadTombstone 在 uploadTombstoneTTL 内记住已完成的上传会话
func (w *WebsocketServer) AddUploadTombstone(uid, sessionID string, t UploadTombstone) {
	now := time.Now()
	t.expires = now.Add(uploadTombstoneTTL)
	t.hinted = false
	h := &w.slotHint
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.tombstones == nil {
		h.tombstones = make(map[string]map[string]*UploadTombstone)
	}
	// Prune expired entries once the store grows, keeping the cost amortized
	// 存储增长后再清理过期条目，摊薄清理成本
	if h.tombCount >= max(h.pruneAt, 1024) {
		h.tombCount = 0
		for u, m := range h.tombstones {
			for id, ts := range m {
				if now.After(ts.expires) {
					delete(m, id)
				}
			}
			if len(m) == 0 {
				delete(h.tombstones, u)
			}
			h.tombCount += len(m)
		}
		h.pruneAt = 2 * h.tombCount
	}
	m := h.tombstones[uid]
	if m == nil {
		m = make(map[string]*UploadTombstone)
		h.tombstones[uid] = m
	}
	if _, exists := m[sessionID]; !exists {
		h.tombCount++
	}
	m[sessionID] = &t
}

// TakeUploadTombstone looks up a completed upload session. found reports whether the session completed
// recently; first is true only for the first lookup, so the caller hints the client once per session.
// TakeUploadTombstone 查询已完成的上传会话。found 表示该会话最近已完成；first 仅在首次查询时为 true，
// 调用方据此每个会话只提示客户端一次。
func (w *WebsocketServer) TakeUploadTombstone(uid, sessionID string) (t UploadTombstone, found bool, first bool) {
	h := &w.slotHint
	h.mu.Lock()
	defer h.mu.Unlock()
	ts, ok := h.tombstones[uid][sessionID]
	if !ok || time.Now().After(ts.expires) {
		return UploadTombstone{}, false, false
	}
	first = !ts.hinted
	ts.hinted = true
	return *ts, true, first
}
