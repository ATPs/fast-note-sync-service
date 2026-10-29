package websocket_router

import (
	"github.com/haierkeys/fast-note-sync-service/internal/dto"
	pkgapp "github.com/haierkeys/fast-note-sync-service/pkg/app"
	"github.com/haierkeys/fast-note-sync-service/pkg/code"
)

// Slot-release hints for Obsidian plugin <= 2.4.0.
// 面向 Obsidian 插件 <= 2.4.0 的并发槽位释放提示。
//
// The plugin's concurrency limiter frees a slot (keyed by path, or "download_"+path for downloads) only
// when an ack for that key arrives under the active sync context. After a reconnect, or when an ack carries
// an earlier round's context, slots leak and sync stalls once all of them are held. A FileDeleteAck with
// lastTime 0 for a path that has no pending delete makes the plugin release the slot for that key and do
// nothing else, so the server sends one for every key that may have leaked.
// 插件的并发限制器只在活跃同步上下文下收到对应 ack 时才释放槽位（键为路径，下载为 "download_"+路径）。
// 重连后或 ack 携带更早轮次的上下文时槽位会泄漏，全部占满后同步卡死。对没有待确认删除的路径发送 lastTime 为 0
// 的 FileDeleteAck，插件只会释放该键的槽位而无其他副作用，因此服务端为可能泄漏的键发送此提示。

// slotKeyDownloadPrefix plugin slot key prefix for file downloads
// slotKeyDownloadPrefix 插件文件下载槽位键前缀
const slotKeyDownloadPrefix = "download_"

// sendSlotReleaseHint sends one no-op FileDeleteAck that releases the plugin slot held under key
// sendSlotReleaseHint 发送一条无副作用的 FileDeleteAck，释放插件以 key 持有的槽位
func sendSlotReleaseHint(c *pkgapp.WebsocketClient, vault, syncCtx, key string) {
	if key == "" {
		return
	}
	c.ToResponse(code.Success.WithData(dto.FileDeleteAckMessage{
		Path: key,
	}).WithVault(vault).WithContext(syncCtx), string(FileDeleteAck))
}

// sendRoundSlotReleaseHints starts tracking a sync round and, when the plugin may hold leaked slots
// (reconnect or an earlier round on this connection), releases the slot of every key this round will
// ask the client to upload, push or download. Must run before the round's paged messages are sent.
// sendRoundSlotReleaseHints 开始跟踪同步轮次，并在插件可能持有泄漏槽位时（重连或本连接已有更早轮次），
// 释放本轮将要求客户端上传、推送或下载的每个键的槽位。必须在本轮分页消息发送之前调用。
func sendRoundSlotReleaseHints(c *pkgapp.WebsocketClient, vault, syncCtx string, queue []dto.WSQueuedMessage) {
	if !c.BeginSyncRound(vault, syncCtx) {
		return
	}
	for _, key := range roundSlotKeys(queue) {
		sendSlotReleaseHint(c, vault, syncCtx, key)
	}
}

// roundSlotKeys returns the plugin slot keys of the upload, push and download messages in a round's queue
// roundSlotKeys 返回同步轮次消息队列中上传、推送与下载消息对应的插件槽位键
func roundSlotKeys(queue []dto.WSQueuedMessage) []string {
	var keys []string
	for _, m := range queue {
		switch m.Action {
		case string(FileUpload):
			switch d := m.Data.(type) {
			case dto.FileSyncUploadMessage:
				keys = append(keys, d.Path)
			case *dto.FileSyncUploadMessage:
				keys = append(keys, d.Path)
			}
		case string(FileSyncUpdate):
			switch d := m.Data.(type) {
			case dto.FileSyncModifyMessage:
				keys = append(keys, slotKeyDownloadPrefix+d.Path)
			case *dto.FileSyncModifyMessage:
				keys = append(keys, slotKeyDownloadPrefix+d.Path)
			}
		case string(NoteSyncNeedPush):
			switch d := m.Data.(type) {
			case dto.NoteSyncNeedPushMessage:
				keys = append(keys, d.Path)
			case *dto.NoteSyncNeedPushMessage:
				keys = append(keys, d.Path)
			}
		}
	}
	return keys
}

// sendStaleAckSlotReleaseHint is called next to an ack: if the ack carries an earlier round's context the
// plugin discards it, so release the slot for path under the latest round context instead.
// sendStaleAckSlotReleaseHint 在发送 ack 时调用：若 ack 携带更早轮次的上下文会被插件丢弃，
// 因此改用最新轮次上下文释放该路径的槽位。
func sendStaleAckSlotReleaseHint(c *pkgapp.WebsocketClient, vault, ackCtx, path string) {
	if latest, stale := c.StaleRoundContext(ackCtx); stale {
		sendSlotReleaseHint(c, vault, latest, path)
	}
}
