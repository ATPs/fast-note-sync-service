package websocket_router

import (
	"reflect"
	"testing"

	"github.com/haierkeys/fast-note-sync-service/internal/dto"
)

func TestRoundSlotKeys(t *testing.T) {
	queue := []dto.WSQueuedMessage{
		{Action: string(FileUpload), Data: dto.FileSyncUploadMessage{Path: "a.png", SessionID: "s"}},
		{Action: string(FileSyncUpdate), Data: &dto.FileSyncModifyMessage{Path: "b.png"}},
		{Action: string(FileSyncUpdate), Data: dto.FileSyncModifyMessage{Path: "c.pdf"}},
		{Action: string(NoteSyncNeedPush), Data: dto.NoteSyncNeedPushMessage{Path: "n.md"}},
		{Action: string(FileSyncDelete), Data: dto.FileSyncDeleteMessage{Path: "gone.png"}},
		{Action: string(FileSyncMtime), Data: dto.FileSyncMtimeMessage{Path: "m.png"}},
		{Action: string(FileUpload), Data: "unexpected type"},
	}
	got := roundSlotKeys(queue)
	want := []string{"a.png", "download_b.png", "download_c.pdf", "n.md"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("roundSlotKeys = %v, want %v", got, want)
	}
	if keys := roundSlotKeys(nil); len(keys) != 0 {
		t.Fatalf("empty queue must give no keys, got %v", keys)
	}
}
