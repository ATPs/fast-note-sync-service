package app

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/haierkeys/fast-note-sync-service/pkg/code"
	"github.com/lxzan/gws"
	"go.uber.org/zap"
)

const e2eSecret = "e2e-test-secret"

// fakeApp is a minimal AppContainer for end-to-end WebSocket tests.
type fakeApp struct{}

func (fakeApp) Logger() *zap.Logger { return zap.NewNop() }
func (fakeApp) SubmitTask(ctx context.Context, task func(context.Context) error) error {
	return task(ctx)
}
func (fakeApp) SubmitTaskAsync(ctx context.Context, task func(context.Context) error) error {
	go func() { _ = task(ctx) }()
	return nil
}
func (fakeApp) Version() VersionInfo                 { return VersionInfo{Version: "e2e"} }
func (fakeApp) CheckVersion(string) CheckVersionInfo { return CheckVersionInfo{} }
func (fakeApp) Validator() ValidatorInterface        { return nil }
func (fakeApp) IsReturnSuccess() bool                { return true }
func (fakeApp) GetAuthTokenKey() string              { return e2eSecret }
func (fakeApp) IsProductionMode() bool               { return false }
func (fakeApp) GetTokenService() any                 { return nil }
func (fakeApp) SyncChunkNums() (int, int)            { return 0, 0 }
func (fakeApp) PipelineWindows() (int, int)          { return 0, 0 }

// e2eEnv is a running WS server plus the observations made by its handlers.
type e2eEnv struct {
	srv    *httptest.Server
	engine *gin.Engine

	mu             sync.Mutex
	seenClientType []string
	binaryCalls    int
	binaryUserIDs  []string
	binaryPayloads [][]byte
	binaryCh       chan struct{} // signalled on every binary handler call
}

func (e *e2eEnv) snapshot() (clientTypes []string, calls int, userIDs []string, payloads [][]byte) {
	e.mu.Lock()
	defer e.mu.Unlock()
	return append([]string(nil), e.seenClientType...), e.binaryCalls,
		append([]string(nil), e.binaryUserIDs...), append([][]byte(nil), e.binaryPayloads...)
}

var (
	sharedEnvOnce sync.Once
	sharedEnv     *e2eEnv
)

// newE2EEnv returns the process-wide test server with its observations reset. The server is
// created only once because NewWebsocketServer writes package-level globals (logger, production
// flag) that connection goroutines of earlier tests may still be reading.
func newE2EEnv(t *testing.T) *e2eEnv {
	t.Helper()
	sharedEnvOnce.Do(func() { sharedEnv = buildE2EEnv() })
	env := sharedEnv
	env.mu.Lock()
	env.seenClientType = nil
	env.binaryCalls = 0
	env.binaryUserIDs = nil
	env.binaryPayloads = nil
	env.binaryCh = make(chan struct{}, 64)
	env.mu.Unlock()
	return env
}

func buildE2EEnv() *e2eEnv {
	gin.SetMode(gin.TestMode)

	env := &e2eEnv{}

	wss := NewWebsocketServer(WSConfig{
		GWSOption: gws.ServerOption{
			// Like production: messages are processed in parallel, so a binary frame can be
			// dispatched while Authorization is still running.
			ParallelEnabled: true,
			ParallelGolimit: 8,
		},
		PingInterval: WSPingInterval, // unit is seconds (multiplied inside PingLoop)
		PingWait:     WSPingWait,
	}, fakeApp{})

	// Mimics a token whose scope is "c:ObsidianPlugin": rejects any other client type.
	wss.UseTokenVerify(func(ctx context.Context, uid, tokenID int64, nonce string, reqClientType, reqClientName, reqClientVersion, reqUserAgent, reqIP string) (string, string, error) {
		env.mu.Lock()
		env.seenClientType = append(env.seenClientType, reqClientType)
		env.mu.Unlock()
		if reqClientType != "ObsidianPlugin" {
			return "", "", code.ErrorAuthTokenClientRestricted.WithDetails("Permission denied: Handshake")
		}
		return "", "", nil
	})
	wss.UseUserVerify(func(c *WebsocketClient, uid int64) (*UserSelectEntity, error) {
		return &UserSelectEntity{UID: uid, Nickname: "tester"}, nil
	})
	// Like the real FileUploadChunkBinary: dereferences c.User.
	wss.UseBinary("00", func(c *WebsocketClient, data []byte) {
		id := c.User.ID
		env.mu.Lock()
		env.binaryCalls++
		env.binaryUserIDs = append(env.binaryUserIDs, id)
		env.binaryPayloads = append(env.binaryPayloads, data)
		ch := env.binaryCh
		env.mu.Unlock()
		ch <- struct{}{}
	})

	r := gin.New()
	// Same as the lang middleware: puts a "trans" value on the context.
	r.Use(func(c *gin.Context) { c.Set("lang", "en"); c.Next() })
	api := r.Group("/api")
	api.GET("/user/sync", wss.Run())
	api.GET("/dummy", func(c *gin.Context) { c.String(http.StatusOK, "ok") })

	env.engine = r
	env.srv = httptest.NewServer(r)
	return env
}

func e2eToken(t *testing.T, uid int64) string {
	t.Helper()
	tok, err := NewTokenManager(TokenConfig{SecretKey: e2eSecret}).Generate(uid, "tester", "", 7, "nonce")
	if err != nil {
		t.Fatalf("generate token: %v", err)
	}
	return tok
}

// wsClient is a gws test client collecting received frames.
type wsClient struct {
	conn   *gws.Conn
	text   chan string
	closed chan struct{}
}

func (w *wsClient) OnOpen(*gws.Conn)             {}
func (w *wsClient) OnClose(*gws.Conn, error)     { close(w.closed) }
func (w *wsClient) OnPing(c *gws.Conn, p []byte) { _ = c.WritePong(p) }
func (w *wsClient) OnPong(*gws.Conn, []byte)     {}
func (w *wsClient) OnMessage(c *gws.Conn, m *gws.Message) {
	defer m.Close()
	w.text <- m.Data.String()
}

func (e *e2eEnv) dial(t *testing.T, query string) *wsClient {
	t.Helper()
	cl := &wsClient{text: make(chan string, 32), closed: make(chan struct{})}
	addr := "ws" + strings.TrimPrefix(e.srv.URL, "http") + "/api/user/sync"
	if query != "" {
		addr += "?" + query
	}
	conn, _, err := gws.NewClient(cl, &gws.ClientOption{Addr: addr})
	if err != nil {
		t.Fatalf("dial %s: %v", addr, err)
	}
	cl.conn = conn
	go conn.ReadLoop()
	return cl
}

// recv waits for the next text frame.
func (w *wsClient) recv(t *testing.T, d time.Duration) (string, bool) {
	t.Helper()
	select {
	case m := <-w.text:
		return m, true
	case <-time.After(d):
		return "", false
	}
}

type wsRes struct {
	Code int             `json:"code"`
	Data json.RawMessage `json:"data"`
}

func parseRes(t *testing.T, frame string) (action string, res wsRes) {
	t.Helper()
	body := frame
	if i := strings.Index(frame, "|"); i != -1 && !strings.HasPrefix(frame, "{") {
		action, body = frame[:i], frame[i+1:]
	}
	if err := json.Unmarshal([]byte(body), &res); err != nil {
		t.Fatalf("bad response frame %q: %v", frame, err)
	}
	return
}

func (w *wsClient) sendAuth(t *testing.T, token string) {
	t.Helper()
	if err := w.conn.WriteString("Authorization|" + token); err != nil {
		t.Fatalf("write auth: %v", err)
	}
}

func chunkFrame(sessionID string, idx uint32, data string) []byte {
	b := []byte("00" + sessionID)
	b = binary.BigEndian.AppendUint32(b, idx)
	return append(b, data...)
}

func (w *wsClient) sendChunk(t *testing.T, sessionID string, idx uint32, data string) {
	t.Helper()
	if err := w.conn.WriteMessage(gws.OpcodeBinary, chunkFrame(sessionID, idx, data)); err != nil {
		t.Fatalf("write binary: %v", err)
	}
}

// expectAuthSuccess reads frames until the Authorization response arrives and asserts success.
func expectAuthSuccess(t *testing.T, w *wsClient) {
	t.Helper()
	deadline := time.After(5 * time.Second)
	for {
		select {
		case m := <-w.text:
			if !strings.HasPrefix(m, "Authorization|") {
				continue
			}
			_, res := parseRes(t, m)
			if res.Code != code.Success.Code() {
				t.Fatalf("Authorization failed: %s", m)
			}
			return
		case <-deadline:
			t.Fatal("timeout waiting for Authorization response")
		}
	}
}

const sessionID36 = "0123456789abcdef0123456789abcdef0123" // 36 bytes

// C1: gin recycles the upgrade *gin.Context once Run() returns. Authorization sent afterwards
// must still see the x-client/client value captured at handshake time.
func TestE2E_AuthorizationUsesHandshakeSnapshotAfterContextRecycle(t *testing.T) {
	env := newE2EEnv(t)
	cl := env.dial(t, "client=ObsidianPlugin&clientName=e2e&clientVersion=1.0.0")

	// Recycle gin's pooled contexts with unrelated requests (no x-client header, no client query),
	// before Authorization is sent. Many requests so the upgrade context is certainly reused
	// (sync.Pool is per-P and drops items randomly under -race).
	for i := 0; i < 300; i++ {
		resp, err := http.Get(env.srv.URL + "/api/dummy?other=" + strings.Repeat("x", i%5))
		if err != nil {
			t.Fatal(err)
		}
		resp.Body.Close()
	}

	cl.sendAuth(t, e2eToken(t, 1001))
	expectAuthSuccess(t, cl)

	types, _, _, _ := env.snapshot()
	if len(types) != 1 || types[0] != "ObsidianPlugin" {
		t.Fatalf("tokenVerify saw client types %v, want [ObsidianPlugin]", types)
	}
}

// x-client header takes precedence over the client query and is captured at handshake.
func TestE2E_XClientHeaderSnapshotPrecedence(t *testing.T) {
	env := newE2EEnv(t)
	cl := &wsClient{text: make(chan string, 8), closed: make(chan struct{})}
	addr := "ws" + strings.TrimPrefix(env.srv.URL, "http") + "/api/user/sync?client=web"
	hdr := http.Header{}
	hdr.Set("x-client", "ObsidianPlugin")
	conn, _, err := gws.NewClient(cl, &gws.ClientOption{Addr: addr, RequestHeader: hdr})
	if err != nil {
		t.Fatal(err)
	}
	cl.conn = conn
	go conn.ReadLoop()

	for i := 0; i < 100; i++ {
		resp, err := http.Get(env.srv.URL + "/api/dummy")
		if err != nil {
			t.Fatal(err)
		}
		resp.Body.Close()
	}
	cl.sendAuth(t, e2eToken(t, 1002))
	expectAuthSuccess(t, cl)
	types, _, _, _ := env.snapshot()
	if len(types) != 1 || types[0] != "ObsidianPlugin" {
		t.Fatalf("tokenVerify saw %v, want [ObsidianPlugin]", types)
	}
}

// C2: a binary chunk frame sent right behind Authorization (before it completed) must not crash
// the server; it waits for Authorization and then reaches the handler with a non-nil user.
func TestE2E_BinaryFrameRightAfterAuthorizationWaitsForAuth(t *testing.T) {
	env := newE2EEnv(t)
	cl := env.dial(t, "client=ObsidianPlugin")

	cl.sendAuth(t, e2eToken(t, 2002))
	cl.sendChunk(t, sessionID36, 3, "payload-data") // not waiting for the auth reply

	select {
	case <-env.binaryCh:
	case <-time.After(8 * time.Second):
		t.Fatal("binary handler was never invoked")
	}
	expectAuthSuccess(t, cl)

	_, calls, ids, payloads := env.snapshot()
	if calls != 1 || ids[0] != "2002" {
		t.Fatalf("binary handler calls=%d userIDs=%v, want 1 call by user 2002", calls, ids)
	}
	want := chunkFrame(sessionID36, 3, "payload-data")[2:]
	if string(payloads[0]) != string(want) {
		t.Fatalf("payload mismatch: %q vs %q", payloads[0], want)
	}

	// server still alive: a second connection authenticates fine
	cl2 := env.dial(t, "client=ObsidianPlugin")
	cl2.sendAuth(t, e2eToken(t, 2003))
	expectAuthSuccess(t, cl2)
}

// Many binary frames raced against Authorization on one connection: none may panic or be lost.
func TestE2E_ManyBinaryFramesRacingAuthorization(t *testing.T) {
	env := newE2EEnv(t)
	cl := env.dial(t, "client=ObsidianPlugin")
	const n = 10
	cl.sendAuth(t, e2eToken(t, 2500))
	for i := 0; i < n; i++ {
		cl.sendChunk(t, sessionID36, uint32(i), "d")
	}
	for i := 0; i < n; i++ {
		select {
		case <-env.binaryCh:
		case <-time.After(8 * time.Second):
			t.Fatalf("only %d/%d binary frames handled", i, n)
		}
	}
	_, _, ids, _ := env.snapshot()
	for _, id := range ids {
		if id != "2500" {
			t.Fatalf("binary handler saw user %q", id)
		}
	}
}

// A binary frame on a connection that never authorizes: after the (bounded) wait the client gets
// ErrorNotUserAuthToken carrying the sessionID, the handler is never invoked, the server survives.
// Waits for binaryAuthWait (10s), so it is skipped in -short mode.
func TestE2E_BinaryFrameWithoutAuthorizationRejectedWithSessionID(t *testing.T) {
	if testing.Short() {
		t.Skip("waits binaryAuthWait")
	}
	env := newE2EEnv(t)
	cl := env.dial(t, "client=ObsidianPlugin")

	cl.sendChunk(t, sessionID36, 0, "x")

	m, ok := cl.recv(t, binaryAuthWait+5*time.Second)
	if !ok {
		t.Fatal("no rejection received for binary frame on unauthenticated connection")
	}
	action, res := parseRes(t, m)
	if action != "" || res.Code != code.ErrorNotUserAuthToken.Code() {
		t.Fatalf("frame %q: action=%q code=%d, want code %d", m, action, res.Code, code.ErrorNotUserAuthToken.Code())
	}
	var d map[string]string
	if err := json.Unmarshal(res.Data, &d); err != nil || d["sessionID"] != sessionID36 {
		t.Fatalf("data=%s, want sessionID %s", res.Data, sessionID36)
	}
	if _, calls, _, _ := env.snapshot(); calls != 0 {
		t.Fatalf("binary handler invoked %d times", calls)
	}

	// connection is still usable for Authorization afterwards
	cl.sendAuth(t, e2eToken(t, 4004))
	expectAuthSuccess(t, cl)
}

// NOTE on ordering: this test must stay last in the file. WebsocketServer.OnClose reads
// len(w.clients) without holding w.mu (pre-existing, unrelated to the patch), so any later
// AddClient/RemoveClient on the same shared server would be flagged by -race. Test connections
// are therefore never closed by the tests themselves, and the only server-initiated close
// (this test) happens last. Do not run with -count>1 under -race.
//
// C3: token verify rejects (no client param). A binary frame racing the failing Authorization must
// never reach the handler, must not crash the server, and a later connection must still work.
func TestE2E_BinaryFrameOnRejectedAuthorizationNeverReachesHandler(t *testing.T) {
	env := newE2EEnv(t)
	cl := env.dial(t, "") // no client param -> tokenVerify rejects

	cl.sendAuth(t, e2eToken(t, 3003))
	cl.sendChunk(t, sessionID36, 0, "should-not-be-processed")

	// Authorization failure response arrives first
	m, ok := cl.recv(t, 5*time.Second)
	if !ok {
		t.Fatal("no Authorization failure response")
	}
	action, res := parseRes(t, m)
	if action != "Authorization" || res.Code != code.ErrorAuthTokenClientRestricted.Code() {
		t.Fatalf("unexpected first frame %q (action=%q code=%d)", m, action, res.Code)
	}

	// Server still alive: a new, well-formed connection authenticates (done inside the 2s window
	// before the server closes the rejected connection; see the note on test ordering below).
	cl2 := env.dial(t, "client=ObsidianPlugin")
	cl2.sendAuth(t, e2eToken(t, 3004))
	expectAuthSuccess(t, cl2)

	// The server closes the rejected connection ~2s after the failure; wait for it.
	select {
	case <-cl.closed:
	case <-time.After(6 * time.Second):
		t.Fatal("connection not closed after authorization failure")
	}

	// Collect any further frames: if the sessionID rejection was delivered it must carry the sessionID.
	var gotSession bool
	for {
		m, ok := cl.recv(t, 300*time.Millisecond)
		if !ok {
			break
		}
		_, r := parseRes(t, m)
		if r.Code == code.ErrorNotUserAuthToken.Code() {
			var d map[string]string
			if err := json.Unmarshal(r.Data, &d); err != nil || d["sessionID"] != sessionID36 {
				t.Fatalf("rejection data = %s, want sessionID %s", r.Data, sessionID36)
			}
			gotSession = true
		}
	}
	t.Logf("sessionID rejection delivered to client after failed auth: %v", gotSession)

	time.Sleep(200 * time.Millisecond)
	if _, calls, _, _ := env.snapshot(); calls != 0 {
		t.Fatalf("binary handler invoked %d times for an unauthenticated connection", calls)
	}
}
