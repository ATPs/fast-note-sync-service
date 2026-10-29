package app

import (
	"context"
	"testing"
	"time"

	validatorV10 "github.com/go-playground/validator/v10"
)

func newAuthClient() *WebsocketClient {
	return &WebsocketClient{authDone: make(chan struct{})}
}

func TestWaitAuth_BlocksUntilMarkAuthDone(t *testing.T) {
	c := newAuthClient()

	result := make(chan bool, 1)
	go func() { result <- c.waitAuth(t.Context(), 5*time.Second) }()

	select {
	case <-result:
		t.Fatal("waitAuth returned before markAuthDone")
	case <-time.After(150 * time.Millisecond):
	}

	c.User = &UserEntity{}
	c.User.ID = "1"
	c.markAuthDone()

	select {
	case ok := <-result:
		if !ok {
			t.Fatal("waitAuth = false, want true (user set before markAuthDone)")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("waitAuth did not return after markAuthDone")
	}
}

func TestWaitAuth_FalseWhenUserNilAfterDone(t *testing.T) {
	c := newAuthClient()
	c.markAuthDone()
	start := time.Now()
	if c.waitAuth(t.Context(), 5*time.Second) {
		t.Fatal("waitAuth = true for unauthenticated client")
	}
	if time.Since(start) > time.Second {
		t.Fatal("waitAuth should return immediately once auth is done")
	}
}

func TestWaitAuth_TrueWhenUserSetBeforeDone(t *testing.T) {
	c := newAuthClient()
	c.User = &UserEntity{}
	c.markAuthDone()
	if !c.waitAuth(t.Context(), time.Second) {
		t.Fatal("waitAuth = false, want true")
	}
}

func TestWaitAuth_TimeoutWhenNeverMarked(t *testing.T) {
	c := newAuthClient()
	start := time.Now()
	if c.waitAuth(t.Context(), 100*time.Millisecond) {
		t.Fatal("waitAuth = true, want false after timeout")
	}
	d := time.Since(start)
	if d < 90*time.Millisecond || d > 2*time.Second {
		t.Fatalf("waitAuth returned after %v, want about 100ms", d)
	}
}

func TestWaitAuth_ReturnsOnContextCancel(t *testing.T) {
	c := newAuthClient()
	ctx, cancel := contextWithCancel(t)
	time.AfterFunc(50*time.Millisecond, cancel)
	start := time.Now()
	if c.waitAuth(ctx, 10*time.Second) {
		t.Fatal("waitAuth = true, want false on cancelled ctx")
	}
	if time.Since(start) > 2*time.Second {
		t.Fatal("waitAuth did not return promptly on ctx cancel")
	}
}

func TestWaitAuth_NilAuthDoneDoesNotBlock(t *testing.T) {
	c := &WebsocketClient{}
	start := time.Now()
	if c.waitAuth(t.Context(), 5*time.Second) {
		t.Fatal("waitAuth = true for nil user")
	}
	if time.Since(start) > time.Second {
		t.Fatal("waitAuth blocked with nil authDone")
	}
	c.User = &UserEntity{}
	if !c.waitAuth(t.Context(), 5*time.Second) {
		t.Fatal("waitAuth = false for set user")
	}
}

func TestMarkAuthDone_Idempotent(t *testing.T) {
	c := newAuthClient()
	defer func() {
		if r := recover(); r != nil {
			t.Fatalf("markAuthDone panicked on repeated call: %v", r)
		}
	}()
	c.markAuthDone()
	c.markAuthDone()
	c.markAuthDone()
	select {
	case <-c.authDone:
	default:
		t.Fatal("authDone not closed")
	}
}

func TestUID_GuestForNilUser(t *testing.T) {
	c := &WebsocketClient{}
	if got := c.uid(); got != "Guest" {
		t.Fatalf("uid() = %q, want Guest", got)
	}
	c.User = &UserEntity{}
	c.User.ID = "42"
	if got := c.uid(); got != "42" {
		t.Fatalf("uid() = %q, want 42", got)
	}
}

// Without a translator (e.g. no lang middleware) validation errors fall back to the raw message instead of panicking.
func TestTranslateValidationErrors_NilTranslator(t *testing.T) {
	type req struct {
		Name string `validate:"required"`
	}
	err := validatorV10.New().Struct(&req{})
	verrs, ok := err.(validatorV10.ValidationErrors)
	if !ok {
		t.Fatalf("expected ValidationErrors, got %v", err)
	}
	c := &WebsocketClient{}
	out := c.translateValidationErrors(verrs)
	if len(out) != 1 || out[0].Key != "Name" || out[0].Message == "" {
		t.Fatalf("unexpected result: %+v", out)
	}
}

func contextWithCancel(t *testing.T) (ctx context.Context, cancel context.CancelFunc) {
	t.Helper()
	ctx, cancel = context.WithCancel(t.Context())
	t.Cleanup(cancel)
	return
}
