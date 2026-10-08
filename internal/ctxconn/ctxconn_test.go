// SPDX-FileCopyrightText: 2023 The Pion community <https://pion.ly>
// SPDX-License-Identifier: MIT

package ctxconn

import (
	"context"
	"errors"
	"net"
	"runtime"
	"testing"
	"time"
)

func pair(t *testing.T) (net.PacketConn, net.PacketConn) {
	t.Helper()
	a, err := net.ListenPacket("udp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	b, err := net.ListenPacket("udp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = a.Close(); _ = b.Close() })

	return a, b
}

func TestReadCancelledByContext(t *testing.T) {
	a, _ := pair(t)
	c := New(a)
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		time.Sleep(20 * time.Millisecond)
		cancel()
	}()
	start := time.Now()
	_, _, err := c.ReadFromContext(ctx, make([]byte, 16))
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("err = %v, want context.Canceled", err)
	}
	if time.Since(start) > 2*time.Second {
		t.Fatal("cancel did not unblock the read")
	}

	// The deadline is cleared again, so a later read works.
	_, b := pair(t)
	go func() {
		time.Sleep(20 * time.Millisecond)
		_, _ = b.WriteTo([]byte("x"), a.LocalAddr())
	}()
	n, _, err := c.ReadFromContext(context.Background(), make([]byte, 16))
	if err != nil || n != 1 {
		t.Fatalf("read after cancel: %d, %v", n, err)
	}
}

// A read under a live context starts no goroutine.
func TestReadStartsNoGoroutine(t *testing.T) {
	a, b := pair(t)
	c := New(a)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	buf := make([]byte, 16)
	before := runtime.NumGoroutine()
	peak := before
	for i := 0; i < 100; i++ {
		if _, err := b.WriteTo([]byte("x"), a.LocalAddr()); err != nil {
			t.Fatal(err)
		}
		if _, _, err := c.ReadFromContext(ctx, buf); err != nil {
			t.Fatal(err)
		}
		peak = max(peak, runtime.NumGoroutine())
	}
	if peak > before {
		t.Fatalf("goroutines went from %d to %d during reads", before, peak)
	}
}

func TestWriteCancelledContext(t *testing.T) {
	a, b := pair(t)
	c := New(a)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := c.WriteToContext(ctx, []byte("x"), b.LocalAddr()); err != nil && !errors.Is(err, context.Canceled) {
		t.Fatalf("write with a cancelled context: %v", err)
	}
	// The deadline is cleared again afterwards.
	if _, err := c.WriteToContext(context.Background(), []byte("y"), b.LocalAddr()); err != nil {
		t.Fatalf("write after cancel: %v", err)
	}
}

func TestClosed(t *testing.T) {
	a, _ := pair(t)
	c := New(a)
	_ = c.Close()
	if _, _, err := c.ReadFromContext(context.Background(), make([]byte, 1)); !errors.Is(err, net.ErrClosed) {
		t.Fatalf("read after close: %v", err)
	}
}
