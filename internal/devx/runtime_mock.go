package devx

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"time"
)

type mockFoundryRuntime struct {
	HostURL      string
	ContainerURL string
	close        func()
}

func (m mockFoundryRuntime) Close() {
	if m.close != nil {
		m.close()
	}
}

func startMockFoundryServer(handler http.Handler, forContainer bool) (mockFoundryRuntime, error) {
	if !forContainer {
		s := httptest.NewServer(handler)
		return mockFoundryRuntime{
			HostURL:      s.URL,
			ContainerURL: s.URL,
			close:        s.Close,
		}, nil
	}

	ln, err := net.Listen("tcp4", "0.0.0.0:0")
	if err != nil {
		return mockFoundryRuntime{}, fmt.Errorf("start mock Foundry listener: %w", err)
	}
	tcpAddr, ok := ln.Addr().(*net.TCPAddr)
	if !ok {
		_ = ln.Close()
		return mockFoundryRuntime{}, fmt.Errorf("mock Foundry listener returned unexpected address %T", ln.Addr())
	}
	srv := &http.Server{Handler: handler}
	done := make(chan struct{})
	go func() {
		defer close(done)
		if err := srv.Serve(ln); err != nil && err != http.ErrServerClosed {
			// The request path will surface connection failures; avoid racing logs into stdout.
		}
	}()
	port := tcpAddr.Port
	return mockFoundryRuntime{
		HostURL:      fmt.Sprintf("http://127.0.0.1:%d", port),
		ContainerURL: fmt.Sprintf("http://127.0.0.1:%d", port),
		close: func() {
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()
			_ = srv.Shutdown(ctx)
			<-done
		},
	}, nil
}
