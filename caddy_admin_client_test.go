package main

import (
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"
)

func startCaddyAdminTestServer(
	t *testing.T,
	handler http.Handler,
) string {
	t.Helper()

	socketPath := filepath.Join(t.TempDir(), "a.sock")
	listener, err := net.Listen("unix", socketPath)
	if err != nil {
		t.Fatalf("listen on test Unix socket: %v", err)
	}

	server := &http.Server{
		Handler: handler,
	}

	go func() {
		_ = server.Serve(listener)
	}()

	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(
			context.Background(),
			time.Second,
		)
		defer cancel()

		if err := server.Shutdown(ctx); err != nil {
			_ = server.Close()
			t.Errorf("shut down Caddy Admin test server: %v", err)
		}
	})

	return socketPath
}

func newCaddyAdminTestClient(
	t *testing.T,
	handler http.Handler,
	config caddyAdminClientConfig,
) *caddyAdminClient {
	t.Helper()

	socketPath := startCaddyAdminTestServer(t, handler)
	if config.SocketPath == "" {
		config.SocketPath = socketPath
	}

	client, err := newCaddyAdminClient(config)
	if err != nil {
		t.Fatalf("create Caddy Admin client: %v", err)
	}

	t.Cleanup(client.Close)

	return client
}

func caddyAdminTestMutationBody(method string) []byte {
	switch method {
	case http.MethodPost, http.MethodPatch:
		return []byte(`{"routes":[]}`)
	default:
		return nil
	}
}

func TestCaddyAdminClientUsesUnixSocketAndCapturesETag(
	t *testing.T,
) {
	client := newCaddyAdminTestClient(
		t,
		http.HandlerFunc(func(
			writer http.ResponseWriter,
			request *http.Request,
		) {
			if request.Method != http.MethodGet {
				t.Errorf("method = %q, want GET", request.Method)
			}

			if request.URL.Path != "/config/" {
				t.Errorf(
					"path = %q, want /config/",
					request.URL.Path,
				)
			}

			if request.Header.Get("Accept") != "application/json" {
				t.Errorf(
					"Accept = %q, want application/json",
					request.Header.Get("Accept"),
				)
			}

			writer.Header().Set("ETag", `"snapshot-v1"`)
			writer.Header().Set(
				"Content-Type",
				"application/json",
			)
			_, _ = writer.Write([]byte(`{"apps":{}}`))
		}),
		caddyAdminClientConfig{},
	)

	response, err := client.do(
		context.Background(),
		http.MethodGet,
		"/config/",
		nil,
		"",
	)
	if err != nil {
		t.Fatalf("read Caddy configuration: %v", err)
	}

	if response.StatusCode != http.StatusOK {
		t.Errorf(
			"status code = %d, want %d",
			response.StatusCode,
			http.StatusOK,
		)
	}

	if response.ETag != `"snapshot-v1"` {
		t.Errorf(
			"ETag = %q, want %q",
			response.ETag,
			`"snapshot-v1"`,
		)
	}

	if string(response.Body) != `{"apps":{}}` {
		t.Errorf(
			"response body = %q, want configuration JSON",
			response.Body,
		)
	}
}

func TestCaddyAdminClientDoesNotUseConfiguredHTTPProxy(
	t *testing.T,
) {
	proxyListener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen for test proxy: %v", err)
	}
	t.Cleanup(func() {
		_ = proxyListener.Close()
	})

	proxyURL := "http://" + proxyListener.Addr().String()

	for _, name := range []string{
		"HTTP_PROXY",
		"http_proxy",
		"HTTPS_PROXY",
		"https_proxy",
		"ALL_PROXY",
		"all_proxy",
	} {
		t.Setenv(name, proxyURL)
	}
	t.Setenv("NO_PROXY", "")
	t.Setenv("no_proxy", "")

	missingSocketPath := filepath.Join(
		t.TempDir(),
		"missing.sock",
	)

	client, err := newCaddyAdminClient(caddyAdminClientConfig{
		SocketPath: missingSocketPath,
	})
	if err != nil {
		t.Fatalf("create Caddy Admin client: %v", err)
	}
	t.Cleanup(client.Close)

	_, requestErr := client.do(
		context.Background(),
		http.MethodGet,
		"/config/",
		nil,
		"",
	)
	if !errors.Is(requestErr, errCaddyAdminUnavailable) {
		t.Fatalf(
			"request error = %v, want Caddy Admin unavailable",
			requestErr,
		)
	}

	tcpListener, ok := proxyListener.(*net.TCPListener)
	if !ok {
		t.Fatal("test proxy listener is not a TCP listener")
	}

	if err := tcpListener.SetDeadline(
		time.Now().Add(100 * time.Millisecond),
	); err != nil {
		t.Fatalf("set proxy listener deadline: %v", err)
	}

	connection, acceptErr := tcpListener.Accept()
	if acceptErr == nil {
		_ = connection.Close()
		t.Fatal("Caddy Admin client unexpectedly connected to HTTP proxy")
	}

	if timeout, ok := acceptErr.(net.Error); !ok || !timeout.Timeout() {
		t.Fatalf(
			"proxy accept error = %v, want timeout with no connection",
			acceptErr,
		)
	}
}

func TestCaddyAdminClientRejectsMutationsWithoutIfMatch(
	t *testing.T,
) {
	var requestCount atomic.Int32

	client := newCaddyAdminTestClient(
		t,
		http.HandlerFunc(func(
			writer http.ResponseWriter,
			_ *http.Request,
		) {
			requestCount.Add(1)
			writer.WriteHeader(http.StatusOK)
		}),
		caddyAdminClientConfig{},
	)

	for _, method := range []string{
		http.MethodPost,
		http.MethodPatch,
		http.MethodDelete,
	} {
		t.Run(method, func(t *testing.T) {
			_, err := client.do(
				context.Background(),
				method,
				"/config/",
				caddyAdminTestMutationBody(method),
				"",
			)

			if !errors.Is(err, errCaddyAdminInvalidRequest) {
				t.Fatalf(
					"request error = %v, want invalid request",
					err,
				)
			}

			if got := requestCount.Load(); got != 0 {
				t.Errorf(
					"server request count = %d, want 0",
					got,
				)
			}
		})
	}
}

func TestCaddyAdminClientPreservesIfMatchAndJSONBody(
	t *testing.T,
) {
	const (
		wantETag = `"snapshot-v2"`
		wantBody = `{"routes":[{"@id":"route-1"}]}`
	)

	client := newCaddyAdminTestClient(
		t,
		http.HandlerFunc(func(
			writer http.ResponseWriter,
			request *http.Request,
		) {
			if request.Method != http.MethodPost {
				t.Errorf(
					"method = %q, want POST",
					request.Method,
				)
			}

			if got := request.Header.Get("If-Match"); got != wantETag {
				t.Errorf(
					"If-Match = %q, want %q",
					got,
					wantETag,
				)
			}

			if got := request.Header.Get("Content-Type"); got != "application/json" {
				t.Errorf(
					"Content-Type = %q, want application/json",
					got,
				)
			}

			if got := request.Header.Get("Accept"); got != "application/json" {
				t.Errorf(
					"Accept = %q, want application/json",
					got,
				)
			}

			body, err := io.ReadAll(request.Body)
			if err != nil {
				t.Errorf("read request body: %v", err)
				return
			}

			if string(body) != wantBody {
				t.Errorf(
					"request body = %q, want %q",
					body,
					wantBody,
				)
			}

			writer.WriteHeader(http.StatusOK)
		}),
		caddyAdminClientConfig{},
	)

	_, err := client.do(
		context.Background(),
		http.MethodPost,
		"/config/apps/http/servers/nexvarin/routes",
		[]byte(wantBody),
		wantETag,
	)
	if err != nil {
		t.Fatalf("send conditional Caddy mutation: %v", err)
	}
}

func TestCaddyAdminClientDoesNotRetryPreconditionFailure(
	t *testing.T,
) {
	var requestCount atomic.Int32

	client := newCaddyAdminTestClient(
		t,
		http.HandlerFunc(func(
			writer http.ResponseWriter,
			_ *http.Request,
		) {
			requestCount.Add(1)
			writer.WriteHeader(http.StatusPreconditionFailed)
			_, _ = writer.Write([]byte("stale configuration"))
		}),
		caddyAdminClientConfig{},
	)

	response, err := client.do(
		context.Background(),
		http.MethodPost,
		"/config/routes",
		[]byte(`{"routes":[]}`),
		`"stale-etag"`,
	)
	if err != nil {
		t.Fatalf("HTTP 412 should be returned for caller reconciliation: %v", err)
	}

	if response.StatusCode != http.StatusPreconditionFailed {
		t.Errorf(
			"status code = %d, want %d",
			response.StatusCode,
			http.StatusPreconditionFailed,
		)
	}

	if string(response.Body) != "stale configuration" {
		t.Errorf(
			"response body = %q, want stale configuration",
			response.Body,
		)
	}

	if got := requestCount.Load(); got != 1 {
		t.Errorf("request count = %d, want exactly 1", got)
	}
}

func TestCaddyAdminClientClassifiesMutationServerErrorsAsUnknown(
	t *testing.T,
) {
	for _, statusCode := range []int{
		http.StatusInternalServerError,
		http.StatusServiceUnavailable,
	} {
		for _, method := range []string{
			http.MethodPost,
			http.MethodPatch,
			http.MethodDelete,
		} {
			t.Run(
				http.StatusText(statusCode)+"/"+method,
				func(t *testing.T) {
					var requestCount atomic.Int32

					client := newCaddyAdminTestClient(
						t,
						http.HandlerFunc(func(
							writer http.ResponseWriter,
							_ *http.Request,
						) {
							requestCount.Add(1)
							writer.WriteHeader(statusCode)
							_, _ = writer.Write(
								[]byte("server failure"),
							)
						}),
						caddyAdminClientConfig{},
					)

					response, err := client.do(
						context.Background(),
						method,
						"/config/routes",
						caddyAdminTestMutationBody(method),
						`"snapshot-v3"`,
					)

					if !errors.Is(err, errCaddyAdminOutcomeUnknown) {
						t.Fatalf(
							"request error = %v, want unknown mutation outcome",
							err,
						)
					}

					if response.StatusCode != statusCode {
						t.Errorf(
							"status code = %d, want %d",
							response.StatusCode,
							statusCode,
						)
					}

					if string(response.Body) != "server failure" {
						t.Errorf(
							"response body = %q, want server failure",
							response.Body,
						)
					}

					if got := requestCount.Load(); got != 1 {
						t.Errorf(
							"request count = %d, want exactly 1",
							got,
						)
					}
				},
			)
		}
	}
}

func TestCaddyAdminClientClassifiesConnectionDropAsUnknown(
	t *testing.T,
) {
	var requestCount atomic.Int32
	handlerErr := make(chan error, 1)

	client := newCaddyAdminTestClient(
		t,
		http.HandlerFunc(func(
			writer http.ResponseWriter,
			_ *http.Request,
		) {
			requestCount.Add(1)

			hijacker, ok := writer.(http.Hijacker)
			if !ok {
				handlerErr <- errors.New(
					"HTTP response writer does not support hijacking",
				)
				writer.WriteHeader(http.StatusInternalServerError)
				return
			}

			connection, _, err := hijacker.Hijack()
			if err != nil {
				handlerErr <- err
				return
			}

			_ = connection.Close()
		}),
		caddyAdminClientConfig{},
	)

	_, err := client.do(
		context.Background(),
		http.MethodPost,
		"/config/routes",
		[]byte(`{"routes":[]}`),
		`"snapshot-v4"`,
	)
	if !errors.Is(err, errCaddyAdminOutcomeUnknown) {
		t.Fatalf(
			"request error = %v, want unknown mutation outcome",
			err,
		)
	}

	select {
	case handlerErr := <-handlerErr:
		t.Fatalf("drop test handler failed: %v", handlerErr)
	default:
	}

	if got := requestCount.Load(); got != 1 {
		t.Errorf("request count = %d, want exactly 1", got)
	}
}

func TestCaddyAdminClientClassifiesCancelledMutationAsUnknown(
	t *testing.T,
) {
	var requestCount atomic.Int32
	receivedRequest := make(chan struct{}, 1)
	releaseHandler := make(chan struct{})
	defer close(releaseHandler)

	client := newCaddyAdminTestClient(
		t,
		http.HandlerFunc(func(
			_ http.ResponseWriter,
			_ *http.Request,
		) {
			requestCount.Add(1)
			receivedRequest <- struct{}{}
			<-releaseHandler
		}),
		caddyAdminClientConfig{},
	)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	type result struct {
		response caddyAdminResponse
		err      error
	}

	done := make(chan result, 1)

	go func() {
		response, err := client.do(
			ctx,
			http.MethodPost,
			"/config/routes",
			[]byte(`{"routes":[]}`),
			`"snapshot-v5"`,
		)
		done <- result{
			response: response,
			err:      err,
		}
	}()

	select {
	case <-receivedRequest:
		cancel()
	case <-time.After(2 * time.Second):
		cancel()
		t.Fatal("server did not receive the mutation request")
	}

	select {
	case result := <-done:
		if !errors.Is(result.err, errCaddyAdminOutcomeUnknown) {
			t.Fatalf(
				"request error = %v, want unknown mutation outcome",
				result.err,
			)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("cancelled mutation request did not return")
	}

	if got := requestCount.Load(); got != 1 {
		t.Errorf("request count = %d, want exactly 1", got)
	}
}

func TestCaddyAdminClientClassifiesMutationTimeoutAsUnknown(
	t *testing.T,
) {
	var requestCount atomic.Int32
	receivedRequest := make(chan struct{}, 1)
	releaseHandler := make(chan struct{})
	defer close(releaseHandler)

	client := newCaddyAdminTestClient(
		t,
		http.HandlerFunc(func(
			_ http.ResponseWriter,
			_ *http.Request,
		) {
			requestCount.Add(1)
			receivedRequest <- struct{}{}
			<-releaseHandler
		}),
		caddyAdminClientConfig{
			RequestTimeout: 500 * time.Millisecond,
		},
	)

	_, err := client.do(
		context.Background(),
		http.MethodPost,
		"/config/routes",
		[]byte(`{"routes":[]}`),
		`"snapshot-v6"`,
	)
	if !errors.Is(err, errCaddyAdminOutcomeUnknown) {
		t.Fatalf(
			"request error = %v, want unknown mutation outcome",
			err,
		)
	}

	select {
	case <-receivedRequest:
	default:
		t.Fatal("server did not receive the mutation request")
	}

	if got := requestCount.Load(); got != 1 {
		t.Errorf("request count = %d, want exactly 1", got)
	}
}

func TestCaddyAdminClientRejectsOversizedResponse(
	t *testing.T,
) {
	client := newCaddyAdminTestClient(
		t,
		http.HandlerFunc(func(
			writer http.ResponseWriter,
			_ *http.Request,
		) {
			_, _ = writer.Write([]byte("12345"))
		}),
		caddyAdminClientConfig{
			MaxResponseBytes: 4,
		},
	)

	_, err := client.do(
		context.Background(),
		http.MethodGet,
		"/config/",
		nil,
		"",
	)
	if !errors.Is(err, errCaddyAdminResponseTooLarge) {
		t.Fatalf(
			"request error = %v, want response-too-large",
			err,
		)
	}
}

func TestCaddyAdminClientRejectsRedirectsWithoutFollowing(
	t *testing.T,
) {
	var targetRequestCount atomic.Int32

	client := newCaddyAdminTestClient(
		t,
		http.HandlerFunc(func(
			writer http.ResponseWriter,
			request *http.Request,
		) {
			switch request.URL.Path {
			case "/redirect":
				writer.Header().Set("Location", "/target")
				writer.WriteHeader(http.StatusTemporaryRedirect)
			case "/target":
				targetRequestCount.Add(1)
				writer.WriteHeader(http.StatusOK)
			default:
				writer.WriteHeader(http.StatusNotFound)
			}
		}),
		caddyAdminClientConfig{},
	)

	response, err := client.do(
		context.Background(),
		http.MethodGet,
		"/redirect",
		nil,
		"",
	)
	if !errors.Is(err, errCaddyAdminRedirectRejected) {
		t.Fatalf(
			"request error = %v, want redirect rejected",
			err,
		)
	}

	if response.StatusCode != http.StatusTemporaryRedirect {
		t.Errorf(
			"status code = %d, want %d",
			response.StatusCode,
			http.StatusTemporaryRedirect,
		)
	}

	if got := targetRequestCount.Load(); got != 0 {
		t.Errorf("redirect target request count = %d, want 0", got)
	}
}

func TestCaddyAdminClientDoesNotTreat304AsRedirect(
	t *testing.T,
) {
	client := newCaddyAdminTestClient(
		t,
		http.HandlerFunc(func(
			writer http.ResponseWriter,
			_ *http.Request,
		) {
			writer.WriteHeader(http.StatusNotModified)
		}),
		caddyAdminClientConfig{},
	)

	response, err := client.do(
		context.Background(),
		http.MethodGet,
		"/config/",
		nil,
		"",
	)
	if err != nil {
		t.Fatalf("HTTP 304 should not be classified as redirect: %v", err)
	}

	if response.StatusCode != http.StatusNotModified {
		t.Errorf(
			"status code = %d, want %d",
			response.StatusCode,
			http.StatusNotModified,
		)
	}
}

func TestCaddyAdminClientRejectsMultipleETags(
	t *testing.T,
) {
	client := newCaddyAdminTestClient(
		t,
		http.HandlerFunc(func(
			writer http.ResponseWriter,
			_ *http.Request,
		) {
			writer.Header().Add("ETag", `"first"`)
			writer.Header().Add("ETag", `"second"`)
			writer.WriteHeader(http.StatusOK)
		}),
		caddyAdminClientConfig{},
	)

	_, err := client.do(
		context.Background(),
		http.MethodGet,
		"/config/",
		nil,
		"",
	)
	if !errors.Is(err, errCaddyAdminInvalidResponse) {
		t.Fatalf(
			"request error = %v, want invalid response",
			err,
		)
	}
}

func TestValidateCaddyAdminRequestPath(t *testing.T) {
	validPaths := []string{
		"/config/",
		"/config/apps/http/servers",
		"/id/nexvarin-preview-route-namespace",
	}

	for _, requestPath := range validPaths {
		t.Run("valid/"+requestPath, func(t *testing.T) {
			if _, err := validateCaddyAdminRequestPath(requestPath); err != nil {
				t.Fatalf("valid path rejected: %v", err)
			}
		})
	}

	invalidPaths := []string{
		"",
		"config/",
		"//example.com/config",
		"/config?query=1",
		"/config#fragment",
		"/one/../two",
		"/one/./two",
		"/one\\two",
		"/config%2f..%2fsecret",
	}

	for _, requestPath := range invalidPaths {
		t.Run("invalid/"+requestPath, func(t *testing.T) {
			if _, err := validateCaddyAdminRequestPath(requestPath); !errors.Is(
				err,
				errCaddyAdminInvalidRequest,
			) {
				t.Fatalf(
					"error = %v, want invalid request",
					err,
				)
			}
		})
	}
}

func TestNewCaddyAdminClientRejectsInvalidConfiguration(
	t *testing.T,
) {
	validSocketPath := filepath.Join(t.TempDir(), "a.sock")

	tests := []struct {
		name   string
		config caddyAdminClientConfig
	}{
		{
			name:   "empty socket path",
			config: caddyAdminClientConfig{},
		},
		{
			name: "relative socket path",
			config: caddyAdminClientConfig{
				SocketPath: "caddy-admin.sock",
			},
		},
		{
			name: "socket path with surrounding whitespace",
			config: caddyAdminClientConfig{
				SocketPath: validSocketPath + " ",
			},
		},
		{
			name: "unclean socket path",
			config: caddyAdminClientConfig{
				SocketPath: validSocketPath + "/../caddy-admin.sock",
			},
		},
		{
			name: "socket path with null byte",
			config: caddyAdminClientConfig{
				SocketPath: validSocketPath + "\x00",
			},
		},
		{
			name: "request timeout below minimum",
			config: caddyAdminClientConfig{
				SocketPath:     validSocketPath,
				RequestTimeout: time.Millisecond / 2,
			},
		},
		{
			name: "negative response limit",
			config: caddyAdminClientConfig{
				SocketPath:       validSocketPath,
				MaxResponseBytes: -1,
			},
		},
		{
			name: "response limit above maximum",
			config: caddyAdminClientConfig{
				SocketPath:       validSocketPath,
				MaxResponseBytes: maxCaddyAdminMaxResponse + 1,
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, err := newCaddyAdminClient(test.config)
			if !errors.Is(err, errCaddyAdminInvalidRequest) {
				t.Fatalf(
					"configuration error = %v, want invalid request",
					err,
				)
			}
		})
	}
}

func TestNewCaddyAdminClientAppliesDefaultLimits(
	t *testing.T,
) {
	client, err := newCaddyAdminClient(caddyAdminClientConfig{
		SocketPath: filepath.Join(t.TempDir(), "a.sock"),
	})
	if err != nil {
		t.Fatalf("create Caddy Admin client: %v", err)
	}
	t.Cleanup(client.Close)

	if client.requestTimeout != defaultCaddyAdminRequestTimeout {
		t.Errorf(
			"request timeout = %s, want %s",
			client.requestTimeout,
			defaultCaddyAdminRequestTimeout,
		)
	}

	if client.maxResponseBytes != defaultCaddyAdminMaxResponse {
		t.Errorf(
			"maximum response bytes = %d, want %d",
			client.maxResponseBytes,
			defaultCaddyAdminMaxResponse,
		)
	}
}
