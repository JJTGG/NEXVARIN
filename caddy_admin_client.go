package main

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"path/filepath"
	"strings"
	"time"
)

const (
	caddyAdminURLHost = "caddy-admin"
	caddyAdminBaseURL = "http://" + caddyAdminURLHost

	defaultCaddyAdminRequestTimeout = 5 * time.Second
	defaultCaddyAdminMaxResponse    = int64(4 << 20)
	maxCaddyAdminMaxResponse        = int64(64 << 20)
	caddyAdminMaxHeaderBytes        = 32 << 10
)

var (
	errCaddyAdminClientNotConfigured = errors.New(
		"Caddy admin client is not configured",
	)
	errCaddyAdminInvalidRequest = errors.New(
		"Caddy admin request is invalid",
	)
	errCaddyAdminUnavailable = errors.New(
		"Caddy admin API is unavailable",
	)
	errCaddyAdminOutcomeUnknown = errors.New(
		"Caddy admin mutation outcome is unknown",
	)
	errCaddyAdminResponseTooLarge = errors.New(
		"Caddy admin response exceeds the configured size limit",
	)
	errCaddyAdminRedirectRejected = errors.New(
		"Caddy admin API redirect was rejected",
	)
	errCaddyAdminInvalidResponse = errors.New(
		"Caddy admin API response is invalid",
	)
)

type caddyAdminClientConfig struct {
	SocketPath       string
	RequestTimeout   time.Duration
	MaxResponseBytes int64
}

type caddyAdminClient struct {
	httpClient       *http.Client
	transport        *http.Transport
	requestTimeout   time.Duration
	maxResponseBytes int64
}

type caddyAdminResponse struct {
	StatusCode int
	Header     http.Header
	ETag       string
	Body       []byte
}

// newCaddyAdminClient creates an HTTP client that can communicate only with
// the configured Caddy Unix-domain socket. It does not probe or fall back to
// a TCP administration endpoint.
func newCaddyAdminClient(
	config caddyAdminClientConfig,
) (*caddyAdminClient, error) {
	socketPath := config.SocketPath
	if socketPath == "" ||
		strings.TrimSpace(socketPath) != socketPath ||
		!filepath.IsAbs(socketPath) ||
		filepath.Clean(socketPath) != socketPath ||
		strings.ContainsAny(socketPath, "\x00\r\n") {
		return nil, fmt.Errorf(
			"%w: socket path must be a clean absolute filesystem path",
			errCaddyAdminInvalidRequest,
		)
	}

	requestTimeout := config.RequestTimeout
	if requestTimeout == 0 {
		requestTimeout = defaultCaddyAdminRequestTimeout
	}
	if requestTimeout < time.Millisecond {
		return nil, fmt.Errorf(
			"%w: request timeout must be at least one millisecond",
			errCaddyAdminInvalidRequest,
		)
	}

	maxResponseBytes := config.MaxResponseBytes
	if maxResponseBytes == 0 {
		maxResponseBytes = defaultCaddyAdminMaxResponse
	}
	if maxResponseBytes < 1 || maxResponseBytes > maxCaddyAdminMaxResponse {
		return nil, fmt.Errorf(
			"%w: maximum response size must be between 1 byte and 64 MiB",
			errCaddyAdminInvalidRequest,
		)
	}

	dialer := &net.Dialer{
		Timeout:   requestTimeout,
		KeepAlive: 30 * time.Second,
	}

	transport := &http.Transport{
		// A nil Proxy disables HTTP_PROXY, HTTPS_PROXY, and ALL_PROXY.
		Proxy: nil,
		DialContext: func(
			ctx context.Context,
			network string,
			address string,
		) (net.Conn, error) {
			if network != "tcp" || address != caddyAdminURLHost+":80" {
				return nil, fmt.Errorf(
					"%w: unexpected Caddy admin dial target",
					errCaddyAdminInvalidRequest,
				)
			}

			conn, err := dialer.DialContext(ctx, "unix", socketPath)
			if err != nil {
				return nil, fmt.Errorf("dial Caddy admin Unix socket: %w", err)
			}
			return conn, nil
		},
		ForceAttemptHTTP2:      false,
		DisableKeepAlives:      true,
		ResponseHeaderTimeout:  requestTimeout,
		MaxResponseHeaderBytes: caddyAdminMaxHeaderBytes,
		DisableCompression:     true,
	}

	httpClient := &http.Client{
		Transport: transport,
		CheckRedirect: func(
			_ *http.Request,
			_ []*http.Request,
		) error {
			return http.ErrUseLastResponse
		},
	}

	return &caddyAdminClient{
		httpClient:       httpClient,
		transport:        transport,
		requestTimeout:   requestTimeout,
		maxResponseBytes: maxResponseBytes,
	}, nil
}

// do sends exactly one request. It never retries, follows redirects, or
// changes the supplied If-Match value. Mutating methods require a non-empty
// If-Match value from a previously read Caddy configuration snapshot.
func (client *caddyAdminClient) do(
	ctx context.Context,
	method string,
	requestPath string,
	body []byte,
	ifMatch string,
) (caddyAdminResponse, error) {
	if client == nil || client.httpClient == nil {
		return caddyAdminResponse{}, errCaddyAdminClientNotConfigured
	}
	if ctx == nil {
		return caddyAdminResponse{}, fmt.Errorf(
			"%w: request context is required",
			errCaddyAdminInvalidRequest,
		)
	}

	parsedPath, err := validateCaddyAdminRequestPath(requestPath)
	if err != nil {
		return caddyAdminResponse{}, err
	}
	if err := validateCaddyAdminRequest(method, body, ifMatch); err != nil {
		return caddyAdminResponse{}, err
	}

	requestContext, cancel := context.WithTimeout(
		ctx,
		client.requestTimeout,
	)
	defer cancel()

	var requestBody io.Reader
	if body != nil {
		requestBody = bytes.NewReader(body)
	}

	request, err := http.NewRequestWithContext(
		requestContext,
		method,
		caddyAdminBaseURL+parsedPath,
		requestBody,
	)
	if err != nil {
		return caddyAdminResponse{}, fmt.Errorf(
			"%w: construct request: %v",
			errCaddyAdminInvalidRequest,
			err,
		)
	}

	request.Header.Set("Accept", "application/json")
	if body != nil {
		request.Header.Set("Content-Type", "application/json")
	}
	if ifMatch != "" {
		// Preserve the Caddy ETag byte-for-byte. Do not trim or reconstruct it.
		request.Header.Set("If-Match", ifMatch)
	}

	httpResponse, err := client.httpClient.Do(request)
	if err != nil {
		if ctx.Err() != nil {
			return caddyAdminResponse{}, ctx.Err()
		}
		if isCaddyAdminMutation(method) {
			return caddyAdminResponse{}, fmt.Errorf(
				"%w: %v",
				errCaddyAdminOutcomeUnknown,
				err,
			)
		}
		return caddyAdminResponse{}, fmt.Errorf(
			"%w: %v",
			errCaddyAdminUnavailable,
			err,
		)
	}
	defer httpResponse.Body.Close()

	response, readErr := client.readResponse(httpResponse)
	if readErr != nil {
		if isCaddyAdminMutation(method) &&
			((httpResponse.StatusCode >= http.StatusOK &&
				httpResponse.StatusCode < http.StatusMultipleChoices) ||
				httpResponse.StatusCode >= http.StatusInternalServerError) {
			return response, fmt.Errorf(
				"%w: %v",
				errCaddyAdminOutcomeUnknown,
				readErr,
			)
		}
		return response, readErr
	}

	if httpResponse.StatusCode >= http.StatusMultipleChoices &&
		httpResponse.StatusCode < 400 {
		return response, errCaddyAdminRedirectRejected
	}

	return response, nil
}

func (client *caddyAdminClient) readResponse(
	httpResponse *http.Response,
) (caddyAdminResponse, error) {
	response := caddyAdminResponse{
		StatusCode: httpResponse.StatusCode,
		Header:     httpResponse.Header.Clone(),
	}

	etagValues := httpResponse.Header.Values("ETag")
	switch len(etagValues) {
	case 0:
		// The caller decides whether an ETag is mandatory for this operation.
	case 1:
		response.ETag = etagValues[0]
	default:
		return response, fmt.Errorf(
			"%w: response contained multiple ETag headers",
			errCaddyAdminInvalidResponse,
		)
	}

	body, err := io.ReadAll(io.LimitReader(
		httpResponse.Body,
		client.maxResponseBytes+1,
	))
	if err != nil {
		return response, fmt.Errorf(
			"%w: read response body: %v",
			errCaddyAdminUnavailable,
			err,
		)
	}
	if int64(len(body)) > client.maxResponseBytes {
		return response, errCaddyAdminResponseTooLarge
	}

	response.Body = body
	return response, nil
}

func (client *caddyAdminClient) Close() {
	if client == nil || client.transport == nil {
		return
	}
	client.transport.CloseIdleConnections()
}

func validateCaddyAdminRequestPath(requestPath string) (string, error) {
	if requestPath == "" ||
		strings.ContainsAny(requestPath, "\x00\r\n\t \\#?") {
		return "", fmt.Errorf(
			"%w: request path must be an absolute path without query or fragment",
			errCaddyAdminInvalidRequest,
		)
	}

	parsed, err := url.ParseRequestURI(requestPath)
	if err != nil ||
		parsed.IsAbs() ||
		parsed.Host != "" ||
		parsed.User != nil ||
		parsed.Opaque != "" ||
		parsed.RawQuery != "" ||
		parsed.ForceQuery ||
		parsed.Fragment != "" ||
		parsed.Path == "" ||
		!strings.HasPrefix(parsed.Path, "/") ||
		strings.HasPrefix(parsed.Path, "//") ||
		parsed.EscapedPath() != requestPath ||
		strings.ContainsAny(parsed.Path, "\x00\r\n\t \\#?") {
		return "", fmt.Errorf(
			"%w: request path is not a valid Caddy Admin API path",
			errCaddyAdminInvalidRequest,
		)
	}

	for _, segment := range strings.Split(parsed.Path, "/") {
		if segment == "." || segment == ".." {
			return "", fmt.Errorf(
				"%w: request path cannot contain dot segments",
				errCaddyAdminInvalidRequest,
			)
		}
	}

	return parsed.EscapedPath(), nil
}

func validateCaddyAdminRequest(
	method string,
	body []byte,
	ifMatch string,
) error {
	mutating := isCaddyAdminMutation(method)

	switch method {
	case http.MethodGet:
		if body != nil {
			return fmt.Errorf(
				"%w: GET requests cannot include a body",
				errCaddyAdminInvalidRequest,
			)
		}
	case http.MethodPost, http.MethodPatch:
		if len(body) == 0 {
			return fmt.Errorf(
				"%w: mutating JSON requests require a body",
				errCaddyAdminInvalidRequest,
			)
		}
	case http.MethodDelete:
		if body != nil {
			return fmt.Errorf(
				"%w: DELETE requests cannot include a body",
				errCaddyAdminInvalidRequest,
			)
		}
	default:
		return fmt.Errorf(
			"%w: unsupported HTTP method %q",
			errCaddyAdminInvalidRequest,
			method,
		)
	}

	if mutating {
		if ifMatch == "" || strings.ContainsAny(ifMatch, "\x00\r\n") {
			return fmt.Errorf(
				"%w: mutating requests require a valid If-Match ETag",
				errCaddyAdminInvalidRequest,
			)
		}
	} else if ifMatch != "" && strings.ContainsAny(ifMatch, "\x00\r\n") {
		return fmt.Errorf(
			"%w: If-Match contains invalid header characters",
			errCaddyAdminInvalidRequest,
		)
	}

	return nil
}

func isCaddyAdminMutation(method string) bool {
	switch method {
	case http.MethodPost, http.MethodPatch, http.MethodDelete:
		return true
	default:
		return false
	}
}
