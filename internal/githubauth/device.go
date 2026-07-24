// Package githubauth implements GitHub's OAuth device flow — the `gh auth
// login` experience: print a short code, the user enters it in a browser,
// deckhand receives a token. No PAT creation, nothing pasted into files.
package githubauth

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"time"
)

// DefaultClientID is deckhand's public OAuth app client ID. OAuth client IDs
// are not secrets (gh ships its own the same way). Maintainers: register an
// OAuth app (Settings → Developer settings → OAuth Apps, enable Device Flow,
// no callback needed) and set this — until then, device login requires
// passing a client ID explicitly (--oauth-client-id / DECKHAND_OAUTH_CLIENT_ID).
const DefaultClientID = "Ov23liUpBIqsqcNgKWaF"

const defaultBaseURL = "https://github.com"

// Flow drives one device authorization. BaseURL is overridable for tests.
type Flow struct {
	ClientID string
	// Scopes: "repo" covers repo-level scale sets; org-level needs "admin:org".
	Scopes  []string
	BaseURL string
	// Prompt receives the user-facing instruction (code + URL) once available.
	Prompt func(userCode, verificationURI string)

	http *http.Client
	// minPoll floors the polling interval (default 5s per GitHub's docs);
	// tests shrink it.
	minPoll time.Duration
}

type DeviceCode struct {
	DeviceCode      string `json:"device_code"`
	UserCode        string `json:"user_code"`
	VerificationURI string `json:"verification_uri"`
	ExpiresIn       int    `json:"expires_in"`
	Interval        int    `json:"interval"`
}

// Run performs the whole flow and returns the access token. It blocks until
// the user approves, the code expires, or ctx ends.
func (f *Flow) Run(ctx context.Context) (string, error) {
	if f.ClientID == "" {
		return "", errors.New("no OAuth client ID configured (set --oauth-client-id or DECKHAND_OAUTH_CLIENT_ID; see README)")
	}
	if f.BaseURL == "" {
		f.BaseURL = defaultBaseURL
	}
	if f.http == nil {
		f.http = &http.Client{Timeout: 15 * time.Second}
	}

	dc, err := f.requestDeviceCode(ctx)
	if err != nil {
		return "", err
	}
	if f.Prompt != nil {
		f.Prompt(dc.UserCode, dc.VerificationURI)
	}

	if f.minPoll == 0 {
		f.minPoll = 5 * time.Second
	}
	interval := max(time.Duration(dc.Interval)*time.Second, f.minPoll)
	deadline := time.Now().Add(time.Duration(dc.ExpiresIn) * time.Second)
	for {
		select {
		case <-ctx.Done():
			return "", ctx.Err()
		case <-time.After(interval):
		}
		if time.Now().After(deadline) {
			return "", errors.New("device code expired before approval — run init again")
		}
		token, retry, err := f.pollToken(ctx, dc.DeviceCode, &interval)
		if err != nil {
			return "", err
		}
		if !retry {
			return token, nil
		}
	}
}

func (f *Flow) requestDeviceCode(ctx context.Context) (*DeviceCode, error) {
	var dc DeviceCode
	err := f.post(ctx, "/login/device/code", url.Values{
		"client_id": {f.ClientID},
		"scope":     {strings.Join(f.Scopes, " ")},
	}, &dc)
	if err != nil {
		return nil, fmt.Errorf("request device code: %w", err)
	}
	if dc.DeviceCode == "" || dc.UserCode == "" {
		return nil, errors.New("request device code: empty response (is the OAuth app's device flow enabled?)")
	}
	return &dc, nil
}

// pollToken returns (token, false, nil) on success, ("", true, nil) when the
// user hasn't approved yet, and an error for terminal failures.
func (f *Flow) pollToken(ctx context.Context, deviceCode string, interval *time.Duration) (string, bool, error) {
	var res struct {
		AccessToken string `json:"access_token"`
		Error       string `json:"error"`
		ErrorDesc   string `json:"error_description"`
	}
	err := f.post(ctx, "/login/oauth/access_token", url.Values{
		"client_id":   {f.ClientID},
		"device_code": {deviceCode},
		"grant_type":  {"urn:ietf:params:oauth:grant-type:device_code"},
	}, &res)
	if err != nil {
		return "", false, fmt.Errorf("poll token: %w", err)
	}
	switch res.Error {
	case "":
		if res.AccessToken == "" {
			return "", false, errors.New("poll token: empty access token")
		}
		return res.AccessToken, false, nil
	case "authorization_pending":
		return "", true, nil
	case "slow_down":
		*interval += 5 * time.Second
		return "", true, nil
	default: // access_denied, expired_token, incorrect_device_code, ...
		msg := res.Error
		if res.ErrorDesc != "" {
			msg += ": " + res.ErrorDesc
		}
		return "", false, errors.New(msg)
	}
}

func (f *Flow) post(ctx context.Context, path string, form url.Values, out any) error {
	if f.http == nil {
		f.http = &http.Client{Timeout: 15 * time.Second}
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, f.BaseURL+path, strings.NewReader(form.Encode()))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.Header.Set("Accept", "application/json")
	resp, err := f.http.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("%s: HTTP %d", path, resp.StatusCode)
	}
	return json.NewDecoder(resp.Body).Decode(out)
}
