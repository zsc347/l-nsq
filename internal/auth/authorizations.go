package auth

import (
	"errors"
	"fmt"
	"log"
	"net/url"
	"regexp"
	"time"

	"github.com/l-nsq/internal/http_api"
)

// permision [subscribe, publish]

// Authorization describes permisstions for topic and channels
type Authorization struct {
	Topic       string   `json:"topic"`
	Channels    []string `json:"channels"`
	Permissions []string `json:"permissions"`
}

// State describes current authrozation state
type State struct {
	TTL            int             `json:"ttl"`
	Authorizations []Authorization `json:"authorizations"`
	Identity       string          `json:"identity"`
	IdentityURL    string          `json:"identity_url"`
	Expires        time.Time
}

// HasPermission checks whether authorization contains a specified permission
func (a *Authorization) HasPermission(permission string) bool {
	for _, p := range a.Permissions {
		if permission == p {
			return true
		}
	}
	return false
}

// IsAllowed checks whether allow to access a channel of a topic
// channel name empty string used for check for publish
// otherwise means check for subscribe
func (a *Authorization) IsAllowed(topic, channel string) bool {
	if channel != "" {
		if !a.HasPermission("subscribe") {
			return false
		}
	} else {
		if !a.HasPermission("publish") {
			return false
		}
	}

	topicRegex := regexp.MustCompile(a.Topic)

	if !topicRegex.MatchString(topic) {
		return false
	}

	for _, c := range a.Channels {
		channelRegex := regexp.MustCompile(c)
		if channelRegex.MatchString(channel) {
			return true
		}
	}

	return false
}

// IsAllowed checks whether allow to access a channel of a topic
func (a *State) IsAllowed(topic, channel string) bool {
	for _, aa := range a.Authorizations {
		if aa.IsAllowed(topic, channel) {
			return true
		}
	}
	return false
}

// IsExpired returns whether state already expired
func (a *State) IsExpired() bool {
	if a.Expires.Before(time.Now()) {
		return true
	}
	return false
}

// QueryAnyAuthd get authorization data for a list of auth servers
func QueryAnyAuthd(authd []string, remoteIP, tlsEnabled, authSecred string,
	connectTimeout time.Duration, requestTimeout time.Duration) (*State, error) {
	for _, a := range authd {
		state, err := QueryAuthd(a, remoteIP, tlsEnabled, authSecred, connectTimeout, requestTimeout)
		if err != nil {
			log.Printf("Error: failed auth against %s %s", a, err)
			continue
		}
		return state, err
	}
	return nil, errors.New("Unable to access auth server")
}

// QueryAuthd get authorization data from a auth server
func QueryAuthd(authd, remoteIP, tlsEnabled, authSecred string,
	connectTimeout time.Duration, requestTimeout time.Duration) (*State, error) {
	v := url.Values{}
	v.Set("remote_ip", remoteIP)
	v.Set("tls", tlsEnabled)
	v.Set("secret", authSecred)

	endpoint := fmt.Sprintf("http://%s/auth?%s", authd, v.Encode())

	var authState State
	client := http_api.NewClient(nil, connectTimeout, requestTimeout)
	if err := client.GETV1(endpoint, &authState); err != nil {
		return nil, err
	}

	// validation on response
	for _, auth := range authState.Authorizations {
		for _, p := range auth.Permissions {
			switch p {
			case "subscribe", "publish":
			default:
				return nil, fmt.Errorf("unknown permission  %s", p)
			}
		}

		if _, err := regexp.Compile(auth.Topic); err != nil {
			return nil, fmt.Errorf("unable to compile topic %q %s", auth.Topic, err)
		}

		for _, channel := range auth.Channels {
			if _, err := regexp.Compile(channel); err != nil {
				return nil, fmt.Errorf("unable to compile channel %q %s", channel, err)
			}
		}
	}

	if authState.TTL <= 0 {
		return nil, fmt.Errorf("invalid TTL %d (must be > 0)", authState.TTL)
	}

	authState.Expires = time.Now().Add(time.Duration(authState.TTL) * time.Second)
	return &authState, nil
}
