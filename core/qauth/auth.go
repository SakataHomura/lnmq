package qauth

import (
    "fmt"
    "net/url"
    "regexp"
    "time"
)

type Auth struct {
    Topic string
    Channels []string
    Permissions []string
}

type AuthState struct {
    TTL int
    Auths []Auth
    Identify string
    Expires time.Time
}

func (a *Auth) HasPermission(permission string) bool {
    for _, p := range a.Permissions {
        if p == permission {
            return true
        }
    }

    return false
}

func (a *Auth) IsAllowed(topic, channel string) bool {
    if channel != "" {
        if !a.HasPermission("subscribe") {
            return false
        }
    } else {
        if !a.HasPermission("publish") {
            return false
        }
    }

    r := regexp.MustCompile(a.Topic)

    if !r.MatchString(topic) {
        return false
    }

    for _, c := range a.Channels {
        cr := regexp.MustCompile(c)
        if cr.MatchString(channel) {
            return true
        }
    }

    return false
}

func (s *AuthState) IsAllowed(topic, channel string) bool {
    for _, aa := range s.Auths{
        if aa.IsAllowed(topic, channel) {
            return true
        }
    }

    return false
}

func (s *AuthState) IsExpired() bool {
    if s.Expires.Before(time.Now()) {
        return true
    }

    return false
}

func QueryAnyAuthd(authd []string, remoteIp string, tlsEnabled bool, commonName string,
    authSecret string, connectTimeout time.Duration, requestTimeout time.Duration) (*AuthState, error) {
    for _, a := range authd {
        as, err := QueryAuthd(a, remoteIp, tlsEnabled, commonName, authSecret, connectTimeout, requestTimeout)
        if err != nil {
            continue
        }

        return as, nil
    }

    return nil, fmt.Errorf("unable to query auth")
}

func QueryAuthd(authd string, remoteIp string, tlsEnabled bool, commonName string,
    authSecret string, connectTimeout time.Duration, requestTimeout time.Duration) (*AuthState, error) {
    v := url.Values{}
    v.Set("remote_ip", remoteIp)
    if tlsEnabled {
        v.Set("tls", "true")
    } else {
        v.Set("tls", "false")
    }
    v.Set("secret", authSecret)
    v.Set("common_name", commonName)

    //endpoint := fmt.Sprintf("http://%s/auth?%s", authd, v.Encode())

    var as AuthState

    for _, auth := range as.Auths {
        for _, p := range auth.Permissions {
            if p != "subscribe" && p != "publish" {
                return nil, fmt.Errorf("unknown permission")
            }
        }

        if _, err := regexp.Compile(auth.Topic); err != nil {
            return nil, fmt.Errorf("unable to compile topic")
        }

        for _, c := range auth.Channels {
            if _, err := regexp.Compile(c); err != nil {
                return nil, fmt.Errorf("unable to compile channel")
            }
        }
    }

    if as.TTL <= 0 {
        return nil, fmt.Errorf("invalid TTL")
    }

    as.Expires = time.Now().Add(time.Duration(as.TTL) * time.Second)

    return &as, nil
}