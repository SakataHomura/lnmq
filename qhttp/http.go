package qhttp

import (
	"crypto/tls"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"
)

func NewDeadlineTransport(connectTimeout time.Duration, requestTimeout time.Duration) *http.Transport {
	t := &http.Transport{
		DialContext: (&net.Dialer{
			Timeout:   connectTimeout,
			KeepAlive: 30 * time.Second,
			DualStack: true,
		}).DialContext,
		ResponseHeaderTimeout: requestTimeout,
		MaxIdleConns:          100,
		IdleConnTimeout:       90 * time.Second,
		TLSHandshakeTimeout:   10 * time.Second,
	}

	return t
}

func NewHttpClient(tlsConfig *tls.Config, connectTimeout time.Duration, requestTimeout time.Duration) *http.Client {
	t := NewDeadlineTransport(connectTimeout, requestTimeout)
	t.TLSClientConfig = tlsConfig

	return &http.Client{
		Transport: t,
		Timeout:   requestTimeout,
	}
}

func GetV1(client *http.Client, endpoint string, v interface{}) error {
	for {
		req, err := http.NewRequest("GET", endpoint, nil)
		if err != nil {
			return err
		}

		req.Header.Add("Accept", "application/vnd.nsq; version=1.0")

		resp, err := client.Do(req)
		if err != nil {
			return err
		}

		body, err := ioutil.ReadAll(resp.Body)
		resp.Body.Close()
		if err != nil {
			return err
		}

		if resp.StatusCode != 200 {
			if resp.StatusCode == 403 && !strings.HasPrefix(endpoint, "https") {
				endpoint, err = httpsEndpoint(endpoint, body)
				if err != nil {
					return err
				}
				continue
			} else {
				return fmt.Errorf("get response")
			}
		}

		err = json.Unmarshal(body, &v)
		if err != nil {
			return err
		}

		return nil
	}
}

func httpsEndpoint(endpoint string, body []byte) (string, error) {
	var resp struct {
		HttpsPort int `json:"https_port"`
	}

	err := json.Unmarshal(body, &resp)
	if err != nil {
		return "", err
	}

	u, err := url.Parse(endpoint)
	if err != nil {
		return "", err
	}

	host, _, err := net.SplitHostPort(u.Host)
	if err != nil {
		return "", err
	}

	u.Scheme = "https"
	u.Host = net.JoinHostPort(host, strconv.Itoa(resp.HttpsPort))

	return u.String(), nil
}
