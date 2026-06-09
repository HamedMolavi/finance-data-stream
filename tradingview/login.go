package tradingview

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"time"
)

// UserSession is the returned information from the login endpoint.
type UserSession struct {
	ID             int64     `json:"id"`
	Username       string    `json:"username"`
	FirstName      string    `json:"firstName"`
	LastName       string    `json:"lastName"`
	Reputation     int       `json:"reputation"`
	Following      int       `json:"following"`
	Followers      int       `json:"followers"`
	Notifications  int       `json:"notifications"`
	Session        string    `json:"session"`
	Signature      string    `json:"signature"`
	SessionHash    string    `json:"sessionHash"`
	PrivateChannel string    `json:"privateChannel"`
	AuthToken      string    `json:"authToken"`
	JoinDate       time.Time `json:"joinDate"`
}

// rawResponse matches the shape returned by TradingView (as used in your JS).
// Fields use the JSON names we expect from the API.
type rawResponse struct {
	Error string `json:"error"`
	User  struct {
		ID                int64  `json:"id"`
		Username          string `json:"username"`
		FirstName         string `json:"first_name"`
		LastName          string `json:"last_name"`
		Reputation        int    `json:"reputation"`
		Following         int    `json:"following"`
		Followers         int    `json:"followers"`
		NotificationCount int    `json:"notification_count"`
		SessionHash       string `json:"session_hash"`
		PrivateChannel    string `json:"private_channel"`
		AuthToken         string `json:"auth_token"`
		DateJoined        string `json:"date_joined"`
	} `json:"user"`
}

// LoginUser logs into TradingView and returns a UserSession.
// If UA is empty, defaults to "TWAPI/3.0". If remember is true, sends remember=on.
func LoginUser() (string, error) {
	const username = "alisani0961@gmail.com"
	const password = "@Alisani8018000"
	form := url.Values{}
	form.Set("username", username)
	form.Set("password", password)
	form.Set("remember", "on")
	endpoint := "https://www.tradingview.com/accounts/signin/"

	req, err := http.NewRequest("POST", endpoint, bytes.NewBufferString(form.Encode()))
	if err != nil {
		// fmt.Println(err)
		return "", err
	}

	req.Header.Set("Cache-Control", "no-cache")
	req.Header.Set("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/83.0.4103.116 Safari/537.36")
	req.Header.Set("Pragma", "no-cache")
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.Header.Set("Referer", "https://www.tradingview.com")

	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		// fmt.Println(err)
		return "", err
	}
	defer resp.Body.Close()

	// Non-2xx status codes — read body for debugging and return error
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		var b bytes.Buffer
		_, _ = b.ReadFrom(resp.Body)
		// fmt.Println(resp.StatusCode, strings.TrimSpace(b.String()))
		return "", fmt.Errorf("login request failed: status %d: %s", resp.StatusCode, strings.TrimSpace(b.String()))
	}

	var r rawResponse
	if err := json.NewDecoder(resp.Body).Decode(&r); err != nil {
		// fmt.Println(err)
		return "", err
	}

	if r.Error != "" {
		// fmt.Println(r.Error)
		return "", errors.New(r.Error)
	}
	// fmt.Println(r)
	return r.User.AuthToken, nil
}
