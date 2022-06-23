package slack

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"mime"
	"mime/multipart"
	"net/http"
	"net/http/httputil"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/slack-go/slack/internal/misc"
)

// SlackResponse handles parsing out errors from the web api.
type SlackResponse struct {
	Ok               bool             `json:"ok"`
	Error            string           `json:"error"`
	ResponseMetadata ResponseMetadata `json:"response_metadata"`
}

func (t SlackResponse) Err() error {
	if t.Ok {
		return nil
	}

	// handle pure text based responses like chat.post
	// which while they have a slack response in their data structure
	// it doesn't actually get set during parsing.
	if strings.TrimSpace(t.Error) == "" {
		return nil
	}

	return errors.New(t.Error)
}

// RateLimitedError represents the rate limit respond from slack
type RateLimitedError struct {
	RetryAfter time.Duration
}

func (e *RateLimitedError) Error() string {
	return fmt.Sprintf("slack rate limit exceeded, retry after %s", e.RetryAfter)
}

func (e *RateLimitedError) Retryable() bool {
	return true
}

func fileUploadReq(ctx context.Context, path string, values url.Values, r io.Reader) (*http.Request, error) {
	req, err := http.NewRequest("POST", path, r)
	if err != nil {
		return nil, err
	}

	req = req.WithContext(ctx)
	req.URL.RawQuery = (values).Encode()
	return req, nil
}

func downloadFile(client httpClient, token string, downloadURL string, writer io.Writer, d Debug) error {
	if downloadURL == "" {
		return fmt.Errorf("received empty download URL")
	}

	req, err := http.NewRequest("GET", downloadURL, &bytes.Buffer{})
	if err != nil {
		return err
	}

	var bearer = "Bearer " + token
	req.Header.Add("Authorization", bearer)
	req.WithContext(context.Background())

	resp, err := client.Do(req)
	if err != nil {
		return err
	}

	defer resp.Body.Close()

	err = checkStatusCode(resp, d)
	if err != nil {
		return err
	}

	_, err = io.Copy(writer, resp.Body)

	return err
}

func formReq(endpoint string, values url.Values) (req *http.Request, err error) {
	if req, err = http.NewRequest("POST", endpoint, strings.NewReader(values.Encode())); err != nil {
		return nil, err
	}

	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	return req, nil
}

func jsonReq(endpoint string, body interface{}) (req *http.Request, err error) {
	buffer := bytes.NewBuffer([]byte{})
	if err = json.NewEncoder(buffer).Encode(body); err != nil {
		return nil, err
	}

	if req, err = http.NewRequest("POST", endpoint, buffer); err != nil {
		return nil, err
	}

	req.Header.Set("Content-Type", "application/json; charset=utf-8")
	return req, nil
}

func parseResponseBody(body io.ReadCloser, intf interface{}, d Debug) error {
	response, err := ioutil.ReadAll(body)
	if err != nil {
		return err
	}

	if d.Debug() {
		d.Debugln("parseResponseBody", string(response))
	}

	return json.Unmarshal(response, intf)
}

func postLocalWithMultipartResponse(ctx context.Context, client httpClient, method, fpath, fieldname, token string, values url.Values, intf interface{}, d Debug) error {
	fullpath, err := filepath.Abs(fpath)
	if err != nil {
		return err
	}
	file, err := os.Open(fullpath)
	if err != nil {
		return err
	}
	defer file.Close()

	return postWithMultipartResponse(ctx, client, method, filepath.Base(fpath), fieldname, token, values, file, intf, d)
}

func postWithMultipartResponse(ctx context.Context, client httpClient, path, name, fieldname, token string, values url.Values, r io.Reader, intf interface{}, d Debug) error {
	pipeReader, pipeWriter := io.Pipe()
	wr := multipart.NewWriter(pipeWriter)
	errc := make(chan error)
	go func() {
		defer pipeWriter.Close()
		ioWriter, err := wr.CreateFormFile(fieldname, name)
		if err != nil {
			errc <- err
			return
		}
		_, err = io.Copy(ioWriter, r)
		if err != nil {
			errc <- err
			return
		}
		if err = wr.Close(); err != nil {
			errc <- err
			return
		}
	}()
	req, err := fileUploadReq(ctx, path, values, pipeReader)
	if err != nil {
		return err
	}
	req.Header.Add("Content-Type", wr.FormDataContentType())
	req.Header.Set("Authorization", fmt.Sprintf("Bearer %s", token))
	req = req.WithContext(ctx)
	resp, err := client.Do(req)

	if err != nil {
		return err
	}
	defer resp.Body.Close()

	err = checkStatusCode(resp, d)
	if err != nil {
		return err
	}

	select {
	case err = <-errc:
		return err
	default:
		return newJSONParser(intf)(resp)
	}
}

func doPost(ctx context.Context, client httpClient, req *http.Request, parser responseParser, d Debug) error {
	req = req.WithContext(ctx)
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	err = checkStatusCode(resp, d)
	if err != nil {
		return err
	}

	return parser(resp)
}

// post JSON.
func postJSON(ctx context.Context, client httpClient, endpoint, token string, json []byte, intf interface{}, d Debug) error {
	reqBody := bytes.NewBuffer(json)
	req, err := http.NewRequest("POST", endpoint, reqBody)
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", fmt.Sprintf("Bearer %s", token))

	return doPost(ctx, client, req, newJSONParser(intf), d)
}

// post a url encoded form.
func postForm(ctx context.Context, client httpClient, endpoint string, values url.Values, intf interface{}, d Debug) error {
	reqBody := strings.NewReader(values.Encode())
	req, err := http.NewRequest("POST", endpoint, reqBody)
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	return doPost(ctx, client, req, newJSONParser(intf), d)
}

func getResource(ctx context.Context, client httpClient, endpoint, token string, values url.Values, intf interface{}, d Debug) error {
	req, err := http.NewRequest("GET", endpoint, nil)
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.Header.Set("Authorization", fmt.Sprintf("Bearer %s", token))

	req.URL.RawQuery = values.Encode()

	return doPost(ctx, client, req, newJSONParser(intf), d)
}

func parseAdminResponse(ctx context.Context, client httpClient, method string, teamName string, values url.Values, intf interface{}, d Debug) error {
	endpoint := fmt.Sprintf(WEBAPIURLFormat, teamName, method, time.Now().Unix())
	return postForm(ctx, client, endpoint, values, intf, d)
}

func logResponse(resp *http.Response, d Debug) error {
	if d.Debug() {
		text, err := httputil.DumpResponse(resp, true)
		if err != nil {
			return err
		}
		d.Debugln(string(text))
	}

	return nil
}

func okJSONHandler(rw http.ResponseWriter, r *http.Request) {
	rw.Header().Set("Content-Type", "application/json")
	response, _ := json.Marshal(SlackResponse{
		Ok: true,
	})
	rw.Write(response)
}

// timerReset safely reset a timer, see time.Timer.Reset for details.
func timerReset(t *time.Timer, d time.Duration) {
	if !t.Stop() {
		<-t.C
	}
	t.Reset(d)
}

func checkStatusCode(resp *http.Response, d Debug) error {
	if resp.StatusCode == http.StatusTooManyRequests {
		retry, err := strconv.ParseInt(resp.Header.Get("Retry-After"), 10, 64)
		if err != nil {
			return err
		}
		return &RateLimitedError{time.Duration(retry) * time.Second}
	}

	// Slack seems to send an HTML body along with 5xx error codes. Don't parse it.
	if resp.StatusCode != http.StatusOK {
		logResponse(resp, d)
		return misc.StatusCodeError{Code: resp.StatusCode, Status: resp.Status}
	}

	return nil
}

type responseParser func(*http.Response) error

func newJSONParser(dst interface{}) responseParser {
	return func(resp *http.Response) error {
		return json.NewDecoder(resp.Body).Decode(dst)
	}
}

func newTextParser(dst interface{}) responseParser {
	return func(resp *http.Response) error {
		b, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			return err
		}

		if !bytes.Equal(b, []byte("ok")) {
			return errors.New(string(b))
		}

		return nil
	}
}

func newContentTypeParser(dst interface{}) responseParser {
	return func(req *http.Response) (err error) {
		var (
			ctype string
		)

		if ctype, _, err = mime.ParseMediaType(req.Header.Get("Content-Type")); err != nil {
			return err
		}

		switch ctype {
		case "application/json":
			return newJSONParser(dst)(req)
		default:
			return newTextParser(dst)(req)
		}
	}
}
