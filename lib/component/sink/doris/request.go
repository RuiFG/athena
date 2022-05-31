package doris

import (
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"net/http/httptrace"
	"net/textproto"
	"sync"
	"time"

	"github.com/sirupsen/logrus"
)

const formatUrlTemp = `%s://%s/api/%s/%s/%s`

var (
	ErrConfigHostEmpty   = errors.New("host is empty")
	ErrConfigDBNameEmpty = errors.New("db name is empty")
)

type Option func(req *request)

type LoadConfig struct {
	IsHttps   bool
	Host      string
	DBName    string
	TableName string
	User      string
	Password  string
	Timeout   time.Duration

	ShowDebug bool
}

func (lc *LoadConfig) check() error {
	if len(lc.Host) == 0 {
		return ErrConfigHostEmpty
	}
	if len(lc.DBName) == 0 {
		return ErrConfigDBNameEmpty
	}
	if len(lc.User) == 0 {
		lc.User = "root"
		lc.printf("user default is root")
	}
	return nil
}
func (lc *LoadConfig) getScheme() string {
	if lc.IsHttps {
		return "https"
	}
	return "http"
}
func (lc *LoadConfig) printf(format string, a ...interface{}) {
	if lc.ShowDebug {
		fmt.Printf("doris# "+format+"\r\n", a...)
	}

}
func (lc *LoadConfig) url(action string) string {
	return fmt.Sprintf(formatUrlTemp, lc.getScheme(), lc.Host, lc.DBName, lc.TableName, action)
}
func (lc *LoadConfig) urlLabel(label, action string) string {
	return fmt.Sprintf(formatUrlTemp, lc.getScheme(), lc.Host, lc.DBName, label, action)
}

type request struct {
	config  LoadConfig
	header  map[string]string
	getBody func() (io.ReadCloser, error)
}

func NewRequest(config LoadConfig) *request {
	req := &request{config: config}
	req.header = make(map[string]string)
	return req

}
func (rq *request) setOptions(options ...Option) {
	for _, v := range options {
		v(rq)
	}
}

var mtx = new(sync.Mutex)
var pool = new(sync.Map)

type transport struct {
	Reqs *sync.Map
	http.RoundTripper
}

func (t *transport) RoundTrip(req *http.Request) (*http.Response, error) {
	if label := req.Header["Label"]; len(label) > 0 && label[0] != "" {
		if _, ok := t.Reqs.LoadOrStore(label[0], req); !ok {
			defer t.Reqs.Delete(label[0])
		} else {
			logrus.WithField("label", label[0]).Info("label exists")
		}
	}
	return t.RoundTripper.RoundTrip(req)
}

var defaultTrans = http.DefaultTransport.(*http.Transport)

func (rq *request) getHttpClient() *http.Client {
	i, ok := pool.Load(rq.config)
	if ok {
		return i.(*http.Client)
	}
	mtx.Lock()
	defer mtx.Unlock()
	trans := defaultTrans.Clone()
	trans.DisableKeepAlives = true
	c := &http.Client{
		Transport: &transport{RoundTripper: trans, Reqs: new(sync.Map)},
		CheckRedirect: func(req *http.Request, via []*http.Request) error {
			if len(via) >= 10 {
				return errors.New("stopped after 10 redirects")
			}
			req.SetBasicAuth(rq.config.User, rq.config.Password)
			return nil
		},
		Timeout: rq.config.Timeout,
	}
	pool.Store(rq.config, c)
	return c
}

type lfm = logrus.Fields

func (rq *request) httpRequest(method string, urlValue string, body io.Reader) (res []byte, err error) {

	var httpReq *http.Request
	var httpRes *http.Response

	httpReq, err = http.NewRequest(method, urlValue, body)
	if err != nil {
		return
	}
	httpReq.Header.Set("Expect", "100-continue")

	for k, v := range rq.header {
		httpReq.Header.Set(k, v)
	}
	httpReq.SetBasicAuth(rq.config.User, rq.config.Password)
	httpReq.GetBody = rq.getBody

	rq.config.printf("url: %s  header: %+v", urlValue, httpReq.Header)
	client := rq.getHttpClient()
	if rq.config.ShowDebug {
		m := client.Transport.(*transport).Reqs
		label := rq.header["label"]
		log := func(f lfm, message string) {
			if f == nil {
				f = lfm{"label": label}
			} else {
				f["label"] = label
			}
			i, ok := m.Load(label)
			if ok {
				header := i.(*http.Request).Header
				f["Label"] = header["Label"]
				if f["Headers"] != nil {
					f["Headers"] = header
				}
			} else {
				f["Label"] = "labeled-request-not-found"
			}
			logrus.WithFields(f).Info(message)
		}
		trace := &httptrace.ClientTrace{
			GetConn:              func(hostPort string) { log(lfm{"hostPort": hostPort}, "GetConn") },
			GotConn:              func(info httptrace.GotConnInfo) { log(lfm{"info": info}, "GotConnInfo") },
			PutIdleConn:          func(err error) { log(lfm{"error": err}, "PutIdleConn") },
			GotFirstResponseByte: func() { log(nil, "GotFirstResponseByte") },
			Got100Continue:       func() { log(nil, "Got100Continue") },
			Got1xxResponse: func(code int, header textproto.MIMEHeader) error {
				log(lfm{"code": code, "header": header}, "Got1xxResponse")
				return nil
			},
			ConnectStart: func(network, addr string) { log(lfm{"net": network, "addr": addr}, "ConnectStart") },
			ConnectDone: func(network, addr string, err error) {
				log(lfm{"network": network, "addr": addr, "error": err}, "ConnectDone")
			},
			WroteHeaders:    func() { log(lfm{"Headers": ""}, "WroteHeaders") },
			Wait100Continue: func() { log(nil, "Wait100Continue") },
			WroteRequest:    func(info httptrace.WroteRequestInfo) { log(lfm{"info": info}, "WroteRequest") },
		}
		httpReq = httpReq.WithContext(httptrace.WithClientTrace(httpReq.Context(), trace))
	}
	httpRes, err = client.Do(httpReq)
	if err != nil {
		return
	}
	if httpRes.Body != nil {
		defer httpRes.Body.Close()
		res, _ = ioutil.ReadAll(httpRes.Body)
	}

	if httpRes.StatusCode != 200 {
		err = errors.New(fmt.Sprintf("StatusCode: %d\r\nBody: %s", httpRes.StatusCode, string(res)))
		return
	}
	return
}

func WithGetBody(getBody func() (io.ReadCloser, error)) Option {
	return func(req *request) {
		req.getBody = getBody
	}
}

func WithTableName(tableName string) Option {
	return func(req *request) {
		req.config.TableName = tableName
	}
}

func WithDBName(dbName string) Option {
	return func(req *request) {
		req.config.DBName = dbName
	}
}
func WithCustomHeader(key, value string) Option {
	return func(req *request) {
		req.header[key] = value
	}
}
func WithColumnSeparator(columnSeparator string) Option {
	return func(req *request) {
		req.header["column_separator"] = columnSeparator
	}
}
func WithWhere(where string) Option {
	return func(req *request) {
		req.header["where"] = where
	}
}
func WithMaxFilterRatio(maxFilterRatio string) Option {
	return func(req *request) {
		req.header["max_filter_ratio"] = maxFilterRatio
	}
}

func WithColumns(columns string) Option {
	return func(req *request) {
		req.header["columns"] = columns
	}
}
func WithPartitions(partitions string) Option {
	return func(req *request) {
		req.header["partitions"] = partitions
	}
}

func WithLabel(label string) Option {
	return func(req *request) {
		req.header["label"] = label
	}
}
