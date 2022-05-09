package nginx

import (
	"github.com/pkg/errors"
	"net"
	"net/url"
	"path"
	"strconv"
	"strings"
	"time"
)

const (
	TypeV4 = "v4"
	TypeV6 = "v6"
)

type LogRaw struct {
	IP              string
	Domain          string
	ContentType     string
	Time            time.Time //raw
	Method          string
	Url             string
	HttpVersion     string
	HttpCode        int
	SizeResponse    int64
	Referer         string
	UserAgent       string
	TimeResponse    int64
	SizeContent     int64
	Range           string
	XForwardedFor   string
	LocalSourcePort string
	UpstreamInfo    string
	XPeer           string
	StatusSquid     string
	Parent          string
	Ext             []string

	Splits []string
	Raw    string

	Alias map[string]*string
}

func (p *LogRaw) Get(dollar int, field int, separator string, as string) string {
	if p.Alias[as] != nil {
		return *p.Alias[as]
	}
	ps := strings.Split(p.Splits[dollar-1], separator)
	if len(ps) < field {
		result := ""
		p.Alias[as] = &result
		return result
	}
	p.Alias[as] = &ps[field-1]
	return ps[field-1]
}

func SplitBySpace(raw string, maxPart int, ch byte) []string {
	spaces := make([]int, maxPart)
	splites := make([]string, maxPart+1)
	p := 0
	for i := 0; i < len(raw) && p < maxPart; i++ {
		if raw[i] == ch {
			spaces[p] = i
			p++
		}
	}

	lst := 0
	for i := 0; i < p; i++ {
		splites[i] = raw[lst:spaces[i]]
		lst = spaces[i] + 1
	}
	splites[p] = raw[lst:]

	return splites[:p+1]
}

func parseInt64(s string) (int64, error) {
	x := int64(0)
	for _, c := range s {
		if c < '0' || c > '9' {
			return 0, errors.New(string(s) + " is not int, when parseInt64")
		}
		x = x*10 + int64(c) - 48
	}

	return x, nil
}

func NewLogRaw(logRecord string) (*LogRaw, error) {
	logRecord = strings.TrimSpace(logRecord)
	//tmp := strings.Split(logRecord, " ")
	tmp := SplitBySpace(logRecord, 24, ' ')

	if len(tmp) != 24 {
		return nil, errors.New("illegal field count")
	}

	//if _ip := net.ParseIP(tmp[0]); _ip == nil {
	//	return nil, errors.New("illegal ip : " + tmp[0])
	//}

	tm, err := time.Parse("[02/Jan/2006:15:04:05 -0700]", tmp[3]+" "+tmp[4])
	if err != nil {
		return nil, errors.New("illegal time format: " + tmp[3] + " " + tmp[4])
	}

	httpCode, err := strconv.Atoi(tmp[8])
	if err != nil {
		return nil, errors.New("illegal httpCode format: " + tmp[8])
	}
	SizeResponse, err := parseInt64(tmp[9])
	if err != nil {
		return nil, errors.New("illegal SizeResponse format: " + tmp[9])
	}
	TimeResponse, err := parseInt64(tmp[12])
	if err != nil {
		return nil, errors.New("illegal TimeResponse format: " + tmp[12])
	}
	SizeContent, err := parseInt64(tmp[13])
	if err != nil {
		return nil, errors.New("illegal SizeContent format: " + tmp[13])
	}

	exts := strings.Split(tmp[23], "@_@")

	return &LogRaw{
		tmp[0], tmp[1], strings.Trim(tmp[2], "\""), //ip //domain //content-type
		tm,
		strings.Trim(tmp[5], "\""), tmp[6], strings.Trim(tmp[7], "\""), //method //url //http-version
		httpCode,
		SizeResponse,
		tmp[10], tmp[11], //referer  //ua
		TimeResponse,
		SizeContent,
		tmp[15], tmp[16], tmp[17], //range //x forward port //local source port
		tmp[18], tmp[19], // squid_info // parent
		tmp[21], tmp[22], exts, //status-squid //squid proxypass //ext
		tmp, logRecord, map[string]*string{},
	}, nil
}

type HTTPSPart struct {
	SSLHandshake      bool
	SSLSessionReuse   bool
	SSLHandshakeTime  int64
	SSLHandshakeBytes int64
	SSLProtocol       string
	SSLCipher         string
	SupportSNI        bool
	HTTP2             bool
}

type LogExt struct {
	MinutelyTime    int64 //truncate with minute
	NetIP           net.IP
	Vendor          string
	NodeVersionType int

	Isp      string
	Province string
	Country  string

	Dirs     []string
	Filename string
	FirstDir string
	BaseName string //without extname
	ExtName  string

	UpstreamResponseLength int64
	UpstreamResponseTime   int64

	HTTPS        *HTTPSPart
	IsParent     bool
	Miss         bool
	MissInternal bool

	ParsedUrl *url.URL
	DstIPStr  string

	*LogRaw
}

func IpConvert(ipraw string) (ip int64) {
	if strings.Contains(ipraw, ":") {
		return -666
	}
	tmp := strings.Split(ipraw, ".")
	if len(tmp) != 4 {
		return -1
	}
	for i := 0; i < 4; i++ {
		cur, err := parseInt64(tmp[i])
		if err != nil || cur < 0 || cur > 255 {
			return -1
		}
		ip = ip*256 + cur
	}
	return ip
}

func splitExt(file string) (filename string, ext string) {
	if file == "" {
		return "", ""
	}
	i := strings.LastIndex(file, ".")
	if i < 0 {
		return file, ""
	}
	return file[:i], file[i+1:]
}

func SplitAll(fullPath string) (file string, filename string, ext string, Dirs []string, first_dir string) {
	var Dir string

	Dir, file = path.Split(fullPath)
	Dir = path.Clean(Dir)
	filename, ext = splitExt(file)
	ext = strings.ToLower(ext)

	paths := strings.Split(Dir, "/")

	if len(paths) >= 2 && len(paths[1]) > 0 {
		first_dir = paths[1] + "/"
	}

	Dirs = paths[1:]
	return
}

func AddHTTPS(log *LogExt) {
	if log.ParsedUrl.Scheme != "https" {
		return
	}

	localSourcePortStaff := strings.Split(log.LocalSourcePort, "@")
	if len(localSourcePortStaff) < 16 { // missing https info
		return
	}

	log.HTTPS = &HTTPSPart{}
	if sslSessionReused := localSourcePortStaff[9]; sslSessionReused == "r" {
		log.HTTPS.SSLSessionReuse = true
	}
	log.HTTPS.SSLProtocol = localSourcePortStaff[10]
	log.HTTPS.SSLCipher = localSourcePortStaff[11]
	if SSLServerName := localSourcePortStaff[12]; SSLServerName != "" {
		log.HTTPS.SupportSNI = true
	}
	if SSLHandshakeBytes, err := strconv.ParseInt(localSourcePortStaff[13], 10, 64); err == nil {
		log.HTTPS.SSLHandshakeBytes = SSLHandshakeBytes
	}
	if SSLHandshakeTime, err := strconv.ParseFloat(localSourcePortStaff[14], 64); err == nil {
		log.HTTPS.SSLHandshakeTime = int64(SSLHandshakeTime * 1000)
	}
	if SSLHandshake := localSourcePortStaff[15]; SSLHandshake == "1" {
		log.HTTPS.SSLHandshake = true
	}

	log.HTTPS.HTTP2 = log.HttpVersion == "HTTP/2.0"
}

func NewLogExt(log string, vendor string, nodeVersionType int) (*LogExt, error) {
	lograw, err := NewLogRaw(log)
	if err != nil {
		return nil, err
	}

	netIP := net.ParseIP(lograw.IP)
	if netIP == nil {
		return nil, errors.New("illegal ip address : " + lograw.IP)
	}
	//TODO ipdb
	isp, province, country := "", "", ""

	parsed_url, err := url.Parse(lograw.Url)
	if err != nil {
		return nil, errors.New("illegal url format : " + lograw.Url)
	}

	isParent := lograw.XPeer != `"-"`
	miss := strings.Contains(lograw.StatusSquid, "MISS")
	miss_internal := lograw.Parent != "NONE"

	upstreamResponseLength, upstreamResponseTime := int64(0), int64(0)

	upstreams := strings.Split(lograw.UpstreamInfo, "@")
	if len(upstreams) >= 10 {
		upstreamResponseLength, err = strconv.ParseInt(upstreams[9], 10, 64)
		if err != nil {
			upstreamResponseLength = 0
		}

		upstreamResponseTimeFloat, err := strconv.ParseFloat(upstreams[4], 64)
		if err != nil {
			upstreamResponseTime = 0
		} else {
			upstreamResponseTime = int64(upstreamResponseTimeFloat * 1000)
		}
	}

	var dstIPStr string
	sp := strings.SplitN(lograw.LocalSourcePort, "@", 3)
	if len(sp) > 1 {
		sp = strings.Split(sp[1], ":")
		if len(sp) > 1 {
			dstIPStr = sp[0]
		}
	}

	file, filename, ext, dir, first_dir := SplitAll(parsed_url.Path)
	logExt := &LogExt{
		MinutelyTime: lograw.Time.Truncate(time.Minute).Unix(), NetIP: netIP, Vendor: vendor, NodeVersionType: nodeVersionType, Isp: isp, Province: province, Country: country,
		Dirs: dir, Filename: file, FirstDir: first_dir, BaseName: filename, ExtName: ext, UpstreamResponseLength: upstreamResponseLength,
		UpstreamResponseTime: upstreamResponseTime, IsParent: isParent, Miss: miss, MissInternal: miss_internal, ParsedUrl: parsed_url, DstIPStr: dstIPStr, LogRaw: lograw,
	}
	AddHTTPS(logExt)
	return logExt, nil
}
