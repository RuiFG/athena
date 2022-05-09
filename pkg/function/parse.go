package function

import (
	"github.com/pkg/errors"
	"strings"
)

func ParseLog(raw string) (map[string]interface{}, error) {
	trimSpace := strings.TrimSpace(raw)
	splitN := strings.SplitN(trimSpace, " ", 2)
	if len(splitN) != 2 {
		return nil, errors.New("illegal field count")
	}
	tmp, err := ParseEdgeLog(splitN[1])
	if err != nil {
		return nil, err
	}
	tmp["host"] = splitN[0]
	return tmp, nil
}

func ParseEdgeLog(raw string) (map[string]interface{}, error) {
	trimSpace := strings.TrimSpace(raw)
	splitN := strings.SplitN(trimSpace, " ", 24)
	if len(splitN) != 24 {
		return nil, errors.New("illegal field count")
	}
	return map[string]interface{}{
			"client_ip":        splitN[0],
			"domain":           splitN[1],
			"content_type":     splitN[2],
			"request_time":     splitN[3],
			"request_timezone": splitN[4],
			"request_method":   splitN[5],
			"request_url":      splitN[6],
			"request_version":  splitN[7],
			"http_code":        splitN[8],
			"response_size":    splitN[9],
			"refer":            splitN[10],
			"ua":               splitN[11],
			"response_time":    splitN[12],
			"size_content":     splitN[13],
			"content_length":   splitN[14],
			"range":            splitN[15],
			"x_forwarded_for":  splitN[16],
			"local_source":     splitN[17],
			"last_modified":    splitN[18],
			"parent":           splitN[19],
			"size_package":     splitN[20],
			"squid_status":     splitN[21],
			"squid_proxy_pass": splitN[22],
			"ext":              splitN[23],
		},
		nil
}
