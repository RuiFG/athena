package function

import (
	"fmt"
	"strconv"
	"strings"
)

var (
	ErrIllegalFieldCount = fmt.Errorf("illegal field count")
)

func ParseOrigin(raw string) (map[string]any, error) {
	trimSpace := strings.TrimSpace(raw)
	splitN := strings.SplitN(trimSpace, " ", 2)
	if len(splitN) != 2 {
		return nil, ErrIllegalFieldCount
	}
	tmp, err := ParseEdgeOrigin(splitN[1])
	if err != nil {
		return nil, err
	}
	tmp["host"] = splitN[0]
	return tmp, nil
}

func ParseEdgeOrigin(raw string) (map[string]any, error) {
	trimSpace := strings.TrimSpace(raw)
	splitN := strings.SplitN(trimSpace, " ", 24)
	if len(splitN) != 24 {
		return nil, ErrIllegalFieldCount
	}
	httpCode, err := strconv.ParseInt(splitN[8], 0, 0)
	if err != nil {
		httpCode = 0
	}
	bytesSent, err := strconv.ParseInt(splitN[9], 0, 0)
	if err != nil {
		bytesSent = 0
	}

	responseTime, err := strconv.ParseInt(splitN[12], 0, 0)
	if err != nil {
		responseTime = 0
	}
	bodyBytesSent, err := strconv.ParseInt(splitN[13], 0, 0)
	if err != nil {
		bodyBytesSent = 0
	}

	contentLength, err := strconv.ParseInt(splitN[14], 0, 0)
	if err != nil {
		contentLength = 0
	}
	return map[string]any{
			"client_ip":       splitN[0],
			"domain":          splitN[1],
			"content_type":    splitN[2][1 : len(splitN[2])-1],
			"request_time":    (splitN[3] + splitN[4])[1 : len(splitN[3])+len(splitN[3])-1],
			"http_method":     splitN[5][1:],
			"url":             splitN[6],
			"http_version":    splitN[7][:len(splitN[7])-1],
			"http_code":       httpCode,
			"bytes_sent":      bytesSent,
			"refer":           splitN[10][1 : len(splitN[10])-1],
			"ua":              splitN[11][1 : len(splitN[11])-1],
			"response_time":   responseTime,
			"body_bytes_sent": bodyBytesSent,
			"content_length":  contentLength,
			"range":           splitN[15][1 : len(splitN[15])-1],
			"x_forwarded_for": splitN[16][1 : len(splitN[16])-1],
			"complex_field0":  strings.Split(splitN[17], "@"),
			"complex_field1":  strings.Split(splitN[18], "@"),
			"x_peer":          splitN[19],
			"unnamed":         splitN[20],
			"hit":             splitN[21],
			"hierarchy":       splitN[22],
			"ext":             strings.Split(splitN[23], "@_@"),
		},
		nil
}
