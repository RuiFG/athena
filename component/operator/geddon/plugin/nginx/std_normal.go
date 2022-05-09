package nginx

import (
	"athena"
	"athena/component/operator/geddon/log_format/nginx"
	"fmt"
	"strings"
)

type stdLogFormat struct {
	Timestamp       int64
	Vendor          string
	Domain          string
	Httpcode        int
	Miss            bool
	Parent          bool
	IPType          string
	Scheme          string
	NodeVersionType int

	Traffic       int64
	Time_response int64
	Req_num       int
	PV            int

	UpstreamResponseLength int64
	UpstreamResponseTime   int64
}

type PluginSTD struct{}

func (p *PluginSTD) PluginName() string { return "std" }

func (p *PluginSTD) NodeType() interface{} { return &stdLogFormat{} }

func (p *PluginSTD) ID(log *nginx.LogExt) string {

	return fmt.Sprintln(log.MinutelyTime, log.Vendor, log.Domain, log.HttpCode, log.Miss, log.IsParent, log.NodeVersionType,
		log.NetIP.To4() == nil, strings.ToLower(log.ParsedUrl.Scheme))
}

func (p *PluginSTD) NeedCalculate(ctx athena.Context, log *nginx.LogExt) bool {
	return true
}

func (p *PluginSTD) NewStruct(log *nginx.LogExt) interface{} {
	res := &stdLogFormat{
		Timestamp:       log.MinutelyTime,
		Vendor:          log.Vendor,
		Domain:          log.Domain,
		Httpcode:        log.HttpCode,
		Miss:            log.Miss,
		Parent:          log.IsParent,
		IPType:          nginx.TypeV4,
		Scheme:          strings.ToLower(log.ParsedUrl.Scheme),
		NodeVersionType: log.NodeVersionType,
	}
	if log.NetIP.To4() == nil {
		res.IPType = nginx.TypeV6
	}
	return res
}

var PVContentType = []string{
	"text/html", "text/asp", "text/plain",
}

func (p *PluginSTD) Calculate(ctx athena.Context, log *nginx.LogExt, vari any) {

	v := vari.(*stdLogFormat)
	v.Traffic += log.SizeResponse
	v.Time_response += log.TimeResponse
	v.UpstreamResponseLength += log.UpstreamResponseLength
	v.UpstreamResponseTime += log.UpstreamResponseTime
	v.Req_num++

	if !log.IsParent {
		if (log.HttpCode <= 299 && log.HttpCode > 0) || log.HttpCode == 304 {
			for _, contentType := range PVContentType {
				if strings.HasPrefix(log.ContentType, contentType) {
					v.PV++
					break
				}
			}
		}
	}
}
