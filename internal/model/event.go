package model

import "time"

// AdEvent 上报事件（Kafka 消息体 / DB 行）
type AdEvent struct {
	EventID      string            `json:"event_id"      ch:"event_id"`
	EventTime    time.Time         `json:"event_time"    ch:"event_time"`
	EventType    string            `json:"event_type"    ch:"event_type"`
	AdID         string            `json:"ad_id"         ch:"ad_id"`
	CampaignID   string            `json:"campaign_id"   ch:"campaign_id"`
	AdvertiserID string            `json:"advertiser_id" ch:"advertiser_id"`
	UserID       string            `json:"user_id"       ch:"user_id"`
	Device       string            `json:"device"        ch:"device"`
	OS           string            `json:"os"            ch:"os"`
	Region       string            `json:"region"        ch:"region"`
	City         string            `json:"city"          ch:"city"`
	Cost         float64           `json:"cost"          ch:"cost"`
	Revenue      float64           `json:"revenue"       ch:"revenue"`
	Extra        map[string]string `json:"extra,omitempty" ch:"-"`
}

// RealtimeOverview 实时总览
type RealtimeOverview struct {
	Window      string  `json:"window"      ch:"-"`
	Impressions uint64  `json:"impressions" ch:"impressions"`
	Clicks      uint64  `json:"clicks"      ch:"clicks"`
	Conversions uint64  `json:"conversions" ch:"conversions"`
	UV          uint64  `json:"uv"          ch:"uv"`
	Cost        float64 `json:"cost"        ch:"cost"`
	Revenue     float64 `json:"revenue"     ch:"revenue"`
	CTR         float64 `json:"ctr"         ch:"-"`
	CVR         float64 `json:"cvr"         ch:"-"`
	ROI         float64 `json:"roi"         ch:"-"`
	RPM         float64 `json:"rpm"         ch:"-"`
}

// HourlyTrend 小时趋势
type HourlyTrend struct {
	Hour        time.Time `json:"hour"        ch:"hour"`
	Impressions uint64    `json:"impressions" ch:"impressions"`
	Clicks      uint64    `json:"clicks"      ch:"clicks"`
	Conversions uint64    `json:"conversions" ch:"conversions"`
	UV          uint64    `json:"uv"          ch:"uv"`
	Cost        float64   `json:"cost"        ch:"cost"`
	Revenue     float64   `json:"revenue"     ch:"revenue"`
}

// CampaignStat 活动维度统计
type CampaignStat struct {
	CampaignID   string  `json:"campaign_id"   ch:"campaign_id"`
	CampaignName string  `json:"campaign_name" ch:"campaign_name"`
	Impressions  uint64  `json:"impressions"   ch:"impressions"`
	Clicks       uint64  `json:"clicks"        ch:"clicks"`
	Conversions  uint64  `json:"conversions"   ch:"conversions"`
	UV           uint64  `json:"uv"            ch:"uv"`
	Cost         float64 `json:"cost"          ch:"cost"`
	Revenue      float64 `json:"revenue"       ch:"revenue"`
	CTR          float64 `json:"ctr"           ch:"-"`
	CVR          float64 `json:"cvr"           ch:"-"`
	ROI          float64 `json:"roi"           ch:"-"`
}

// AdStat 广告维度
type AdStat struct {
	AdID        string  `json:"ad_id"       ch:"ad_id"`
	AdName      string  `json:"ad_name"     ch:"ad_name"`
	Impressions uint64  `json:"impressions" ch:"impressions"`
	Clicks      uint64  `json:"clicks"      ch:"clicks"`
	Conversions uint64  `json:"conversions" ch:"conversions"`
	Cost        float64 `json:"cost"        ch:"cost"`
	Revenue     float64 `json:"revenue"     ch:"revenue"`
	CTR         float64 `json:"ctr"         ch:"-"`
	CVR         float64 `json:"cvr"         ch:"-"`
}

// RegionStat 地域维度
type RegionStat struct {
	Region      string  `json:"region"      ch:"region"`
	Impressions uint64  `json:"impressions" ch:"impressions"`
	Clicks      uint64  `json:"clicks"      ch:"clicks"`
	UV          uint64  `json:"uv"          ch:"uv"`
	Cost        float64 `json:"cost"        ch:"cost"`
}

// DeviceStat 设备维度
type DeviceStat struct {
	Device      string  `json:"device"      ch:"device"`
	Impressions uint64  `json:"impressions" ch:"impressions"`
	Clicks      uint64  `json:"clicks"      ch:"clicks"`
	Conversions uint64  `json:"conversions" ch:"conversions"`
}

// FunnelStep 漏斗（手动 Scan，不用 ScanStruct）
type FunnelStep struct {
	Step  string  `json:"step"`
	Count uint64  `json:"count"`
	Rate  float64 `json:"rate"`
}

// RetentionStat 留存
type RetentionStat struct {
	Day   int64   `json:"day"   ch:"day"`
	Users uint64  `json:"users" ch:"users"`
	Rate  float64 `json:"rate"  ch:"-"`
}

// TopItem 通用 TopN
type TopItem struct {
	Key   string  `json:"key"   ch:"path"`
	Name  string  `json:"name,omitempty" ch:"-"`
	Value float64 `json:"value" ch:"cnt"`
}