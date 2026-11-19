package events

import "fmt"

const (
	TopicStatusDLR        = "sms.status.dlr"
	TopicWebhookOutbound  = "webhook.outbound"
	TopicLedgerEvents     = "ledger.events"
	TopicCampaignProgress = "campaign.progress"
	TopicLinkClicks       = "link.clicks"
	TopicDLQ              = "dlq.global"

	RetryShort = "retry.5m"
	RetryLong  = "retry.1h"
)

func TopicSubmit(env string) string {
	return "sms.submit." + env
}

func TopicDispatch(routeID string) string {
	return "sms.dispatch." + routeID
}

func RetryTopic(topic, tier string) string {
	return fmt.Sprintf("%s.%s", topic, tier)
}
