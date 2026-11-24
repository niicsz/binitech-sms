package http

import (
	"context"

	"github.com/niicsz/binitech-sms/internal/adapter/in/http/api"
	"github.com/niicsz/binitech-sms/internal/application/port/in"
)

func (h *handlers) SendBulk(ctx context.Context, req api.SendBulkRequestObject) (api.SendBulkResponseObject, error) {
	principal, ok := principalFrom(ctx)
	if !ok {
		return nil, errNoPrincipal
	}
	cmd := in.BulkCommand{Principal: principal}
	if req.Params.IdempotencyKey != nil {
		cmd.IdempotencyKey = *req.Params.IdempotencyKey
	}
	if b := req.Body; b != nil {
		cmd.From = b.From
		cmd.Text = deref(b.Text)
		cmd.Normalize = b.Normalize != nil && *b.Normalize
		if b.ValidityMinutes != nil {
			cmd.ValidityMinutes = *b.ValidityMinutes
		}
		for _, m := range b.Messages {
			cmd.Items = append(cmd.Items, in.BulkItem{To: m.To, Text: deref(m.Text), WebhookRef: deref(m.WebhookRef)})
		}
	}
	res, err := h.bulk.SendBulk(ctx, cmd)
	if err != nil {
		return nil, err
	}
	data := api.BulkResult{
		Accepted: make([]api.BulkAccepted, len(res.Accepted)), Rejected: make([]api.BulkRejected, len(res.Rejected)),
		TotalCost: res.TotalCost.String(), Currency: res.Currency, Test: res.Test,
	}
	for i, a := range res.Accepted {
		data.Accepted[i] = api.BulkAccepted{Index: a.Index, MessageId: a.MessageID, Segments: a.Segments, Cost: a.Cost.String()}
	}
	for i, r := range res.Rejected {
		field := r.Field
		data.Rejected[i] = api.BulkRejected{Index: r.Index, Message: r.Message}
		if field != "" {
			data.Rejected[i].Field = &field
		}
	}
	return api.SendBulk202JSONResponse{Data: data}, nil
}

func (h *handlers) ListMessages(ctx context.Context, req api.ListMessagesRequestObject) (api.ListMessagesResponseObject, error) {
	principal, ok := principalFrom(ctx)
	if !ok {
		return nil, errNoPrincipal
	}
	p := req.Params
	filter := in.MessageFilter{From: p.From, To: p.To, CampaignID: deref(p.CampaignId), Cursor: deref(p.Cursor)}
	if p.Status != nil {
		filter.Status = string(*p.Status)
	}
	if p.Limit != nil {
		filter.Limit = *p.Limit
	}
	page, err := h.listings.Messages(ctx, principal, filter)
	if err != nil {
		return nil, err
	}
	data := api.MessagePage{Items: make([]api.Message, len(page.Items))}
	for i, v := range page.Items {
		data.Items[i] = messageDTO(v)
	}
	if page.NextCursor != "" {
		data.NextCursor = &page.NextCursor
	}
	return api.ListMessages200JSONResponse{Data: data}, nil
}

func (h *handlers) CreateCampaign(ctx context.Context, req api.CreateCampaignRequestObject) (api.CreateCampaignResponseObject, error) {
	principal, ok := principalFrom(ctx)
	if !ok {
		return nil, errNoPrincipal
	}
	input := in.CampaignInput{}
	if req.Params.IdempotencyKey != nil {
		input.IdempotencyKey = *req.Params.IdempotencyKey
	}
	if b := req.Body; b != nil {
		input.Name, input.From, input.Text, input.ScheduledAt = b.Name, b.From, b.Text, b.ScheduledAt
		input.Normalize = b.Normalize != nil && *b.Normalize
		if b.ValidityMinutes != nil {
			input.ValidityMinutes = *b.ValidityMinutes
		}
		input.WindowStart, input.WindowEnd, input.TimeZone = deref(b.WindowStart), deref(b.WindowEnd), deref(b.TimeZone)
		for _, r := range b.Recipients {
			rc := in.CampaignRecipientInput{To: r.To}
			if r.Vars != nil {
				rc.Vars = *r.Vars
			}
			input.Recipients = append(input.Recipients, rc)
		}
	}
	summary, err := h.campaigns.Create(ctx, principal, input)
	if err != nil {
		return nil, err
	}
	return api.CreateCampaign201JSONResponse{Data: campaignDTO(summary)}, nil
}

func (h *handlers) CampaignSummary(ctx context.Context, req api.CampaignSummaryRequestObject) (api.CampaignSummaryResponseObject, error) {
	principal, ok := principalFrom(ctx)
	if !ok {
		return nil, errNoPrincipal
	}
	summary, err := h.campaigns.Summary(ctx, principal, req.Id)
	if err != nil {
		return nil, err
	}
	return api.CampaignSummary200JSONResponse{Data: campaignDTO(summary)}, nil
}

func (h *handlers) ListAlphanames(ctx context.Context, _ api.ListAlphanamesRequestObject) (api.ListAlphanamesResponseObject, error) {
	principal, ok := principalFrom(ctx)
	if !ok {
		return nil, errNoPrincipal
	}
	list, err := h.listings.Alphanames(ctx, principal)
	if err != nil {
		return nil, err
	}
	data := make([]api.Alphaname, len(list))
	for i, a := range list {
		data[i] = api.Alphaname{Sender: a.Sender, Kind: api.AlphanameKind(a.Kind), Countries: a.Countries, Status: api.AlphanameStatus(a.Status)}
	}
	return api.ListAlphanames200JSONResponse{Data: data}, nil
}

func campaignDTO(s in.CampaignSummary) api.CampaignSummary {
	return api.CampaignSummary{
		CampaignId: s.ID, Name: s.Name, Status: api.CampaignSummaryStatus(s.Status), ScheduledAt: s.ScheduledAt,
		StartedAt: s.StartedAt, FinishedAt: s.FinishedAt, Total: s.Total, Pending: s.Pending, Submitted: s.Submitted,
		Rejected: s.Rejected, Delivered: s.Delivered, Failed: s.Failed, InFlight: s.InFlight, Cost: s.Cost.String(),
		Currency: s.Currency, Test: s.Test,
	}
}

func messageDTO(view in.MessageView) api.Message {
	msg := api.Message{
		MessageId: view.ID, To: view.To, From: view.From, Status: api.MessageStatus(view.Status), Segments: view.Segments,
		Encoding: api.Encoding(view.Encoding), Cost: view.Cost.String(), Currency: view.Currency, Test: &view.Test,
		CreatedAt: view.CreatedAt, ExpiresAt: view.ExpiresAt,
	}
	if !view.UpdatedAt.IsZero() {
		msg.UpdatedAt = &view.UpdatedAt
	}
	if view.ErrorCode != "" {
		msg.ErrorCode = &view.ErrorCode
	}
	if view.WebhookRef != "" {
		msg.WebhookRef = &view.WebhookRef
	}
	return msg
}

func deref(s *string) string {
	if s == nil {
		return ""
	}
	return *s
}
