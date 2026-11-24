package http

import (
	"context"

	"github.com/niicsz/binitech-sms/internal/adapter/in/http/api"
	"github.com/niicsz/binitech-sms/internal/application/port/in"
)

func (h *handlers) ListWebhooks(ctx context.Context, _ api.ListWebhooksRequestObject) (api.ListWebhooksResponseObject, error) {
	principal, ok := principalFrom(ctx)
	if !ok {
		return nil, errNoPrincipal
	}
	views, err := h.webhooks.List(ctx, principal)
	if err != nil {
		return nil, err
	}
	data := make([]api.Webhook, len(views))
	for i, v := range views {
		data[i] = webhookDTO(v)
	}
	return api.ListWebhooks200JSONResponse{Data: data}, nil
}

func (h *handlers) CreateWebhook(ctx context.Context, req api.CreateWebhookRequestObject) (api.CreateWebhookResponseObject, error) {
	principal, ok := principalFrom(ctx)
	if !ok {
		return nil, errNoPrincipal
	}
	var input in.WebhookInput
	if req.Body != nil {
		input = webhookInput(req.Body.Url, req.Body.Events, req.Body.Description)
	}
	view, secret, err := h.webhooks.Create(ctx, principal, input)
	if err != nil {
		return nil, err
	}
	dto := webhookDTO(view)
	return api.CreateWebhook201JSONResponse{Data: api.WebhookCreated{
		Id: dto.Id, Url: dto.Url, Events: dto.Events, Description: dto.Description, Enabled: dto.Enabled,
		Failures: dto.Failures, LastStatus: dto.LastStatus, CreatedAt: dto.CreatedAt, UpdatedAt: dto.UpdatedAt,
		Secret: secret,
	}}, nil
}

func (h *handlers) GetWebhook(ctx context.Context, req api.GetWebhookRequestObject) (api.GetWebhookResponseObject, error) {
	principal, ok := principalFrom(ctx)
	if !ok {
		return nil, errNoPrincipal
	}
	view, err := h.webhooks.Get(ctx, principal, req.Id)
	if err != nil {
		return nil, err
	}
	return api.GetWebhook200JSONResponse{Data: webhookDTO(view)}, nil
}

func (h *handlers) UpdateWebhook(ctx context.Context, req api.UpdateWebhookRequestObject) (api.UpdateWebhookResponseObject, error) {
	principal, ok := principalFrom(ctx)
	if !ok {
		return nil, errNoPrincipal
	}
	var (
		input  in.WebhookInput
		enable bool
	)
	if req.Body != nil {
		input = webhookInput(req.Body.Url, req.Body.Events, req.Body.Description)
		enable = req.Body.Enable != nil && *req.Body.Enable
	}
	view, err := h.webhooks.Update(ctx, principal, req.Id, input, enable)
	if err != nil {
		return nil, err
	}
	return api.UpdateWebhook200JSONResponse{Data: webhookDTO(view)}, nil
}

func (h *handlers) DeleteWebhook(ctx context.Context, req api.DeleteWebhookRequestObject) (api.DeleteWebhookResponseObject, error) {
	principal, ok := principalFrom(ctx)
	if !ok {
		return nil, errNoPrincipal
	}
	if err := h.webhooks.Delete(ctx, principal, req.Id); err != nil {
		return nil, err
	}
	return api.DeleteWebhook204Response{}, nil
}

func webhookInput(url string, events []api.WebhookEvent, description *string) in.WebhookInput {
	input := in.WebhookInput{URL: url}
	for _, e := range events {
		input.Events = append(input.Events, string(e))
	}
	if description != nil {
		input.Description = *description
	}
	return input
}

func webhookDTO(v in.WebhookView) api.Webhook {
	dto := api.Webhook{
		Id: v.ID, Url: v.URL, Enabled: v.Enabled, Failures: v.Failures, CreatedAt: v.CreatedAt, UpdatedAt: v.UpdatedAt,
		Events: make([]api.WebhookEvent, len(v.Events)),
	}
	for i, e := range v.Events {
		dto.Events[i] = api.WebhookEvent(e)
	}
	if v.Description != "" {
		dto.Description = &v.Description
	}
	if v.LastStatus != 0 {
		dto.LastStatus = &v.LastStatus
	}
	return dto
}
