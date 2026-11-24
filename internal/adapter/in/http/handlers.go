package http

import (
	"context"
	"errors"

	"github.com/niicsz/binitech-sms/internal/adapter/in/http/api"
	"github.com/niicsz/binitech-sms/internal/application/port/in"
)

var errNoPrincipal = errors.New("http: requisição sem principal autenticado")

type handlers struct {
	sender   in.MessageSender
	queries  in.AccountQueries
	webhooks in.WebhookManager
}

var _ api.StrictServerInterface = (*handlers)(nil)

func (h *handlers) SendMessage(ctx context.Context, req api.SendMessageRequestObject) (api.SendMessageResponseObject, error) {
	principal, ok := principalFrom(ctx)
	if !ok {
		return nil, errNoPrincipal
	}
	cmd := in.SendCommand{Principal: principal}
	if req.Params.IdempotencyKey != nil {
		cmd.IdempotencyKey = *req.Params.IdempotencyKey
	}
	if req.Body != nil {
		cmd.To, cmd.From, cmd.Text = req.Body.To, req.Body.From, req.Body.Text
		if req.Body.Normalize != nil {
			cmd.Normalize = *req.Body.Normalize
		}
		if req.Body.ValidityMinutes != nil {
			cmd.ValidityMinutes = *req.Body.ValidityMinutes
		}
		if req.Body.WebhookRef != nil {
			cmd.WebhookRef = *req.Body.WebhookRef
		}
	}
	result, err := h.sender.Send(ctx, cmd)
	if err != nil {
		return nil, err
	}
	test := result.Test
	return api.SendMessage202JSONResponse{
		Data: api.SendResult{
			MessageId: result.MessageID,
			Status:    api.MessageStatus(result.Status),
			Segments:  result.Segments,
			Encoding:  api.Encoding(result.Encoding),
			Cost:      result.Cost.String(),
			Currency:  result.Currency,
			Test:      &test,
		},
	}, nil
}

func (h *handlers) GetMessage(ctx context.Context, req api.GetMessageRequestObject) (api.GetMessageResponseObject, error) {
	principal, ok := principalFrom(ctx)
	if !ok {
		return nil, errNoPrincipal
	}
	view, err := h.queries.Message(ctx, principal, req.Id)
	if err != nil {
		return nil, err
	}
	msg := api.Message{
		MessageId: view.ID,
		To:        view.To,
		From:      view.From,
		Status:    api.MessageStatus(view.Status),
		Segments:  view.Segments,
		Encoding:  api.Encoding(view.Encoding),
		Cost:      view.Cost.String(),
		Currency:  view.Currency,
		Test:      &view.Test,
		CreatedAt: view.CreatedAt,
		ExpiresAt: view.ExpiresAt,
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
	return api.GetMessage200JSONResponse{Data: msg}, nil
}

func (h *handlers) GetBalance(ctx context.Context, _ api.GetBalanceRequestObject) (api.GetBalanceResponseObject, error) {
	principal, ok := principalFrom(ctx)
	if !ok {
		return nil, errNoPrincipal
	}
	view, err := h.queries.Balance(ctx, principal)
	if err != nil {
		return nil, err
	}
	return api.GetBalance200JSONResponse{Data: api.Balance{
		Available: view.Available.String(),
		Reserved:  view.Reserved.String(),
		Spendable: view.Spendable.String(),
		Currency:  view.Currency,
	}}, nil
}
