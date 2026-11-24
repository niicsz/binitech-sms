package destination

import (
	"context"
	"strings"

	"github.com/niicsz/binitech-sms/internal/application/port/out"
	"github.com/niicsz/binitech-sms/internal/domain/phone"
)

type Brazil struct{}

var _ out.DestinationResolver = Brazil{}

func (Brazil) Resolve(_ context.Context, number phone.Number) (out.Destination, error) {
	digits := strings.TrimPrefix(number.String(), "+")
	if !strings.HasPrefix(digits, "55") {
		return out.Destination{}, out.ErrNotFound
	}
	national := digits[2:]
	if len(national) != 11 || national[2] != '9' {
		return out.Destination{}, out.ErrNotFound
	}
	return out.Destination{MCC: "724", Country: "BR"}, nil
}
