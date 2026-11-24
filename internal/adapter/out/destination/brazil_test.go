package destination_test

import (
	"context"
	"errors"
	"testing"

	"github.com/niicsz/binitech-sms/internal/adapter/out/destination"
	"github.com/niicsz/binitech-sms/internal/application/port/out"
	"github.com/niicsz/binitech-sms/internal/domain/phone"
)

func TestResolvesBrazilianMobile(t *testing.T) {
	dest, err := destination.Brazil{}.Resolve(context.Background(), phone.Number("+5511987654321"))
	if err != nil || dest.MCC != "724" || dest.Country != "BR" || dest.MNC != "" {
		t.Fatalf("got %+v, %v", dest, err)
	}
}

func TestRejectsLandlineAndOtherCountries(t *testing.T) {
	for _, n := range []string{"+551133334444", "+14155550100", "+55119876543"} {
		if _, err := (destination.Brazil{}).Resolve(context.Background(), phone.Number(n)); !errors.Is(err, out.ErrNotFound) {
			t.Fatalf("%s: got %v", n, err)
		}
	}
}
