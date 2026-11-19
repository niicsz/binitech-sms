package money

import (
	"errors"
	"fmt"
	"strconv"
	"strings"
)

const (
	Scale    = 4
	unitsPer = 10000
)

var ErrInvalid = errors.New("money: valor inválido")

type Amount int64

func Parse(value string) (Amount, error) {
	value = strings.TrimSpace(value)
	if value == "" {
		return 0, ErrInvalid
	}
	negative := strings.HasPrefix(value, "-")
	value = strings.TrimPrefix(value, "-")
	whole, frac, _ := strings.Cut(value, ".")
	if whole == "" || len(frac) > Scale || !digits(whole) || !digits(frac) {
		return 0, ErrInvalid
	}
	frac += strings.Repeat("0", Scale-len(frac))
	w, err := strconv.ParseInt(whole, 10, 64)
	if err != nil {
		return 0, ErrInvalid
	}
	f, err := strconv.ParseInt(frac, 10, 64)
	if err != nil {
		return 0, ErrInvalid
	}
	amount := Amount(w*unitsPer + f)
	if negative {
		amount = -amount
	}
	return amount, nil
}

func (a Amount) Times(n int) Amount {
	return a * Amount(n)
}

func (a Amount) String() string {
	sign := ""
	v := int64(a)
	if v < 0 {
		sign = "-"
		v = -v
	}
	return fmt.Sprintf("%s%d.%04d", sign, v/unitsPer, v%unitsPer)
}

func (a Amount) MarshalText() ([]byte, error) {
	return []byte(a.String()), nil
}

func (a *Amount) UnmarshalText(text []byte) error {
	parsed, err := Parse(string(text))
	if err != nil {
		return err
	}
	*a = parsed
	return nil
}

func digits(s string) bool {
	for _, c := range s {
		if c < '0' || c > '9' {
			return false
		}
	}
	return true
}
