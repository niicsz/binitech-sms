package phone

import (
	"errors"
	"strings"
)

var ErrInvalid = errors.New("phone: número fora do formato E.164")

type Number string

func Parse(raw string) (Number, error) {
	var b strings.Builder
	for i, c := range strings.TrimSpace(raw) {
		switch {
		case c >= '0' && c <= '9':
			b.WriteRune(c)
		case c == '+' && i == 0:
		case c == ' ' || c == '-' || c == '(' || c == ')':
		default:
			return "", ErrInvalid
		}
	}
	digits := b.String()
	if len(digits) < 8 || len(digits) > 15 || digits[0] == '0' {
		return "", ErrInvalid
	}
	return Number("+" + digits), nil
}

func (n Number) String() string {
	return string(n)
}
