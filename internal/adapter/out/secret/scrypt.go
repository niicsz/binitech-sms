package secret

import (
	"crypto/rand"
	"crypto/subtle"
	"fmt"

	"golang.org/x/crypto/scrypt"

	"github.com/niicsz/binitech-sms/internal/application/port/out"
)

const (
	saltBytes = 16
	keyBytes  = 32
)

type Scrypt struct {
	N, R, P int
}

var _ out.SecretHasher = Scrypt{}

func DefaultScrypt() Scrypt {
	return Scrypt{N: 1 << 15, R: 8, P: 1}
}

func (s Scrypt) Hash(secret string) ([]byte, []byte, error) {
	salt := make([]byte, saltBytes)
	if _, err := rand.Read(salt); err != nil {
		return nil, nil, fmt.Errorf("scrypt: salt: %w", err)
	}
	hash, err := scrypt.Key([]byte(secret), salt, s.N, s.R, s.P, keyBytes)
	if err != nil {
		return nil, nil, fmt.Errorf("scrypt: %w", err)
	}
	return hash, salt, nil
}

func (s Scrypt) Verify(secret string, hash, salt []byte) bool {
	got, err := scrypt.Key([]byte(secret), salt, s.N, s.R, s.P, keyBytes)
	if err != nil {
		return false
	}
	return subtle.ConstantTimeCompare(got, hash) == 1
}
