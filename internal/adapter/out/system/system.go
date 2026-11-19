package system

import (
	"crypto/rand"
	"encoding/binary"
	"fmt"
	"time"

	"github.com/niicsz/binitech-sms/internal/application/port/out"
)

const crockford = "0123456789ABCDEFGHJKMNPQRSTVWXYZ"

type Clock struct{}

var _ out.Clock = Clock{}

func (Clock) Now() time.Time { return time.Now().UTC() }

type IDs struct {
	clock out.Clock
}

var _ out.IDGenerator = IDs{}

func NewIDs(clock out.Clock) IDs {
	return IDs{clock: clock}
}

func (g IDs) MessageID() string {
	return "msg_" + ulid(g.clock.Now())
}

func (g IDs) OpID() string {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		panic(fmt.Sprintf("system: crypto/rand falhou: %v", err))
	}
	b[6] = b[6]&0x0f | 0x40
	b[8] = b[8]&0x3f | 0x80
	return fmt.Sprintf("%x-%x-%x-%x-%x", b[0:4], b[4:6], b[6:8], b[8:10], b[10:16])
}

func ulid(at time.Time) string {
	var b, stamp [16]byte
	binary.BigEndian.PutUint64(stamp[:8], uint64(at.UnixMilli()))
	copy(b[0:6], stamp[2:8])
	if _, err := rand.Read(b[6:]); err != nil {
		panic(fmt.Sprintf("system: crypto/rand falhou: %v", err))
	}
	return encode(b)
}

func encode(b [16]byte) string {
	var hi, lo uint64
	hi = binary.BigEndian.Uint64(b[0:8])
	lo = binary.BigEndian.Uint64(b[8:16])
	out := make([]byte, 26)
	for i := 25; i >= 0; i-- {
		out[i] = crockford[lo&0x1f]
		lo = lo>>5 | hi<<59
		hi >>= 5
	}
	return string(out)
}
