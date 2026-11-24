package smpp

import "github.com/fiorix/go-smpp/smpp/pdu"

func (c *Carrier) Handle(p pdu.Body) {
	c.handle(p)
}
