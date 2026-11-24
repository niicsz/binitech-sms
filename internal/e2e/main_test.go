package e2e_test

import (
	"os"
	"testing"

	"github.com/niicsz/binitech-sms/internal/testsupport/pgtest"
)

func TestMain(m *testing.M) {
	os.Exit(pgtest.Main(m))
}
