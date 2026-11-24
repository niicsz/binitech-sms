package main

import (
	"context"
	"crypto/rand"

	httpadapter "github.com/niicsz/binitech-sms/internal/adapter/in/http"
	"github.com/niicsz/binitech-sms/internal/adapter/out/postgres"
	"github.com/niicsz/binitech-sms/internal/adapter/out/system"
	"github.com/niicsz/binitech-sms/internal/application/usecase/links"
	"github.com/niicsz/binitech-sms/internal/bootstrap"
	"github.com/niicsz/binitech-sms/internal/platform/httpserver"
)

func main() {
	bootstrap.Main("link-redirector", func(ctx context.Context, p bootstrap.Process) error {
		clock := system.Clock{}
		resolver := links.New(postgres.NewEngagement(p.Pool), postgres.NewCampaigns(p.Pool), system.NewIDs(clock), clock, rand.Reader, p.Config.LinkBase)
		return httpserver.Run(ctx, p.Log, p.Config.HTTPAddr, httpadapter.NewRedirector(p.Log, resolver), p.Config.ShutdownTimeout)
	})
}
