package main

import (
	"context"
	"time"
	_ "time/tzdata"

	"github.com/niicsz/binitech-sms/internal/adapter/out/postgres"
	"github.com/niicsz/binitech-sms/internal/adapter/out/system"
	"github.com/niicsz/binitech-sms/internal/application/usecase/campaign"
	"github.com/niicsz/binitech-sms/internal/bootstrap"
)

func main() {
	bootstrap.Main("campaign-scheduler", func(ctx context.Context, p bootstrap.Process) error {
		clock := system.Clock{}
		runner := campaign.New(postgres.NewCampaigns(p.Pool), postgres.NewAccounts(p.Pool),
			bootstrap.SendService(p.Pool, p.Producer, p.Config.Env, clock), system.NewIDs(clock), clock)
		bootstrap.Loop(ctx, p.Log, "campaign-scheduler", time.Second, runner.Tick)
		return nil
	})
}
