package main

import (
	"context"
	"crypto/rand"
	"errors"
	"flag"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/niicsz/binitech-sms/internal/adapter/out/postgres"
	"github.com/niicsz/binitech-sms/internal/adapter/out/secret"
	"github.com/niicsz/binitech-sms/internal/adapter/out/system"
	"github.com/niicsz/binitech-sms/internal/application/usecase/admin"
	"github.com/niicsz/binitech-sms/internal/domain/apikey"
	"github.com/niicsz/binitech-sms/internal/domain/money"
	"github.com/niicsz/binitech-sms/internal/platform/postgresx"
)

const usage = `smsctl: operação da plataforma BiniTech SMS

  smsctl account create  -name "Padaria" [-plan default] [-currency BRL]
  smsctl key issue       -account acc_... -env live|test [-label texto]
  smsctl key revoke      -account acc_... -key <id público>
  smsctl balance credit  -account acc_... -amount 50.00
  smsctl tariff set      -plan default -mcc 724 [-mnc 05] -price 0.0800 [-from 2026-12-01T00:00:00Z]
  smsctl sender approve  -account acc_... -sender BINITECH -kind shortcode -countries BR

Usa DATABASE_URL do ambiente.`

func main() {
	if err := run(os.Args[1:]); err != nil {
		fmt.Fprintln(os.Stderr, "erro:", err)
		os.Exit(1)
	}
}

func run(args []string) error {
	if len(args) < 2 {
		fmt.Println(usage)
		return errors.New("informe o recurso e a ação")
	}
	url := os.Getenv("DATABASE_URL")
	if url == "" {
		return errors.New("defina DATABASE_URL")
	}
	ctx := context.Background()
	pool, err := postgresx.NewPool(ctx, url)
	if err != nil {
		return err
	}
	defer pool.Close()
	clock := system.Clock{}
	svc := admin.New(postgres.NewTxManager(pool), postgres.NewAdmin(pool), postgres.NewBalances(pool),
		secret.DefaultScrypt(), system.NewIDs(clock), clock, rand.Reader)

	fs := flag.NewFlagSet(args[0]+" "+args[1], flag.ContinueOnError)
	account := fs.String("account", "", "id da conta")
	name := fs.String("name", "", "nome da conta")
	plan := fs.String("plan", "default", "plano de tarifas")
	currency := fs.String("currency", "BRL", "moeda")
	env := fs.String("env", "live", "ambiente da chave")
	label := fs.String("label", "", "descrição da chave")
	key := fs.String("key", "", "id público da chave")
	amount := fs.String("amount", "", "valor em reais")
	mcc := fs.String("mcc", "", "MCC")
	mnc := fs.String("mnc", "", "MNC (vazio = qualquer operadora)")
	price := fs.String("price", "", "preço por segmento")
	from := fs.String("from", "", "vigência (RFC 3339)")
	sender := fs.String("sender", "", "remetente")
	kind := fs.String("kind", "shortcode", "alphanumeric, shortcode ou longcode")
	countries := fs.String("countries", "BR", "países ISO separados por vírgula")
	if err := fs.Parse(args[2:]); err != nil {
		return err
	}

	switch args[0] + " " + args[1] {
	case "account create":
		id, err := svc.CreateAccount(ctx, *name, *plan, *currency)
		if err != nil {
			return err
		}
		fmt.Println(id)
	case "key issue":
		raw, err := svc.IssueKey(ctx, *account, apikey.Environment(*env), *label)
		if err != nil {
			return err
		}
		fmt.Println(raw)
		fmt.Fprintln(os.Stderr, "Guarde a chave agora: ela não pode ser recuperada depois.")
	case "key revoke":
		return svc.RevokeKey(ctx, *account, *key)
	case "balance credit":
		value, err := money.Parse(*amount)
		if err != nil {
			return fmt.Errorf("valor inválido: %w", err)
		}
		balance, err := svc.Credit(ctx, *account, value)
		if err != nil {
			return err
		}
		fmt.Printf("disponível %s, reservado %s\n", balance.Available, balance.Reserved)
	case "tariff set":
		value, err := money.Parse(*price)
		if err != nil {
			return fmt.Errorf("preço inválido: %w", err)
		}
		var validFrom time.Time
		if *from != "" {
			if validFrom, err = time.Parse(time.RFC3339, *from); err != nil {
				return fmt.Errorf("vigência inválida: %w", err)
			}
		}
		return svc.SetTariff(ctx, *plan, *mcc, *mnc, value, validFrom)
	case "sender approve":
		return svc.ApproveSender(ctx, *account, *sender, *kind, strings.Split(*countries, ","))
	default:
		fmt.Println(usage)
		return fmt.Errorf("comando %q desconhecido", args[0]+" "+args[1])
	}
	return nil
}
