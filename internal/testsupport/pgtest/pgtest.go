package pgtest

import (
	"context"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	embeddedpostgres "github.com/fergusstrange/embedded-postgres"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

const template = "binitech_sms_template"

var (
	adminURL string
	counter  atomic.Int64
)

func Main(m *testing.M) int {
	stop, err := start()
	if err != nil {
		fmt.Fprintln(os.Stderr, "pgtest:", err)
		return 1
	}
	code := m.Run()
	stop()
	return code
}

func New(t *testing.T) *pgxpool.Pool {
	t.Helper()
	ctx := context.Background()
	name := fmt.Sprintf("t_%d_%d", os.Getpid(), counter.Add(1))
	admin, err := pgx.Connect(ctx, adminURL)
	if err != nil {
		t.Fatalf("pgtest: conectar: %v", err)
	}
	defer admin.Close(ctx)
	if _, err := admin.Exec(ctx, fmt.Sprintf("CREATE DATABASE %s TEMPLATE %s", name, template)); err != nil {
		t.Fatalf("pgtest: criar banco: %v", err)
	}
	pool, err := pgxpool.New(ctx, withDatabase(adminURL, name))
	if err != nil {
		t.Fatalf("pgtest: pool: %v", err)
	}
	t.Cleanup(func() {
		pool.Close()
		conn, err := pgx.Connect(context.Background(), adminURL)
		if err != nil {
			return
		}
		defer conn.Close(context.Background())
		_, _ = conn.Exec(context.Background(), fmt.Sprintf("DROP DATABASE IF EXISTS %s WITH (FORCE)", name))
	})
	return pool
}

func start() (func(), error) {
	stop := func() {}
	if url := os.Getenv("TEST_DATABASE_URL"); url != "" {
		adminURL = url
	} else {
		port, err := freePort()
		if err != nil {
			return nil, err
		}
		dir, err := os.MkdirTemp("", "binitech-sms-pg-")
		if err != nil {
			return nil, err
		}
		cache := filepath.Join(os.TempDir(), "binitech-sms-pg-cache")
		db := embeddedpostgres.NewDatabase(embeddedpostgres.DefaultConfig().
			Version(embeddedpostgres.V17).
			Port(uint32(port)).
			Username("sms").Password("sms").Database("postgres").
			RuntimePath(filepath.Join(dir, "runtime")).
			DataPath(filepath.Join(dir, "data")).
			CachePath(cache).
			StartTimeout(90 * time.Second).
			Logger(nil))
		if err := db.Start(); err != nil {
			return nil, fmt.Errorf("subir postgres embutido: %w", err)
		}
		stop = func() {
			_ = db.Stop()
			_ = os.RemoveAll(dir)
		}
		adminURL = fmt.Sprintf("postgres://sms:sms@127.0.0.1:%d/postgres?sslmode=disable", port)
	}
	if err := prepareTemplate(); err != nil {
		stop()
		return nil, err
	}
	return stop, nil
}

func prepareTemplate() error {
	ctx := context.Background()
	admin, err := pgx.Connect(ctx, adminURL)
	if err != nil {
		return fmt.Errorf("conectar: %w", err)
	}
	defer admin.Close(ctx)
	if _, err := admin.Exec(ctx, fmt.Sprintf("DROP DATABASE IF EXISTS %s WITH (FORCE)", template)); err != nil {
		return err
	}
	if _, err := admin.Exec(ctx, "CREATE DATABASE "+template); err != nil {
		return err
	}
	conn, err := pgx.Connect(ctx, withDatabase(adminURL, template))
	if err != nil {
		return err
	}
	defer conn.Close(ctx)
	files, err := filepath.Glob(filepath.Join(migrationsDir(), "*.up.sql"))
	if err != nil {
		return err
	}
	sort.Strings(files)
	for _, file := range files {
		sql, err := os.ReadFile(file)
		if err != nil {
			return err
		}
		if _, err := conn.Exec(ctx, string(sql)); err != nil {
			return fmt.Errorf("migração %s: %w", filepath.Base(file), err)
		}
	}
	return nil
}

func migrationsDir() string {
	_, file, _, _ := runtime.Caller(0)
	return filepath.Join(filepath.Dir(file), "..", "..", "..", "migrations")
}

func withDatabase(url, name string) string {
	base, query, _ := strings.Cut(url, "?")
	i := strings.LastIndex(base, "/")
	result := base[:i+1] + name
	if query != "" {
		result += "?" + query
	}
	return result
}

func freePort() (int, error) {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return 0, err
	}
	defer l.Close()
	return l.Addr().(*net.TCPAddr).Port, nil
}
