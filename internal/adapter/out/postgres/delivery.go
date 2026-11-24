package postgres

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/niicsz/binitech-sms/internal/application/port/out"
	"github.com/niicsz/binitech-sms/internal/domain/money"
	"github.com/niicsz/binitech-sms/internal/domain/route"
)

type Delivery struct {
	pool *pgxpool.Pool
}

var (
	_ out.RoutingStore     = (*Delivery)(nil)
	_ out.StatusStore      = (*Delivery)(nil)
	_ out.RouteHealthStore = (*Delivery)(nil)
)

func NewDelivery(pool *pgxpool.Pool) *Delivery {
	return &Delivery{pool: pool}
}

func (r *Delivery) MarkRouted(ctx context.Context, messageID, routeID string) (bool, error) {
	tag, err := conn(ctx, r.pool).Exec(ctx,
		`UPDATE messages SET status = 'routed', route_id = $2, updated_at = now() WHERE id = $1 AND status = 'accepted'`,
		messageID, routeID)
	if err != nil {
		return false, fmt.Errorf("delivery: routed: %w", err)
	}
	return tag.RowsAffected() == 1, nil
}

func (r *Delivery) Transition(ctx context.Context, t out.Transition) (out.StoredMessage, bool, error) {
	from := make([]string, len(t.From))
	for i, s := range t.From {
		from[i] = string(s)
	}
	final := t.To.Final()
	m, err := scanMessage(conn(ctx, r.pool).QueryRow(ctx,
		`UPDATE messages
		    SET status = $2,
		        error_code = COALESCE(NULLIF($4, ''), error_code),
		        updated_at = now(),
		        submitted_at = CASE WHEN $2 = 'submitted' THEN $5 ELSE submitted_at END,
		        done_at = CASE WHEN $6 THEN $5 ELSE done_at END
		  WHERE id = $1 AND status = ANY($3)
		  RETURNING `+messageColumns,
		t.MessageID, string(t.To), from, t.ErrorCode, t.At, final))
	if err == nil {
		return m, true, nil
	}
	if !errors.Is(err, out.ErrNotFound) {
		return out.StoredMessage{}, false, fmt.Errorf("delivery: transição: %w", err)
	}
	current, err := r.Get(ctx, t.MessageID)
	if err != nil {
		return out.StoredMessage{}, false, err
	}
	return current, false, nil
}

func (r *Delivery) Get(ctx context.Context, messageID string) (out.StoredMessage, error) {
	return scanMessage(conn(ctx, r.pool).QueryRow(ctx, `SELECT `+messageColumns+` FROM messages WHERE id = $1`, messageID))
}

func (r *Delivery) SaveParts(ctx context.Context, messageID, routeID string, providerIDs []string) error {
	for i, id := range providerIDs {
		if _, err := conn(ctx, r.pool).Exec(ctx,
			`INSERT INTO message_parts (message_id, part, route_id, provider_msg_id) VALUES ($1, $2, $3, $4)
			 ON CONFLICT DO NOTHING`, messageID, i+1, routeID, id); err != nil {
			return fmt.Errorf("delivery: partes: %w", err)
		}
	}
	_, err := conn(ctx, r.pool).Exec(ctx,
		`UPDATE messages SET provider_msg_id = $2 WHERE id = $1 AND provider_msg_id IS NULL`, messageID, providerIDs[0])
	return err
}

func (r *Delivery) FindPart(ctx context.Context, routeID, providerID string) (string, int, error) {
	var (
		messageID string
		part      int
	)
	err := conn(ctx, r.pool).QueryRow(ctx,
		`SELECT message_id, part FROM message_parts WHERE route_id = $1 AND provider_msg_id = $2`,
		routeID, providerID).Scan(&messageID, &part)
	if errors.Is(err, pgx.ErrNoRows) {
		return "", 0, out.ErrNotFound
	}
	if err != nil {
		return "", 0, fmt.Errorf("delivery: parte: %w", err)
	}
	return messageID, part, nil
}

func (r *Delivery) UpdatePart(ctx context.Context, messageID string, part int, status, errorCode string) ([]out.PartStatus, error) {
	if _, err := conn(ctx, r.pool).Exec(ctx,
		`UPDATE message_parts SET status = $3, error_code = NULLIF($4, ''), updated_at = now()
		  WHERE message_id = $1 AND part = $2`, messageID, part, status, errorCode); err != nil {
		return nil, fmt.Errorf("delivery: atualizar parte: %w", err)
	}
	rows, err := conn(ctx, r.pool).Query(ctx,
		`SELECT part, status FROM message_parts WHERE message_id = $1 ORDER BY part FOR UPDATE`, messageID)
	if err != nil {
		return nil, fmt.Errorf("delivery: partes: %w", err)
	}
	defer rows.Close()
	var parts []out.PartStatus
	for rows.Next() {
		var p out.PartStatus
		if err := rows.Scan(&p.Part, &p.Status); err != nil {
			return nil, err
		}
		parts = append(parts, p)
	}
	return parts, rows.Err()
}

func (r *Delivery) RecordDLR(ctx context.Context, messageID, routeID, providerStatus, mappedStatus, errorCode, opID string, raw map[string]string) error {
	if raw == nil {
		raw = map[string]string{}
	}
	body, err := json.Marshal(raw)
	if err != nil {
		return err
	}
	_, err = conn(ctx, r.pool).Exec(ctx,
		`INSERT INTO dlr_events (message_id, route_id, provider_status, mapped_status, error_code, raw, op_id)
		 VALUES ($1, $2, $3, $4, NULLIF($5, ''), $6, $7) ON CONFLICT (op_id) DO NOTHING`,
		messageID, routeID, providerStatus, mappedStatus, errorCode, body, opID)
	if err != nil {
		return fmt.Errorf("delivery: dlr: %w", err)
	}
	return nil
}

func (r *Delivery) Report(ctx context.Context, routeID string, healthy bool, detail string) error {
	_, err := conn(ctx, r.pool).Exec(ctx,
		`INSERT INTO route_health (route_id, healthy, detail, checked_at) VALUES ($1, $2, $3, now())
		 ON CONFLICT (route_id) DO UPDATE SET healthy = EXCLUDED.healthy, detail = EXCLUDED.detail, checked_at = now()`,
		routeID, healthy, detail)
	if err != nil {
		return fmt.Errorf("delivery: saúde: %w", err)
	}
	return nil
}

type RouteCatalog struct {
	pool      *pgxpool.Pool
	ttl       time.Duration
	staleness time.Duration

	mu       sync.Mutex
	cached   []route.Route
	loadedAt time.Time
}

var _ out.RouteCatalog = (*RouteCatalog)(nil)

func NewRouteCatalog(pool *pgxpool.Pool, ttl, staleness time.Duration) *RouteCatalog {
	return &RouteCatalog{pool: pool, ttl: ttl, staleness: staleness}
}

func (c *RouteCatalog) Routes(ctx context.Context) ([]route.Route, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.cached != nil && time.Since(c.loadedAt) < c.ttl {
		return c.cached, nil
	}
	routes, err := c.load(ctx)
	if err != nil {
		if c.cached != nil {
			return c.cached, nil
		}
		return nil, err
	}
	c.cached, c.loadedAt = routes, time.Now()
	return routes, nil
}

func (c *RouteCatalog) Invalidate() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.loadedAt = time.Time{}
}

func (c *RouteCatalog) load(ctx context.Context) ([]route.Route, error) {
	rows, err := c.pool.Query(ctx,
		`SELECT r.id, r.kind, r.mcc_mnc, r.cost::text, r.priority, r.weight, r.enabled,
		        COALESCE(h.healthy AND h.checked_at > now() - make_interval(secs => $1), false)
		   FROM routes r LEFT JOIN route_health h ON h.route_id = r.id`, c.staleness.Seconds())
	if err != nil {
		return nil, fmt.Errorf("rotas: %w", err)
	}
	defer rows.Close()
	routes := []route.Route{}
	for rows.Next() {
		var (
			rt   route.Route
			cost string
		)
		if err := rows.Scan(&rt.ID, &rt.Kind, &rt.Covers, &cost, &rt.Priority, &rt.Weight, &rt.Enabled, &rt.Healthy); err != nil {
			return nil, err
		}
		if rt.Cost, err = money.Parse(cost); err != nil {
			return nil, err
		}
		routes = append(routes, rt)
	}
	return routes, rows.Err()
}

func (c *RouteCatalog) Listen(ctx context.Context, onError func(error)) {
	for ctx.Err() == nil {
		if err := c.listenOnce(ctx); err != nil && ctx.Err() == nil {
			onError(err)
			select {
			case <-ctx.Done():
			case <-time.After(5 * time.Second):
			}
		}
	}
}

func (c *RouteCatalog) listenOnce(ctx context.Context) error {
	connection, err := c.pool.Acquire(ctx)
	if err != nil {
		return err
	}
	defer connection.Release()
	if _, err := connection.Exec(ctx, "LISTEN routes_changed"); err != nil {
		return err
	}
	for {
		if _, err := connection.Conn().WaitForNotification(ctx); err != nil {
			return err
		}
		c.Invalidate()
	}
}
