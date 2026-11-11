package meshcore

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
)

// RequestTelemetry запрашивает телеметрию узла по имени, применяя ту же
// политику повторов, что и для личных сообщений: cfg.DirectAttempts попыток
// по известному маршруту, затем cfg.FloodAttempts широковещательно.
//
// Логин и пароль не требуются: обработчик onContactRequest() проверяет только
// telemetry_mode_base/loc/env узла и флаги контакта.
func (c *Client) RequestTelemetry(ctx context.Context, name string) (*Telemetry, error) {
	ct, err := c.ResolveContact(ctx, name)
	if err != nil {
		return nil, err
	}
	return c.RequestTelemetryFrom(ctx, ct)
}

// RequestTelemetryFrom — то же для уже найденного контакта.
func (c *Client) RequestTelemetryFrom(ctx context.Context, ct *Contact) (*Telemetry, error) {
	direct, flood := c.cfg.DirectAttempts, c.cfg.FloodAttempts
	total := direct + flood
	prefix := ct.Prefix()
	flooded := false
	attempt := 0
	var lastErr error

	for i := 0; i < total; i++ {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if !flooded && i == direct && flood > 0 {
			if err := c.ResetPath(ctx, ct); err != nil {
				c.cfg.Logf("meshcore: сброс маршрута: %v", err)
			}
			flooded = true
			c.cfg.Logf("meshcore: телеметрия, переход на flood после %d попыток", direct)
		}
		attempt = i + 1

		// Кадр 0x8B не содержит tag (прошивка его вырезает), поэтому
		// сопоставляем по 6-байтному префиксу публичного ключа.
		push := c.arm(func(f []byte) bool {
			return len(f) >= 8 && f[0] == pushTelemetry && bytes.Equal(f[2:8], prefix)
		})

		payload := make([]byte, 0, 36)
		payload = append(payload, cmdSendTelemetry, 0, 0, 0)
		payload = append(payload, ct.PublicKey[:]...)

		sent, err := c.tx([]byte{respSent, respErr}, payload, c.cfg.CommandTimeout)
		if err != nil {
			push.release()
			lastErr = err
			c.cfg.Logf("meshcore: телеметрия, попытка %d: %v", attempt, err)
			c.sleep(c.cfg.RetryDelay)
			continue
		}
		if sent[0] == respErr {
			push.release()
			lastErr = frameError(sent)
			if errors.Is(lastErr, ErrNotFound) {
				return nil, lastErr
			}
			c.sleep(c.cfg.RetryDelay)
			continue
		}
		if len(sent) < 10 {
			push.release()
			lastErr = errors.New("meshcore: короткий кадр SENT")
			c.sleep(c.cfg.RetryDelay)
			continue
		}

		route := "direct"
		if sent[1] == 1 {
			route = "flood"
		}
		est := binary.LittleEndian.Uint32(sent[6:10])
		wait := c.clampWait(est)
		c.cfg.Logf("meshcore: телеметрия, попытка %d (%s), жду %s", attempt, route, wait)

		f, err := push.wait(wait)
		if err == nil {
			t, ok := parseTelemetryPush(f)
			if !ok {
				lastErr = errors.New("meshcore: не удалось разобрать кадр телеметрии")
				c.sleep(c.cfg.RetryDelay)
				continue
			}
			t.Attempts = attempt
			t.Route = route
			t.FromName = ct.Name
			return t, nil
		}
		lastErr = err
		c.sleep(c.cfg.RetryDelay)
	}

	if lastErr == nil {
		lastErr = ErrTimeout
	}
	return nil, lastErr
}

// RequestSelfTelemetry запрашивает телеметрию самого компаньона.
// Формат запроса — ровно 4 байта без публичного ключа; прошивка отвечает
// кадром 0x8B немедленно, без выхода в эфир.
func (c *Client) RequestSelfTelemetry(ctx context.Context) (*Telemetry, error) {
	push := c.arm(func(f []byte) bool {
		return len(f) >= 8 && f[0] == pushTelemetry
	})
	defer push.release()

	if err := c.send([]byte{cmdSendTelemetry, 0, 0, 0}); err != nil {
		return nil, err
	}
	f, err := push.wait(c.cfg.CommandTimeout)
	if err != nil {
		return nil, err
	}
	t, ok := parseTelemetryPush(f)
	if !ok {
		return nil, errors.New("meshcore: не удалось разобрать кадр телеметрии")
	}
	t.Attempts = 1
	t.Route = "self"
	t.FromName = c.Self().Name
	return t, nil
}

// BatteryStorage — напряжение батареи и занятость хранилища компаньона.
type BatteryStorage struct {
	Millivolts uint16
	UsedKB     uint32
	TotalKB    uint32
}

// Battery возвращает состояние батареи и хранилища собственного узла.
func (c *Client) Battery(ctx context.Context) (BatteryStorage, error) {
	var b BatteryStorage
	f, err := c.tx([]byte{respBattStorage, respErr}, []byte{cmdGetBattStorage}, c.cfg.CommandTimeout)
	if err != nil {
		return b, err
	}
	if f[0] == respErr {
		return b, frameError(f)
	}
	if len(f) < 11 {
		return b, errors.New("meshcore: короткий кадр BATTERY")
	}
	b.Millivolts = binary.LittleEndian.Uint16(f[1:3])
	b.UsedKB = binary.LittleEndian.Uint32(f[3:7])
	b.TotalKB = binary.LittleEndian.Uint32(f[7:11])
	return b, nil
}
