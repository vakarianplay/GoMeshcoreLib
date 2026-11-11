package meshcore

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"strings"
	"time"
)

// Contact — запись из книги контактов компаньона.
type Contact struct {
	Name       string
	PublicKey  [32]byte
	Type       byte
	Flags      byte
	OutPathLen byte
	LastAdvert time.Time
	Lat        float64
	Lon        float64
	LastMod    time.Time
}

// Проверки типа узла.
func (c Contact) IsRepeater() bool { return c.Type == AdvTypeRepeater }
func (c Contact) IsRoom() bool     { return c.Type == AdvTypeRoom }
func (c Contact) IsSensor() bool   { return c.Type == AdvTypeSensor }
func (c Contact) IsChat() bool     { return c.Type == AdvTypeChat }

// Prefix — первые 6 байт публичного ключа (используются в DM-кадрах).
func (c Contact) Prefix() []byte { return c.PublicKey[:6] }

// Channel — слот группового канала.
type Channel struct {
	Index  byte
	Name   string
	Secret [16]byte
}

// Empty сообщает, что слот свободен.
func (c Channel) Empty() bool {
	if c.Name != "" {
		return false
	}
	for _, b := range c.Secret {
		if b != 0 {
			return false
		}
	}
	return true
}

// ---------------------------------------------------------------------------
// Разбор кадра контакта
// ---------------------------------------------------------------------------

// parseContact разбирает RESP_CODE_CONTACT.
// Раскладка из writeContactRespFrame():
//
//	[0]     code
//	[1:33]  pub_key(32)                  <- фиксированный offset
//	[33]    type, [34] flags, [35] out_path_len
//	[36:36+MAX_PATH_SIZE] out_path       <- ЕДИНСТВЕННОЕ поле переменной длины
//	name(32) + last_advert(4) + lat(4) + lon(4) + lastmod(4)
//
// Хвост после имени всегда 16 байт, поэтому имя начинается с len-48 и знать
// MAX_PATH_SIZE не нужно.
func parseContact(f []byte) (Contact, bool) {
	const tail = 32 + 16
	var c Contact
	if len(f) < 1+32+3+tail {
		return c, false
	}
	copy(c.PublicKey[:], f[1:33])
	c.Type = f[33]
	c.Flags = f[34]
	c.OutPathLen = f[35]

	nameOff := len(f) - tail
	c.Name = strings.TrimRight(string(f[nameOff:nameOff+32]), "\x00")

	lastAdv := binary.LittleEndian.Uint32(f[nameOff+32:])
	lat := int32(binary.LittleEndian.Uint32(f[nameOff+36:]))
	lon := int32(binary.LittleEndian.Uint32(f[nameOff+40:]))
	lastMod := binary.LittleEndian.Uint32(f[nameOff+44:])

	c.LastAdvert = time.Unix(int64(lastAdv), 0).UTC()
	c.Lat = float64(lat) / 1e6
	c.Lon = float64(lon) / 1e6
	c.LastMod = time.Unix(int64(lastMod), 0).UTC()
	return c, true
}

// ---------------------------------------------------------------------------
// Контакты
// ---------------------------------------------------------------------------

// RefreshContacts заново вычитывает книгу контактов с узла.
// Поток формируется в MyMesh::loop() по одному кадру за итерацию, поэтому
// чтение может занять секунды на большой книге. perFrame — таймаут на ОДИН
// кадр; общая длительность ограничивается ctx.
func (c *Client) RefreshContacts(ctx context.Context) ([]Contact, error) {
	p := c.arm(matchAny([]byte{respContactsStart, respContact, respContactsEnd, respErr}))
	defer p.release()

	c.cmu.Lock()
	err := c.writeFrame([]byte{cmdGetContacts})
	c.cmu.Unlock()
	if err != nil {
		return nil, err
	}

	perFrame := c.cfg.CommandTimeout * 8

	// Ждём START (или BAD_STATE-ошибку, если итератор занят).
	first, err := p.nextCtx(ctx, perFrame)
	if err != nil {
		return nil, fmt.Errorf("meshcore: GET_CONTACTS: %w", err)
	}
	if first[0] == respErr {
		return nil, frameError(first)
	}

	var out []Contact
	for {
		f, err := p.nextCtx(ctx, perFrame)
		if err != nil {
			return nil, fmt.Errorf("meshcore: поток контактов: %w", err)
		}
		switch f[0] {
		case respContactsEnd:
			c.setContacts(out)
			return out, nil
		case respErr:
			return nil, frameError(f)
		case respContact:
			if ct, ok := parseContact(f); ok {
				out = append(out, ct)
			}
		}
	}
}

func (c *Client) setContacts(list []Contact) {
	c.ctcMu.Lock()
	c.contacts = list
	c.ctcReady = true
	c.ctcMu.Unlock()
}

// Contacts возвращает кэш книги контактов (загружается при подключении).
func (c *Client) Contacts() []Contact {
	c.ctcMu.RLock()
	defer c.ctcMu.RUnlock()
	out := make([]Contact, len(c.contacts))
	copy(out, c.contacts)
	return out
}

// ResolveContact ищет контакт по имени: сначала точное совпадение, затем
// регистронезависимое. При отсутствии кэша он обновляется.
func (c *Client) ResolveContact(ctx context.Context, name string) (*Contact, error) {
	c.ctcMu.RLock()
	ready := c.ctcReady
	c.ctcMu.RUnlock()
	if !ready {
		if _, err := c.RefreshContacts(ctx); err != nil {
			return nil, err
		}
	}

	ctcs := c.Contacts()
	var fallback *Contact
	var candidates []string
	for i := range ctcs {
		ct := ctcs[i]
		if ct.Name == name {
			return &ct, nil
		}
		if ct.IsRepeater() || ct.IsRoom() || ct.IsSensor() {
			candidates = append(candidates, ct.Name)
		}
		if fallback == nil && strings.EqualFold(ct.Name, name) {
			fallback = &ct
		}
	}
	if fallback != nil {
		return fallback, nil
	}
	if len(candidates) == 0 {
		return nil, fmt.Errorf("%w: контакт %q (книга контактов пуста)", ErrNotFound, name)
	}
	return nil, fmt.Errorf("%w: контакт %q; узлы в книге: %s",
		ErrNotFound, name, strings.Join(candidates, ", "))
}

func (c *Client) nameByPrefix(prefix []byte) string {
	if len(prefix) < 6 {
		return ""
	}
	c.ctcMu.RLock()
	defer c.ctcMu.RUnlock()
	for i := range c.contacts {
		if string(c.contacts[i].PublicKey[:6]) == string(prefix[:6]) {
			return c.contacts[i].Name
		}
	}
	return ""
}

// ResetPath сбрасывает исходящий маршрут контакта, чтобы следующая отправка
// ушла широковещательно (CMD_RESET_PATH + 32-байтный ключ).
func (c *Client) ResetPath(ctx context.Context, ct *Contact) error {
	payload := make([]byte, 0, 33)
	payload = append(payload, cmdResetPath)
	payload = append(payload, ct.PublicKey[:]...)
	f, err := c.tx([]byte{respOK, respErr}, payload, c.cfg.CommandTimeout)
	if err != nil {
		return err
	}
	if f[0] == respErr {
		return frameError(f)
	}
	return nil
}

// ContactAdvertPath возвращает реальный маршрут (в хэшах), по которому пришёл
// последний advert контакта. Это единственный способ получить полный маршрут
// до узла: кадры входящих сообщений несут лишь байт path_len.
func (c *Client) ContactAdvertPath(ctx context.Context, ct *Contact) (PathInfo, time.Time, error) {
	payload := make([]byte, 0, 34)
	payload = append(payload, cmdGetAdvertPath, 0)
	payload = append(payload, ct.PublicKey[:]...)
	f, err := c.tx([]byte{respAdvertPath, respErr}, payload, c.cfg.CommandTimeout)
	if err != nil {
		return PathInfo{}, time.Time{}, err
	}
	if f[0] == respErr {
		return PathInfo{}, time.Time{}, frameError(f)
	}
	if len(f) < 6 {
		return PathInfo{}, time.Time{}, errors.New("meshcore: короткий кадр ADVERT_PATH")
	}
	recv := binary.LittleEndian.Uint32(f[1:5])
	pi, _ := decodePathBytes(f[5], f[6:])
	return pi, time.Unix(int64(recv), 0).UTC(), nil
}

// ---------------------------------------------------------------------------
// Каналы
// ---------------------------------------------------------------------------

// RefreshChannels вычитывает все слоты групповых каналов.
func (c *Client) RefreshChannels() ([]Channel, error) {
	c.selfMu.RLock()
	max := c.dev.MaxChannels
	c.selfMu.RUnlock()
	if max <= 0 {
		max = 8
	}

	table := make([]Channel, 0, max)
	for idx := 0; idx < max; idx++ {
		f, err := c.tx([]byte{respChannelInfo, respErr},
			[]byte{cmdGetChannel, byte(idx)}, c.cfg.CommandTimeout)
		if err != nil {
			return nil, err
		}
		if f[0] == respErr {
			table = append(table, Channel{Index: byte(idx)})
			continue
		}
		if len(f) < 50 {
			continue
		}
		ch := Channel{Index: f[1], Name: trimNUL(string(f[2:34]))}
		copy(ch.Secret[:], f[34:50])
		table = append(table, ch)
	}

	c.chMu.Lock()
	c.channels = table
	c.chMu.Unlock()
	return table, nil
}

// ChannelTable возвращает все слоты, включая свободные.
func (c *Client) ChannelTable() []Channel {
	c.chMu.RLock()
	defer c.chMu.RUnlock()
	out := make([]Channel, len(c.channels))
	copy(out, c.channels)
	return out
}

// Channels возвращает только занятые слоты.
func (c *Client) Channels() []Channel {
	var out []Channel
	for _, ch := range c.ChannelTable() {
		if !ch.Empty() {
			out = append(out, ch)
		}
	}
	return out
}

func (c *Client) channelName(idx byte) string {
	for _, ch := range c.ChannelTable() {
		if ch.Index == idx {
			return ch.Name
		}
	}
	return ""
}

// FindChannel ищет канал по имени (с '#' или без).
func (c *Client) FindChannel(name string) (Channel, bool) {
	full := NormalizeHashtag(name)
	for _, ch := range c.Channels() {
		if ch.Name == full {
			return ch, true
		}
	}
	return Channel{}, false
}

// EnsureHashtagChannel находит или создаёт хэштег-канал.
// Ключ канала = первые 16 байт SHA-256 от "#name".
func (c *Client) EnsureHashtagChannel(ctx context.Context, name string) (Channel, error) {
	full := NormalizeHashtag(name)
	if full == "#" || full == "" {
		return Channel{}, errors.New("meshcore: пустое имя канала")
	}
	if ch, ok := c.FindChannel(full); ok {
		return ch, nil
	}

	table := c.ChannelTable()
	if len(table) == 0 {
		var err error
		if table, err = c.RefreshChannels(); err != nil {
			return Channel{}, err
		}
	}

	// Слот 0 зарезервирован под Public — не занимаем его.
	slot := -1
	for _, ch := range table {
		if ch.Index == 0 {
			continue
		}
		if ch.Empty() {
			slot = int(ch.Index)
			break
		}
	}
	if slot < 0 {
		return Channel{}, ErrNoChannelSlots
	}

	secret := DeriveKey16(full)
	if err := c.SetChannel(ctx, byte(slot), full, secret); err != nil {
		return Channel{}, err
	}
	ch := Channel{Index: byte(slot), Name: full, Secret: secret}
	c.chMu.Lock()
	for i := range c.channels {
		if c.channels[i].Index == ch.Index {
			c.channels[i] = ch
		}
	}
	c.chMu.Unlock()
	return ch, nil
}

// SetChannel записывает слот канала (idx + name(32) + secret(16)).
func (c *Client) SetChannel(ctx context.Context, idx byte, name string, secret [16]byte) error {
	payload := make([]byte, 0, 50)
	payload = append(payload, cmdSetChannel, idx)
	var nb [32]byte
	copy(nb[:], []byte(name))
	payload = append(payload, nb[:]...)
	payload = append(payload, secret[:]...)

	f, err := c.tx([]byte{respOK, respErr}, payload, c.cfg.CommandTimeout)
	if err != nil {
		return err
	}
	if f[0] == respErr {
		return frameError(f)
	}
	return nil
}

// ---------------------------------------------------------------------------
// Регион (flood scope)
// ---------------------------------------------------------------------------

// SetFloodScope ограничивает рассылку регионом, например "vostok" -> "#vostok".
// Ключ региона = первые 16 байт SHA-256 от "#region".
func (c *Client) SetFloodScope(ctx context.Context, region string) error {
	full := NormalizeHashtag(region)
	key := DeriveKey16(full)
	payload := make([]byte, 0, 18)
	payload = append(payload, cmdSetFloodScope, 0)
	payload = append(payload, key[:]...)
	f, err := c.tx([]byte{respOK, respErr}, payload, c.cfg.CommandTimeout)
	if err != nil {
		return err
	}
	if f[0] == respErr {
		return frameError(f)
	}
	return nil
}

// ClearFloodScope снимает переопределение региона — действует scope узла.
func (c *Client) ClearFloodScope(ctx context.Context) error {
	f, err := c.tx([]byte{respOK, respErr}, []byte{cmdSetFloodScope, 0}, c.cfg.CommandTimeout)
	if err != nil {
		return err
	}
	if f[0] == respErr {
		return frameError(f)
	}
	return nil
}

// SetUnscoped включает классическую неограниченную рассылку.
func (c *Client) SetUnscoped(ctx context.Context) error {
	f, err := c.tx([]byte{respOK, respErr}, []byte{cmdSetFloodScope, 1}, c.cfg.CommandTimeout)
	if err != nil {
		return err
	}
	if f[0] == respErr {
		return frameError(f)
	}
	return nil
}

// DefaultScope возвращает регион по умолчанию, заданный на узле.
func (c *Client) DefaultScope(ctx context.Context) (string, error) {
	f, err := c.tx([]byte{respDefaultScope, respErr}, []byte{cmdGetDefaultScp}, c.cfg.CommandTimeout)
	if err != nil {
		return "", err
	}
	if f[0] == respErr {
		return "", frameError(f)
	}
	if len(f) < 48 {
		return "", nil // пустое имя и ключ = scope не задан
	}
	return trimNUL(string(f[1:32])), nil
}

// Advertise отправляет advert собственного узла. flood=true — широковещательно.
func (c *Client) Advertise(ctx context.Context, flood bool) error {
	b := byte(0)
	if flood {
		b = 1
	}
	f, err := c.tx([]byte{respOK, respErr}, []byte{cmdSendSelfAdvert, b}, c.cfg.CommandTimeout)
	if err != nil {
		return err
	}
	if f[0] == respErr {
		return frameError(f)
	}
	return nil
}
