# GoMeshcoreLib <img src="https://cdn.jsdelivr.net/gh/selfhst/icons/webp/meshcore.webp" alt="Meshtastic Logo" width="80"/>

[English](README.md) | [Русский](README_ru.md)



![alt text](https://img.shields.io/badge/Golang-%20>1.21-blue?style=flat-square&logo=go)

![alt text](https://img.shields.io/badge/Status-progress-orange?style=for-the-badge&logo=Buddy)
<!-- ![alt text](https://img.shields.io/badge/Status-in%20complete-2E8B57?style=for-the-badge&logo=Buddy) -->

Компактная клиентская библиотека на Go для LoRa-сетей MeshCore. Она работает
напрямую по companion-протоколу, поэтому подходит любому companion-радио
MeshCore через WiFi/TCP или USB/serial — без мобильного приложения и без Python.

Библиотека реализует личные сообщения с политикой повторов «сначала по
маршруту, затем широковещательно», отправку в хэштег-каналы с выбором региона,
приём входящих сообщений с SNR и данными о маршруте, запросы телеметрии и
чтение информации о собственном узле.

## Возможности

- Личные сообщения контакту с настраиваемыми повторами: сначала по последнему
  известному маршруту, затем по flood после автоматического сброса маршрута.
- Отправка в хэштег-каналы с выбором региона и подтверждением, что пакет
  повторил хотя бы один ретранслятор.
- Входящие личные и канальные сообщения с SNR, числом хопов и типом маршрута.
- Приём сырых пакетов через `rx_log` — единственный источник RSSI, хэшей хопов
  и кодов региона.
- Телеметрия с любого узла и с собственного компаньона без выхода в эфир.
- Разбор Cayenne LPP в типизированные поля.
- Информация об узле: имя, радиопараметры, батарея, хранилище, статистика
  ядра/радио/пакетов, часы устройства, произвольные CLI-команды.
- Удалённые CLI-команды другим узлам.
- API подписок на события с буферизованными каналами.
- Независимость от транспорта: один и тот же протокольный код поверх TCP и serial.

## Требования

- Go 1.21 или новее (используются встроенные `min` и `max`).
- Одна внешняя зависимость: `go.bug.st/serial` для USB/COM.
- Companion-радио MeshCore с TCP (WiFi) или USB serial интерфейсом.
- Для `CLI` (`CMD_RUN_CLI_COMMAND`) нужна прошивка компаньона v14 или новее.

## Установка

```bash
go get github.com/vakarianplay/GoMeshcoreLib
```

Ручная установка

```bash
git clone https://github.com/vakarianplay/GoMeshcoreLib
cd GoMeshcoreLib
go mod init GoMeshcoreLib
go mod tidy
go build ./...
```

## Быстрый старт

```go
package main

import (
    "context"
    "fmt"
    "log"

    "meshcore"
)

func main() {
    ctx := context.Background()

    cfg := meshcore.DefaultConfig()
    cfg.Verbose = true
    cfg.Logf = func(f string, a ...any) { fmt.Printf(f+"\n", a...) }

    // Компаньон по WiFi
    c, err := meshcore.DialTCPClient("mesh.lan", 5000, cfg)
    if err != nil {
        log.Fatal(err)
    }
    defer c.Close()

    // Или компаньон по USB:
    // c, err := meshcore.DialSerialClient("/dev/ttyACM0", 115200, cfg)

    fmt.Println("узел:", c.NodeName())
    fmt.Println("радио:", c.RadioPrefs())

    t, err := c.RequestSelfTelemetry(ctx)
    if err != nil {
        log.Fatal(err)
    }
    for _, f := range t.LPP.Fields {
        fmt.Printf("  ch%d %s = %.2f %s\n", f.Channel, f.Name, f.Value, f.Unit)
    }
}
```

## Конфигурация

`DefaultConfig()` возвращает значения ниже. Любое нулевое поле заменяется ими
через `Config.withDefaults`.

| Поле | По умолчанию | Смысл |
|---|---|---|
| `DirectAttempts` | 5 | Попыток по известному маршруту до перехода на flood |
| `FloodAttempts` | 5 | Широковещательных попыток после `ResetPath` |
| `ChannelAttempts` | 3 | Попыток отправки в канал до эха ретранслятора |
| `RetryDelay` | 0 | Пауза между попытками |
| `EchoWindow` | 8s | Сколько ждать эхо от ретрансляторов |
| `RequireRepeats` | 1 | Сколько эхо одного пакета считать подтверждением |
| `CommandTimeout` | 15s | Таймаут короткого обмена команда/ответ |
| `AckSlack` | 1.25 | Множитель к `suggested_timeout` от прошивки |
| `MinWait` | 5s | Нижняя граница ожидания ответа из эфира |
| `MaxWait` | 5m | Верхняя граница ожидания ответа из эфира |
| `AppName` | `go-meshcore` | Имя, передаваемое прошивке в `APP_START` |
| `Verbose` | false | Дамп каждого кадра в hex через `Logf` |
| `Logf` | no-op | Приёмник логов |
| `EventBuffer` | 256 | Размер буфера подписки по умолчанию |

Время ожидания ответа из эфира берётся из `suggested_timeout` кадра `SENT`,
умножается на `AckSlack` и ограничивается диапазоном `[MinWait, MaxWait]`.
Прошивка считает это значение по эфирному времени, поэтому flood-маршруты
естественно получают намного большее окно, чем direct.

## Личные сообщения

```go
res, err := c.SendDirectMessage(ctx, "MO EST 13", "привет")
if err != nil {
    log.Println("ошибка отправки:", err)
}
fmt.Printf("acked=%v попыток=%d direct=%d flood=%d маршрут=%s trip=%s\n",
    res.Acked, res.Attempts, res.DirectAttempts, res.FloodAttempts,
    res.Route, res.TripTime)
```

`SendDirectMessageTo(ctx, ct, text)` принимает уже найденный `*Contact` и
избавляет от повторного поиска.

Порядок повторов:

1. До `DirectAttempts` попыток по маршруту, сохранённому для контакта.
2. `ResetPath` помечает исходящий маршрут неизвестным.
3. До `FloodAttempts` попыток широковещательно.

Каждая попытка подтверждается push-кадром ACK с тем же 4-байтным tag, что и в
`SENT`. Цикл завершается при первом совпадении. `ErrNotFound` прерывает сразу:
повторять отправку неизвестному контакту бессмысленно.

Текст длиннее `MaxTextLen` (133 байта) отклоняется. `SplitText` режет его на
части с индикатором `[i/n]`.

## Каналы и регионы

```go
ch, err := c.EnsureHashtagChannel(ctx, "test")
if err != nil {
    log.Fatal(err)
}

res, err := c.SendChannelMessage(ctx, ch, "привет #test", meshcore.ChannelOptions{
    Scope: "vostok",
})
fmt.Printf("sent=%v heard=%v попыток=%d эхо=%d\n",
    res.Sent, res.Heard, res.Attempts, len(res.Echoes))
for _, e := range res.Echoes {
    fmt.Printf("  эхо %s snr=%.2f rssi=%.0f\n", e.Path, e.SNR, e.RSSI)
}
```

`ChannelOptions`:

| Поле | Смысл |
|---|---|
| `Scope` | Имя региона, например `vostok`. Пусто — scope узла по умолчанию |
| `Unscoped` | Классическая неограниченная рассылка, игнорирует `Scope` |
| `Attempts` | Переопределяет `ChannelAttempts` |
| `EchoWindow` | Переопределяет `EchoWindow` |
| `RequireRepeats` | Переопределяет `RequireRepeats` |
| `RestoreScope` | Восстановить прежнее состояние scope, по умолчанию true |

`EnsureHashtagChannel` находит существующий слот или создаёт новый. Ключ канала —
первые 16 байт `SHA-256("#name")`, это стандартный вывод ключа хэштег-канала,
поэтому другие клиенты MeshCore видят тот же канал. Слот 0 зарезервирован под
встроенный Public и не занимается. При отсутствии свободных слотов возвращается
`ErrNoChannelSlots`.

Регионы используют тот же вывод: `SetFloodScope(ctx, "vostok")` хэширует
`#vostok` в 16-байтный transport-ключ. `ClearFloodScope` снимает
переопределение, `SetUnscoped` включает классическую рассылку, `DefaultScope`
читает регион, заданный на узле.

### Как определяется «услышанность»

Для групповых сообщений прошивка отвечает только `RESP_CODE_OK` — ни tag, ни
ожидаемого ACK, в отличие от личных сообщений. Надёжно связать ответ с
конкретной отправкой поэтому нельзя.

`Heard` выводится из эхо в `rx_log`: ретранслятор, повторивший пакет, добавляет
в него свой хэш хопа, и эта копия приходит клиенту с SNR и RSSI. Сопоставление
эвристическое и использует три условия:

- тип полезной нагрузки `GRP_TXT` (0x05),
- непустой маршрут, то есть хотя бы один ретранслятор добавил хэш,
- кластеризация по побайтовому совпадению payload, который ретрансляторы не
  меняют.

`Sent` равен true всегда, когда прошивка приняла пакет, даже если эхо не
подтверждено. В плотном эфире поднимите `RequireRepeats` до 2, чтобы снизить
риск ложной атрибуции.

## Приём сообщений

```go
sub := c.Subscribe(meshcore.EventDirectMessage, meshcore.EventChannelMessage)
defer sub.Cancel()

for ev := range sub.Events() {
    switch ev.Kind {
    case meshcore.EventDirectMessage:
        m := ev.Direct
        fmt.Printf("[DM] %s: %s (snr=%.2f, %s)\n",
            m.FromName, m.Text, m.SNR, m.Path)
    case meshcore.EventChannelMessage:
        m := ev.Channel
        fmt.Printf("[CH %s] %s (snr=%.2f, %s)\n",
            m.ChannelName, m.Text, m.SNR, m.Path)
    }
}
```

`Subscribe()` без аргументов получает все типы событий. `SubscribeBuffered`
задаёт размер буфера явно — для `EventRxLog` берите большой, поскольку это поток
всех принятых пакетов.

При переполнении буфера события отбрасываются и считаются. `Dropped()`
возвращает накопленный счётчик.

Клиент ведёт внутренний насос: на каждый push `MSG_WAITING` он вычитывает очередь
сообщений прошивки через `CMD_SYNC_NEXT_MESSAGE` и рассылает кадры подписчикам.
`DrainMessages()` запускает тот же цикл вручную.

### Типы событий

| Тип | Поле события |
|---|---|
| `EventDirectMessage` | `ev.Direct` |
| `EventChannelMessage` | `ev.Channel` |
| `EventChannelData` | `ev.Frame` |
| `EventRxLog` | `ev.RxLog` |
| `EventACK` | `ev.Frame` |
| `EventTelemetry` | `ev.Telem` |
| `EventStatus` | `ev.Frame` |
| `EventBinaryResponse` | `ev.Frame` |
| `EventAdvert`, `EventNewAdvert` | `ev.PubKey`, `ev.Frame` |
| `EventPathUpdate` | `ev.Frame` |
| `EventMsgWaiting` | отсутствует |
| `EventLoginSuccess`, `EventLoginFail` | `ev.Frame` |
| `EventTraceData`, `EventControlData`, `EventRawData` | `ev.Frame` |

## Маршрут, SNR и RSSI

Кадры сообщений несут только SNR и байт `path_len`. `PathInfo` разбирает этот
байт в тип маршрута, размер хэша и число хопов:

```go
fmt.Println(m.Path.Flood)      // false означает direct (маркер 0xFF)
fmt.Println(m.Path.HashCount)  // число хопов
fmt.Println(m.Path.HashSize)   // байт на хэш: 1..3
fmt.Println(m.Path)            // "direct", "flood/0hop" или "flood[a3→5f]"
```

Самих хэшей хопов и RSSI в кадрах сообщений нет. Они приходят из `rx_log`
(push 0x88), который несёт сырой пакет:

```go
sub := c.Subscribe(meshcore.EventRxLog)
defer sub.Cancel()

for ev := range sub.Events() {
    e := ev.RxLog
    fmt.Printf("snr=%.2f rssi=%.0f type=%d path=%s hops=%v\n",
        e.SNR, e.RSSI, e.Pkt.PayloadType, e.Pkt.Path, e.Pkt.Path.Hops)
    if e.Pkt.HasTransport {
        fmt.Printf("  код региона=0x%04x ответный=0x%04x\n",
            e.Pkt.TransportCodes[0], e.Pkt.TransportCodes[1])
    }
}
```

Для контакта доступен маршрут его последнего advert:

```go
path, when, err := c.ContactAdvertPath(ctx, ct)
```

Имя региона по transport-коду не восстанавливается. Это усечённый до 2 байт
HMAC-SHA256 от типа нагрузки и тела пакета с ключом региона, поэтому наружу
отдаётся только сырой код.

## Телеметрия

```go
t, err := c.RequestTelemetry(ctx, "MO EST 08")
if err != nil {
    log.Fatal(err)
}
fmt.Printf("%s: %d попыток, маршрут=%s\n", t.FromName, t.Attempts, t.Route)
for _, f := range t.LPP.Fields {
    fmt.Printf("  ch%d %-20s %8.2f %s\n", f.Channel, f.Name, f.Value, f.Unit)
}
if v, ok := t.LPP.Get("voltage"); ok {
    fmt.Println("батарея:", v, "V")
}
```

Логин и пароль не нужны. Обработчик телеметрии в прошивке проверяет только режимы
узла (`telemetry_mode_base`, `_loc`, `_env`) и флаги контакта. Если на узле
телеметрия запрещена, ответ не придёт никогда и вызов исчерпает все попытки.

`RequestTelemetryFrom(ctx, ct)` пропускает поиск по имени.
`RequestSelfTelemetry(ctx)` запрашивает данные самого компаньона: запрос состоит
из 4 байт без публичного ключа, и прошивка отвечает сразу, не выходя в эфир.

Ответы сопоставляются по 6-байтному префиксу публичного ключа, потому что
прошивка вырезает tag из кадра телеметрии.

Повторы работают по той же политике, что и для личных сообщений:
`DirectAttempts` по известному маршруту, затем `ResetPath` и `FloodAttempts`
широковещательно.

### Поля LPP

| Тип | Имя | Единица | Ширина | Масштаб |
|---|---|---|---|---|
| 0 | `digital_input` | — | 1 | 1 |
| 1 | `digital_output` | — | 1 | 1 |
| 2 | `analog_input` | — | 2 | 0.01 signed |
| 3 | `analog_output` | — | 2 | 0.01 signed |
| 100 | `generic_sensor` | — | 4 | 1 |
| 101 | `luminosity` | lx | 2 | 1 |
| 102 | `presence` | — | 1 | 1 |
| 103 | `temperature` | °C | 2 | 0.1 signed |
| 104 | `relative_humidity` | % | 1 | 0.5 |
| 113 | `accelerometer` | G | 6 | 0.001 на ось |
| 115 | `pressure` | hPa | 2 | 0.1 |
| 116 | `voltage` | V | 2 | 0.01 |
| 117 | `current` | A | 2 | 0.001 signed |
| 118 | `frequency` | Hz | 4 | 1 |
| 120 | `percentage` | % | 1 | 1 |
| 121 | `altitude` | m | 2 | 1 signed |
| 125 | `concentration` | ppm | 2 | 1 |
| 128 | `power` | W | 2 | 1 |
| 130 | `distance` | m | 4 | 0.001 |
| 131 | `energy` | kWh | 4 | 0.001 |
| 132 | `direction` | ° | 2 | 1 |
| 133 | `unixtime` | s | 4 | 1 |
| 134 | `gyrometer` | °/s | 6 | 0.01 на ось |
| 135 | `colour` | RGB | 3 | по каналам |
| 136 | `gps` | ° | 9 | 0.0001 lat/lon, 0.01 alt |
| 142 | `switch` | — | 1 | 1 |

Канал 1 по соглашению несёт напряжение батареи и температуру MCU. Канал 0 в
потоке означает конец данных.

Неизвестный тип останавливает разбор, а остаток попадает в `LPPResult.Tail` в
виде hex. Это сделано намеренно: `getDataSize()` в прошивке возвращает 1 для
неизвестных типов, что молча исказило бы все последующие поля.

Если влажность выглядит вдвое завышенной, а следующие поля съехали, значит
прошивка разошлась сама с собой в ширине этого поля. Задайте:

```go
meshcore.HumidityFieldSize = 2
```

## Информация о собственном узле

```go
c.NodeName()        // имя узла, без выхода в эфир
c.Self()            // SelfInfo: ключ, радио, режимы телеметрии, координаты
c.Device()          // DeviceInfo: версия прошивки, модель, лимиты
c.RadioPrefs()      // частота, полоса, SF, CR, мощность, path hash mode

bat, _ := c.Battery(ctx)
fmt.Printf("%d мВ, занято %d/%d КБ\n", bat.Millivolts, bat.UsedKB, bat.TotalKB)

core, _ := c.StatsCore(ctx)     // батарея, аптайм, флаги ошибок, длина очереди
radio, _ := c.StatsRadio(ctx)   // шум, последние RSSI/SNR, эфирное время
pkts, _ := c.StatsPackets(ctx)  // счётчики пакетов, flood против direct

secs, _ := c.CurrentTime(ctx)
_ = c.SyncClock(ctx)            // выставить часы устройства по времени хоста
```

`RadioPrefs()` читается из кэша, полученного при подключении, и не тратит
эфирное время.

### CLI

```go
out, err := c.CLI(ctx, "get radio")   // выполняется на локальном компаньоне (v14+)
fmt.Println(meshcore.ParseCLIValue(out))
```

```go
err := c.RemoteCLI(ctx, ct, "get name") // отправляет команду другому узлу
```

Удалённый узел выполнит команду только если контакту выдано право Remote CLI.
Иначе текст дойдёт до владельца как обычное сообщение. Выдавайте это право
только собственным узлам: оно позволяет менять радиопараметры, переименовывать
узел, перезагружать его и читать сохранённый пароль WiFi.

`ParseCLIValue` разбирает ответ вида `> 910.5250244,62.5,7,5` на поля,
`ParseCLIFloat` приводит первое из них к float.

## Контакты и каналы

```go
_ = c.RefreshContacts(ctx)   // перечитать книгу контактов
for _, ct := range c.Contacts() {
    fmt.Println(ct.Name, ct.Type, ct.IsRepeater())
}

ct, err := c.ResolveContact(ctx, "MO EST 13")  // точное, затем регистронезависимое
_ = c.ResetPath(ctx, ct)                       // следующая отправка уйдёт по flood
path, when, _ := c.ContactAdvertPath(ctx, ct)
```

```go
_ = c.RefreshChannels()
for _, ch := range c.Channels() {
    fmt.Println(ch.Index, ch.Name)
}
ch, ok := c.FindChannel("test")
_ = c.SetChannel(ctx, ch.Index, "#test", meshcore.DeriveKey16("#test"))
```

Кадры контактов разбираются от конца кадра назад, потому что поле маршрута
переменной длины идёт перед именем, а его максимальный размер прошивка наружу не
отдаёт. Хвост после имени всегда 16 байт, поэтому имя начинается с `len - 48`.

Книга контактов передаётся потоком по одному кадру за итерацию цикла прошивки,
поэтому на большой книге чтение занимает секунды. `RefreshContacts` ограничивает
каждый кадр масштабированным `CommandTimeout`, а весь обмен — переданным
контекстом.

## Заметки о протоколе

Обрамление кадров одинаково для TCP и serial:

```text
app  -> radio: '<' + uint16 LE длина + payload
radio -> app : '>' + uint16 LE длина + payload
```

При рассинхронизации чтение ищет следующий `'>'`. Длины 0 и больше 512 считаются
мусором.

Инициализация сессии отправляет `APP_START`, `DEVICE_QUERY` с версией протокола 3
и `SET_DEVICE_TIME`. Именно версия 3 заставляет прошивку отдавать V3-кадры
сообщений с SNR. `APP_START` заодно сбрасывает итератор контактов в прошивке,
поэтому `GET_CONTACTS` не может завершиться `BAD_STATE`, оставшимся от другого
клиента.

Одновременно выполняется только одна короткая транзакция «команда — ответ», она
защищена мьютексом. Запись кадров сериализована отдельно. Ожидания регистрируются
до записи команды, поэтому push, пришедший сразу за `SENT`, не теряется.

Запросы телеметрии должны выполняться последовательно. Прошивка хранит один слот
`pending_telemetry` и очищает ожидающие запросы при каждом новом, поэтому второй
параллельный запрос оставил бы ответ первого невостребованным.

## Ограничения

- Имя региона по transport-коду не восстанавливается. Это усечённое значение
  HMAC, а не идентификатор. Наружу отдаётся только сырой 16-битный код.
- Атрибуция эхо при отправке в канал эвристическая, см. раздел про `Heard`.
- Кадры сообщений не содержат хэшей хопов и RSSI. Используйте `EventRxLog` или
  `ContactAdvertPath`.
- Кадры телеметрии не содержат tag, поэтому ответы сопоставляются по префиксу
  публичного ключа. Два одновременных запроса к одному узлу различить нельзя.
- Ширина поля влажности неоднозначна в самих исходниках прошивки. Значение по
  умолчанию 1 соответствует кодировщику CayenneLPP и `readRelativeHumidity`,
  тогда как `getDataSize` возвращает 2.
- Максимальный размер маршрута прошивка не отдаёт. На разбор это не влияет, но
  поля LPP неизвестного типа из-за этого нельзя безопасно пропустить.
- Библиотека рассчитана на companion-протокол сборок примерно v1.16 — v1.18.
  В более старых сборках может не быть `CMD_RUN_CLI_COMMAND`, scoped flood или
  V3-кадров.
- IPv6-хосты поддерживаются через `net.JoinHostPort`.

## Устранение неполадок

| Симптом | Причина и действие |
|---|---|
| `dial ... connection refused` | Компаньон собран только с BLE. TCP-сервер есть лишь в сборках с WiFi. Проверьте `get wifi.status` и `get wifi.ip` через serial-консоль |
| `NOT_FOUND` при отправке или запросе телеметрии | Узла нет в книге контактов. Отправьте advert, дождитесь появления узла и повторите |
| Приходит `SENT`, но нет кадра телеметрии | На узле телеметрия запрещена либо у контакта не выставлены telemetry-флаги |
| `ErrNotHeard` при `Sent == true` | Ни один ретранслятор не повторил пакет в пределах `EchoWindow`. Пакет в эфир ушёл. Увеличьте окно или `ChannelAttempts` |
| `ErrNoAck` после всех попыток | Абонент вне зоны, выключен или работает на других радиопараметрах |
| Канальные сообщения не приходят вовсе | Нет слота хэштег-канала. Вызовите `EnsureHashtagChannel` |
| `Dropped()` растёт | Переполнение буфера подписки. Используйте `SubscribeBuffered` с большим размером, особенно для `EventRxLog` |
| Влажность выглядит вдвое больше | Задайте `HumidityFieldSize = 2` |
| Поток кадров рассинхронизирован | Включите `Verbose` и изучите hex-дамп; чтение автоматически ресинхронизируется по `'>'` |

## Тестирование

Ничто в протокольной части не требует оборудования:

```bash
go test ./...
```

Чистые функции, которые стоит покрыть табличными тестами: `DecodeLPP`,
`decodePathBytes`, `parseRawPacket`, `parseContact`, `messageBody`.

Smoke-тест на реальном радио без выхода в эфир — `RequestSelfTelemetry`: один
вызов проверяет обрамление кадров, handshake и декодер LPP.


## Источники

Детали протокола взяты из прошивки и официальных клиентских библиотек:

- [meshcore-dev/MeshCore](https://github.com/meshcore-dev/MeshCore) —
  `examples/companion_radio/MyMesh.cpp`, `src/helpers/wifi/SerialWifiInterface.cpp`,
  `src/helpers/ArduinoSerialInterface.cpp`, `src/Packet.h`, `src/Packet.cpp`,
  `src/helpers/TransportKeyStore.cpp`, `src/helpers/sensors/LPPDataHelpers.h`,
  `src/helpers/BaseChatMesh.cpp`, `src/helpers/CommonCLI.cpp`
- [meshcore-dev/meshcore_py](https://github.com/meshcore-dev/meshcore_py) —
  `packets.py`, `messaging.py`, `binary.py`, `tcp_cx.py`
- [meshcore-dev/meshcore-cli](https://github.com/meshcore-dev/meshcore-cli)
- [Companion Protocol — MeshCore Docs](https://docs.meshcore.io/companion_protocol/)
- [CLI Commands — MeshCore Docs](https://docs.meshcore.io/cli_commands/)
- [Repeater commands — MeshCore Ninja](https://meshcore.ninja/repeater-commands/)
