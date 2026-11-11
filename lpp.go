package meshcore

import (
	"encoding/hex"
	"fmt"
)

// HumidityFieldSize — размер поля относительной влажности в LPP.
// В исходниках MeshCore разнобой: LPPData::getDataSize() даёт 2 байта,
// а LPPReader::readRelativeHumidity() читает 1 байт с шагом 0.5 %.
// Кодировщик CayenneLPP пишет 1 байт, поэтому по умолчанию 1.
// Меняйте на 2, если влажность вдвое завышена, а остальные поля съехали.
var HumidityFieldSize = 1

// LPPField — одно значение телеметрии.
type LPPField struct {
	Channel int     `json:"channel"`
	Type    int     `json:"type"`
	Name    string  `json:"name"`
	Unit    string  `json:"unit"`
	Value   float64 `json:"value"`
}

// LPPResult — результат разбора Cayenne LPP.
type LPPResult struct {
	Fields []LPPField `json:"fields"`
	Raw    string     `json:"raw_hex"`
	Tail   string     `json:"unparsed_hex,omitempty"`
}

// Get возвращает первое поле с указанным именем.
func (r LPPResult) Get(name string) (float64, bool) {
	for _, f := range r.Fields {
		if f.Name == name {
			return f.Value, true
		}
	}
	return 0, false
}

// beFloat: значения LPP лежат big-endian.
func beFloat(b []byte, mult float64, signed bool) float64 {
	var v uint32
	for _, x := range b {
		v = (v << 8) + uint32(x)
	}
	sign := 1.0
	if signed {
		bit := uint32(1) << (uint(len(b))*8 - 1)
		if v&bit == bit {
			v = (bit << 1) - v
			sign = -1
		}
	}
	return sign * float64(v) / mult
}

// dataSize — длина поля значения. Для неизвестного типа ok=false: парсинг
// останавливается, а остаток попадает в Tail. Это честнее, чем поведение
// LPPData::getDataSize(), который для неизвестного типа возвращает 1 и
// молча ломает весь последующий разбор.
func dataSize(t byte) (int, bool) {
	switch t {
	case 0, 1, 102, 120, 142: // digital in/out, presence, percentage, switch
		return 1, true
	case 104: // relative humidity — см. HumidityFieldSize
		return HumidityFieldSize, true
	case 2, 3, 101, 103, 115, 116, 117, 121, 125, 128, 132:
		return 2, true
	case 135: // colour RGB
		return 3, true
	case 100, 118, 130, 131, 133:
		return 4, true
	case 113, 134: // accelerometer / gyrometer: 3 оси по 2 байта
		return 6, true
	case 136: // GPS: lat(3) lon(3) alt(3)
		return 9, true
	}
	return 0, false
}

func typeName(t byte) (string, string) {
	switch t {
	case 0:
		return "digital_input", ""
	case 1:
		return "digital_output", ""
	case 2:
		return "analog_input", ""
	case 3:
		return "analog_output", ""
	case 100:
		return "generic_sensor", ""
	case 101:
		return "luminosity", "lx"
	case 102:
		return "presence", ""
	case 103:
		return "temperature", "°C"
	case 104:
		return "relative_humidity", "%"
	case 113:
		return "accelerometer", "G"
	case 115:
		return "pressure", "hPa"
	case 116:
		return "voltage", "V"
	case 117:
		return "current", "A"
	case 118:
		return "frequency", "Hz"
	case 120:
		return "percentage", "%"
	case 121:
		return "altitude", "m"
	case 125:
		return "concentration", "ppm"
	case 128:
		return "power", "W"
	case 130:
		return "distance", "m"
	case 131:
		return "energy", "kWh"
	case 132:
		return "direction", "°"
	case 133:
		return "unixtime", "s"
	case 134:
		return "gyrometer", "°/s"
	case 135:
		return "colour", "RGB"
	case 136:
		return "gps", "°"
	case 142:
		return "switch", ""
	}
	return fmt.Sprintf("unknown_%d", t), ""
}

func decodeScalar(b []byte, t byte) float64 {
	switch t {
	case 103: // temperature: 0.1 °C, signed
		return beFloat(b, 10, true)
	case 104: // relative humidity: 0.5 %, unsigned
		return beFloat(b, 2, false)
	case 115: // pressure: 0.1 hPa, unsigned
		return beFloat(b, 10, false)
	case 116: // voltage: 0.01 V, unsigned
		return beFloat(b, 100, false)
	case 117: // current: 0.001 A, signed
		return beFloat(b, 1000, true)
	case 2, 3: // analog in/out: 0.01, signed
		return beFloat(b, 100, true)
	case 121: // altitude: 1 m, signed
		return beFloat(b, 1, true)
	case 130, 131: // distance / energy: 0.001, unsigned
		return beFloat(b, 1000, false)
	default:
		// luminosity, percentage, power, frequency, direction,
		// concentration, generic_sensor, unixtime, digital, presence, switch
		return beFloat(b, 1, false)
	}
}

// DecodeLPP разбирает буфер Cayenne LPP. Канал 0 — маркер конца данных.
func DecodeLPP(buf []byte) LPPResult {
	res := LPPResult{Raw: hex.EncodeToString(buf)}
	pos := 0
	add := func(ch int, t byte, name, unit string, v float64) {
		res.Fields = append(res.Fields, LPPField{ch, int(t), name, unit, v})
	}
	truncated := func() LPPResult {
		res.Tail = hex.EncodeToString(buf[pos:])
		return res
	}

	for pos+2 <= len(buf) {
		ch := buf[pos]
		typ := buf[pos+1]
		pos += 2
		if ch == 0 {
			break
		}
		name, unit := typeName(typ)

		switch typ {
		case 136: // GPS
			if pos+9 > len(buf) {
				return truncated()
			}
			add(int(ch), typ, name+".lat", "°", beFloat(buf[pos:pos+3], 10000, true))
			add(int(ch), typ, name+".lon", "°", beFloat(buf[pos+3:pos+6], 10000, true))
			add(int(ch), typ, name+".alt", "m", beFloat(buf[pos+6:pos+9], 100, true))
			pos += 9
		case 113, 134: // accelerometer / gyrometer
			mult := 1000.0
			if typ == 134 {
				mult = 100
			}
			if pos+6 > len(buf) {
				return truncated()
			}
			for i, ax := range []string{"x", "y", "z"} {
				add(int(ch), typ, name+"."+ax, unit,
					beFloat(buf[pos+i*2:pos+i*2+2], mult, true))
			}
			pos += 6
		case 135: // colour RGB
			if pos+3 > len(buf) {
				return truncated()
			}
			add(int(ch), typ, name+".r", "", float64(buf[pos]))
			add(int(ch), typ, name+".g", "", float64(buf[pos+1]))
			add(int(ch), typ, name+".b", "", float64(buf[pos+2]))
			pos += 3
		default:
			n, ok := dataSize(typ)
			if !ok || pos+n > len(buf) {
				pos -= 2 // откат на заголовок, чтобы он попал в Tail
				return truncated()
			}
			add(int(ch), typ, name, unit, decodeScalar(buf[pos:pos+n], typ))
			pos += n
		}
	}
	return res
}
