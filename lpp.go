package GoMeshcoreLib

import (
	"encoding/hex"
	"fmt"
)

// HumidityFieldSize is the byte width of the relative humidity field.
// The firmware disagrees with itself: LPPData::getDataSize() says 2 while
// LPPReader::readRelativeHumidity() reads 1 byte at 0.5 % steps, and the
// CayenneLPP encoder writes 1. Set to 2 if humidity reads double and the
// following fields look shifted.
var HumidityFieldSize = lppDefaultHumiditySize

// LPPField is one telemetry value.
type LPPField struct {
	Channel int     `json:"channel"`
	Type    int     `json:"type"`
	Name    string  `json:"name"`
	Unit    string  `json:"unit"`
	Value   float64 `json:"value"`
}

// LPPResult is a decoded Cayenne LPP buffer.
type LPPResult struct {
	Fields []LPPField `json:"fields"`
	Raw    string     `json:"raw_hex"`
	Tail   string     `json:"unparsed_hex,omitempty"`
}

// Get returns the first field with the given name.
func (r LPPResult) Get(name string) (float64, bool) {
	for _, f := range r.Fields {
		if f.Name == name {
			return f.Value, true
		}
	}
	return 0, false
}

type lppSpec struct {
	name   string
	unit   string
	size   int
	div    float64
	signed bool
}

var lppSpecs = map[byte]lppSpec{
	lppDigitalInput:  {"digital_input", "", lppSize1, lppDiv1, false},
	lppDigitalOutput: {"digital_output", "", lppSize1, lppDiv1, false},
	lppAnalogInput:   {"analog_input", "", lppSize2, lppDiv100, true},
	lppAnalogOutput:  {"analog_output", "", lppSize2, lppDiv100, true},
	lppGeneric:       {"generic_sensor", "", lppSize4, lppDiv1, false},
	lppLuminosity:    {"luminosity", "lx", lppSize2, lppDiv1, false},
	lppPresence:      {"presence", "", lppSize1, lppDiv1, false},
	lppTemperature:   {"temperature", "°C", lppSize2, lppDiv10, true},
	lppRelHumidity:   {"relative_humidity", "%", lppSize1, lppDiv2, false},
	lppAccelerometer: {"accelerometer", "G", lppSize6, lppAxisDiv, true},
	lppPressure:      {"pressure", "hPa", lppSize2, lppDiv10, false},
	lppVoltage:       {"voltage", "V", lppSize2, lppDiv100, false},
	lppCurrent:       {"current", "A", lppSize2, lppDiv1000, true},
	lppFrequency:     {"frequency", "Hz", lppSize4, lppDiv1, false},
	lppPercentage:    {"percentage", "%", lppSize1, lppDiv1, false},
	lppAltitude:      {"altitude", "m", lppSize2, lppDiv1, true},
	lppConcentration: {"concentration", "ppm", lppSize2, lppDiv1, false},
	lppPower:         {"power", "W", lppSize2, lppDiv1, false},
	lppDistance:      {"distance", "m", lppSize4, lppDiv1000, false},
	lppEnergy:        {"energy", "kWh", lppSize4, lppDiv1000, false},
	lppDirection:     {"direction", "°", lppSize2, lppDiv1, false},
	lppUnixTime:      {"unixtime", "s", lppSize4, lppDiv1, false},
	lppGyrometer:     {"gyrometer", "°/s", lppSize6, lppGyroDiv, true},
	lppColour:        {"colour", "RGB", lppSize3, lppDiv1, false},
	lppGPS:           {"gps", "°", lppSize9, lppDiv1, true},
	lppSwitch:        {"switch", "", lppSize1, lppDiv1, false},
}

// dataSize reports the value width of an LPP type. ok is false for unknown
// types: parsing then stops and the remainder lands in Tail, which is safer
// than LPPData::getDataSize() returning 1 and silently corrupting the rest.
func dataSize(t byte) (int, bool) {
	if t == lppRelHumidity {
		return HumidityFieldSize, true
	}
	s, ok := lppSpecs[t]
	return s.size, ok
}

func typeName(t byte) (string, string) {
	s, ok := lppSpecs[t]
	if !ok {
		return fmt.Sprintf(lppNameUnknown, t), ""
	}
	return s.name, s.unit
}

// beFloat decodes a big-endian LPP value.
func beFloat(b []byte, div float64, signed bool) float64 {
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
	return sign * float64(v) / div
}

func decodeScalar(b []byte, t byte) float64 {
	s := lppSpecs[t]
	return beFloat(b, s.div, s.signed)
}

// DecodeLPP decodes a Cayenne LPP buffer. Channel 0 marks the end of data.
func DecodeLPP(buf []byte) LPPResult {
	res := LPPResult{Raw: hex.EncodeToString(buf)}
	pos := 0

	add := func(ch int, t byte, name, unit string, v float64) {
		res.Fields = append(res.Fields, LPPField{ch, int(t), name, unit, v})
	}
	// Rewind past the header so it is included in the unparsed remainder.
	truncated := func(rewind int) LPPResult {
		res.Tail = hex.EncodeToString(buf[max(pos-rewind, 0):])
		return res
	}

	for pos+2 <= len(buf) {
		ch, typ := int(buf[pos]), buf[pos+1]
		pos += 2
		if ch == 0 {
			break
		}
		name, unit := typeName(typ)

		switch typ {
		case lppGPS:
			if pos+lppSize9 > len(buf) {
				return truncated(2)
			}
			add(ch, typ, name+lppSuffixLat, "°", beFloat(buf[pos:pos+lppCoordLen], lppCoordDiv, true))
			add(ch, typ, name+lppSuffixLon, "°", beFloat(buf[pos+lppCoordLen:pos+6], lppCoordDiv, true))
			add(ch, typ, name+lppSuffixAlt, "m", beFloat(buf[pos+6:pos+9], lppAltDiv, true))
			pos += lppSize9

		case lppAccelerometer, lppGyrometer:
			if pos+lppSize6 > len(buf) {
				return truncated(2)
			}
			div := lppAxisDiv
			if typ == lppGyrometer {
				div = lppGyroDiv
			}
			for i, suffix := range []string{lppSuffixX, lppSuffixY, lppSuffixZ} {
				off := pos + i*lppAxisLen
				add(ch, typ, name+suffix, unit, beFloat(buf[off:off+lppAxisLen], div, true))
			}
			pos += lppSize6

		case lppColour:
			if pos+lppSize3 > len(buf) {
				return truncated(2)
			}
			for i, suffix := range []string{lppSuffixR, lppSuffixG, lppSuffixB} {
				add(ch, typ, name+suffix, "", float64(buf[pos+i]))
			}
			pos += lppSize3

		default:
			n, known := dataSize(typ)
			if !known || pos+n > len(buf) {
				return truncated(2)
			}
			add(ch, typ, name, unit, decodeScalar(buf[pos:pos+n], typ))
			pos += n
		}
	}
	return res
}
