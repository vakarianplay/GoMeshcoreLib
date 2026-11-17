package GoMeshcoreLib

import "time"

// Framing, shared by the WiFi and serial interfaces.
const (
	frameOutMagic  byte = '<' // app -> radio
	frameInMagic   byte = '>' // radio -> app
	frameHeaderLen      = 3
	maxFrameSize        = 512
	readBufSize         = 4096
)

// Command codes (CMD_*).
const (
	cmdAppStart       byte = 1
	cmdSendTxtMsg     byte = 2
	cmdSendChannelTxt byte = 3
	cmdGetContacts    byte = 4
	cmdGetDeviceTime  byte = 5
	cmdSetDeviceTime  byte = 6
	cmdSendSelfAdvert byte = 7
	cmdAddUpdateCtc   byte = 9
	cmdSyncNextMsg    byte = 10
	cmdResetPath      byte = 13
	cmdGetBattStorage byte = 20
	cmdDeviceQuery    byte = 22
	cmdSendLogin      byte = 26
	cmdGetContactKey  byte = 30
	cmdGetChannel     byte = 31
	cmdSetChannel     byte = 32
	cmdSetOtherParams byte = 38
	cmdSendTelemetry  byte = 39
	cmdGetAdvertPath  byte = 42
	cmdSetFloodScope  byte = 54
	cmdGetStats       byte = 56
	cmdSetDefaultScp  byte = 63
	cmdGetDefaultScp  byte = 64
	cmdRunCLI         byte = 66

	appTargetVer         byte = 3
	handshakeReservedLen      = 7 // APP_START bytes 1..7 are reserved
)

// Response codes (RESP_CODE_*).
const (
	respOK            byte = 0
	respErr           byte = 1
	respContactsStart byte = 2
	respContact       byte = 3
	respContactsEnd   byte = 4
	respSelfInfo      byte = 5
	respSent          byte = 6
	respContactMsg    byte = 7
	respChannelMsg    byte = 8
	respCurrTime      byte = 9
	respNoMoreMsgs    byte = 10
	respBattStorage   byte = 12
	respDeviceInfo    byte = 13
	respDisabled      byte = 15
	respContactMsgV3  byte = 16
	respChannelMsgV3  byte = 17
	respChannelInfo   byte = 18
	respAdvertPath    byte = 22
	respStats         byte = 24
	respChannelData   byte = 27
	respDefaultScope  byte = 28
	respCLIReply      byte = 29
)

// Push codes (PUSH_CODE_*).
const (
	pushAdvert        byte = 0x80
	pushPathUpdated   byte = 0x81
	pushSendConfirmed byte = 0x82
	pushMsgWaiting    byte = 0x83
	pushRawData       byte = 0x84
	pushLoginSuccess  byte = 0x85
	pushLoginFail     byte = 0x86
	pushStatusResp    byte = 0x87
	pushRxLog         byte = 0x88
	pushTraceData     byte = 0x89
	pushNewAdvert     byte = 0x8A
	pushTelemetry     byte = 0x8B
	pushBinaryResp    byte = 0x8C
	pushPathDiscovery byte = 0x8D
	pushControlData   byte = 0x8E
)

// Error codes carried by RESP_CODE_ERR.
const (
	errCodeUnsupported byte = 1
	errCodeNotFound    byte = 2
)

// Node roles (ADV_TYPE_*).
const (
	AdvTypeNone     byte = 0
	AdvTypeChat     byte = 1
	AdvTypeRepeater byte = 2
	AdvTypeRoom     byte = 3
	AdvTypeSensor   byte = 4
)

// Text types (TXT_TYPE_*).
const (
	TxtTypePlain       byte = 0
	TxtTypeCLIData     byte = 1
	TxtTypeSignedPlain byte = 2
	TxtTypeCLICmd      byte = 3
)

// Packet header layout (Packet.h).
const (
	routeMask        uint8 = 0x03
	payloadTypeShift       = 2
	payloadTypeMask  uint8 = 0x0F
	payloadVerShift        = 6
	payloadVerMask   uint8 = 0x03

	routeTransportFlood  uint8 = 0x00
	routeFlood           uint8 = 0x01
	routeDirect          uint8 = 0x02
	routeTransportDirect uint8 = 0x03

	payloadReq       uint8 = 0x00
	payloadResponse  uint8 = 0x01
	payloadTxtMsg    uint8 = 0x02
	payloadAck       uint8 = 0x03
	payloadAdvert    uint8 = 0x04
	payloadGrpTxt    uint8 = 0x05
	payloadGrpData   uint8 = 0x06
	payloadAnonReq   uint8 = 0x07
	payloadPath      uint8 = 0x08
	payloadTrace     uint8 = 0x09
	payloadMultipart uint8 = 0x0A
	payloadControl   uint8 = 0x0B
	payloadRawCustom uint8 = 0x0F

	pathHashCountMask byte = 0x3F
	pathHashSizeShift byte = 6
	pathHashSizeMax   byte = 4 // reserved by the firmware
	pathLenDirect     byte = 0xFF
)

// Field sizes and frame offsets.
const (
	pubKeyLen       = 32
	pubKeyPrefixLen = 6
	signerPrefixLen = 4
	textPrefixLen   = 13 // key(6) + path(1) + txt_type(1) + ts(4) + code(1)
	textPrefixV3Len = 16 // textPrefixLen + snr(1) + reserved(2)

	chanTextPrefixLen   = 8
	chanTextPrefixV3Len = 11

	contactNameLen = 32
	contactTailLen = contactNameLen + 16 // name + 4 uint32 fields
	contactMinLen  = 1 + pubKeyLen + 3 + contactTailLen

	chanNameLen        = 32
	chanSecretLen      = 16
	chanInfoFrameLen   = 1 + chanNameLen + chanSecretLen + 1
	defaultMaxChannels = 8

	scopeNameLen    = 31
	scopeKeyLen     = 16
	defaultScopeLen = 1 + scopeNameLen + scopeKeyLen

	selfInfoMinLen     = 58
	deviceInfoMinVer   = 3
	deviceInfoMinLen   = 80
	deviceInfoFirmwLen = 12
	deviceInfoModelLen = 40
	deviceInfoVerLen   = 20

	sentFrameLen     = 10
	sentRouteIdx     = 1
	sentTagIdx       = 2
	sentTimeoutIdx   = 6
	sentRouteFlood   = 1
	ackFrameLen      = 5
	ackTripFrameLen  = 9
	telemetryMinLen  = 8
	telemetryKeyIdx  = 2
	telemetryDataIdx = 8
	rxLogMinLen      = 4
	rxLogDataIdx     = 3
	rawPacketMinLen  = 2
	advertPathMinLen = 6
	batteryFrameLen  = 11
	currTimeFrameLen = 5

	statsDataIdx    = 2
	statsCoreLen    = 11
	statsRadioLen   = 14
	statsPacketsLen = 30

	telemReqReservedLen = 3

	snrScale       = 4.0
	latLonScale    = 1e6
	radioFreqScale = 1000.0
)

// Transport defaults.
const (
	defaultBaud        = 115200
	serialDataBits     = 8
	dialTimeout        = 10 * time.Second
	serialReadTimeout  = 250 * time.Millisecond
	serialReadAttempts = 512
)

// Config defaults and policy factors.
const (
	defaultDirectAttempts  = 5
	defaultFloodAttempts   = 5
	defaultChannelAttempts = 3
	defaultRequireRepeats  = 1
	defaultEchoWindow      = 8 * time.Second
	defaultCommandTimeout  = 15 * time.Second
	defaultAckSlack        = 1.25
	defaultMinWait         = 5 * time.Second
	defaultMaxWait         = 5 * time.Minute
	defaultEventBuffer     = 256
	defaultEchoBuffer      = 1024
	pendingBuffer          = 64
	drainLimit             = 512
	handshakeFactor        = 2
	contactStreamFactor    = 8
)

// Cayenne LPP type ids.
const (
	lppDigitalInput  byte = 0
	lppDigitalOutput byte = 1
	lppAnalogInput   byte = 2
	lppAnalogOutput  byte = 3
	lppGeneric       byte = 100
	lppLuminosity    byte = 101
	lppPresence      byte = 102
	lppTemperature   byte = 103
	lppRelHumidity   byte = 104
	lppAccelerometer byte = 113
	lppPressure      byte = 115
	lppVoltage       byte = 116
	lppCurrent       byte = 117
	lppFrequency     byte = 118
	lppPercentage    byte = 120
	lppAltitude      byte = 121
	lppConcentration byte = 125
	lppPower         byte = 128
	lppDistance      byte = 130
	lppEnergy        byte = 131
	lppDirection     byte = 132
	lppUnixTime      byte = 133
	lppGyrometer     byte = 134
	lppColour        byte = 135
	lppGPS           byte = 136
	lppSwitch        byte = 142
)

// LPP value widths.
const (
	lppSize1 = 1
	lppSize2 = 2
	lppSize3 = 3
	lppSize4 = 4
	lppSize6 = 6
	lppSize9 = 9

	lppCoordLen = 3
	lppCoordDiv = 10000.0
	lppAltDiv   = 100.0
	lppAxisLen  = 2
	lppAxisDiv  = 1000.0
	lppGyroDiv  = 100.0
)

// LPP divisors.
const (
	lppDiv1    = 1.0
	lppDiv2    = 2.0
	lppDiv10   = 10.0
	lppDiv100  = 100.0
	lppDiv1000 = 1000.0
)

// LPP field names and suffixes.
const (
	lppNameUnknown = "unknown_%d"
	lppSuffixLat   = ".lat"
	lppSuffixLon   = ".lon"
	lppSuffixAlt   = ".alt"
	lppSuffixX     = ".x"
	lppSuffixY     = ".y"
	lppSuffixZ     = ".z"
	lppSuffixR     = ".r"
	lppSuffixG     = ".g"
	lppSuffixB     = ".b"

	lppDefaultHumiditySize = 1
)

// MaxTextLen is the MeshCore per-message text limit, in bytes.
const MaxTextLen = 133

// Chunking used by SplitText.
const (
	chunkTagMin    = 20
	chunkTagFormat = "[%d/%d] %s"
)
