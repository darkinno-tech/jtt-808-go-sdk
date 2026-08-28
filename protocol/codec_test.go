package protocol

import (
	"errors"
	"testing"
	"time"

	"github.com/im10furry/jtt-808-go-sdk/protocol/types"
)

// TestVersionCompatibility 测试版本兼容性
func TestVersionCompatibility(t *testing.T) {
	codec := NewCodec()

	// 测试2011版本
	t.Run("Version2011", func(t *testing.T) {
		testVersion2011(t, codec)
	})

	// 测试2013版本
	t.Run("Version2013", func(t *testing.T) {
		testVersion2013(t, codec)
	})

	// 测试2019版本
	t.Run("Version2019", func(t *testing.T) {
		testVersion2019(t, codec)
	})
}

func TestDecodeStandardFramesByVersion(t *testing.T) {
	codec := NewCodec()
	locationBody := buildLocationBody()

	tests := []struct {
		name            string
		header          []byte
		body            []byte
		wantMsgID       uint16
		wantVersion     uint8
		wantPhone       string
		wantBodyLength  int
		wantSubPackage  bool
		wantEncryption  uint8
		wantMessageFlow uint16
	}{
		{
			name:            "2011 heartbeat legacy header",
			header:          legacyHeader(0x0002, 0, "13800138000", 1),
			wantMsgID:       0x0002,
			wantPhone:       "138001380000",
			wantMessageFlow: 1,
		},
		{
			name:            "2013 location legacy header",
			header:          legacyHeader(0x0200, uint16(len(locationBody)), "13800138000", 2),
			body:            locationBody,
			wantMsgID:       0x0200,
			wantPhone:       "138001380000",
			wantBodyLength:  len(locationBody),
			wantMessageFlow: 2,
		},
		{
			name: "2011 subpackage legacy header",
			header: appendPackageFields(
				legacyHeader(0x0704, types.MsgBodyPropertySubPackageFlag|2, "13800138000", 3),
				2,
				1,
			),
			body:            []byte{0x01, 0x02},
			wantMsgID:       0x0704,
			wantPhone:       "138001380000",
			wantBodyLength:  2,
			wantSubPackage:  true,
			wantMessageFlow: 3,
		},
		{
			name:            "2019 location versioned header",
			header:          version2019Header(0x0200, uint16(len(locationBody)), "13800138000", 4),
			body:            locationBody,
			wantMsgID:       0x0200,
			wantVersion:     0x02,
			wantPhone:       "13800138000000000000",
			wantBodyLength:  len(locationBody),
			wantMessageFlow: 4,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			decoded, err := codec.Decode(buildFrame(codec, tt.header, tt.body))
			if err != nil {
				t.Fatalf("Failed to decode standard frame: %v", err)
			}

			if decoded.Header.MsgID != tt.wantMsgID {
				t.Errorf("MsgID mismatch: got %04X, want %04X", decoded.Header.MsgID, tt.wantMsgID)
			}
			if decoded.Header.ProtocolVersion != tt.wantVersion {
				t.Errorf("ProtocolVersion mismatch: got %d, want %d", decoded.Header.ProtocolVersion, tt.wantVersion)
			}
			if decoded.Header.PhoneNo != tt.wantPhone {
				t.Errorf("PhoneNo mismatch: got %s, want %s", decoded.Header.PhoneNo, tt.wantPhone)
			}
			if decoded.Header.MsgBodyLength != uint16(tt.wantBodyLength) {
				t.Errorf("MsgBodyLength mismatch: got %d, want %d", decoded.Header.MsgBodyLength, tt.wantBodyLength)
			}
			if len(decoded.Body) != tt.wantBodyLength {
				t.Errorf("Body length mismatch: got %d, want %d", len(decoded.Body), tt.wantBodyLength)
			}
			if decoded.Header.SubPackage != tt.wantSubPackage {
				t.Errorf("SubPackage mismatch: got %v, want %v", decoded.Header.SubPackage, tt.wantSubPackage)
			}
			if decoded.Header.Encryption != tt.wantEncryption {
				t.Errorf("Encryption mismatch: got %d, want %d", decoded.Header.Encryption, tt.wantEncryption)
			}
			if decoded.Header.MsgFlowNo != tt.wantMessageFlow {
				t.Errorf("MsgFlowNo mismatch: got %d, want %d", decoded.Header.MsgFlowNo, tt.wantMessageFlow)
			}
		})
	}
}

func TestEncodedHeaderLengthByVersion(t *testing.T) {
	codec := NewCodec()

	tests := []struct {
		name       string
		msg        *Message
		headerSize int
	}{
		{
			name: "2011/2013 legacy header",
			msg: &Message{
				Header: &MessageHeader{
					MsgID:     0x0002,
					PhoneNo:   "13800138000",
					MsgFlowNo: 1,
				},
			},
			headerSize: 12,
		},
		{
			name: "2019 versioned header",
			msg: &Message{
				Header: &MessageHeader{
					MsgID:           0x0002,
					ProtocolVersion: 0x02,
					PhoneNo:         "13800138000",
					MsgFlowNo:       1,
				},
			},
			headerSize: 17,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			encoded, err := codec.Encode(tt.msg)
			if err != nil {
				t.Fatalf("Failed to encode message: %v", err)
			}

			unescaped, err := codec.unescape(encoded[1 : len(encoded)-1])
			if err != nil {
				t.Fatalf("Failed to unescape encoded message: %v", err)
			}
			if got := len(unescaped) - 1; got != tt.headerSize {
				t.Fatalf("Header length mismatch: got %d, want %d", got, tt.headerSize)
			}
		})
	}
}

func TestDecodeRejectsMalformedVersionCompatibilityFrames(t *testing.T) {
	codec := NewCodec()

	t.Run("legacy subpackage missing package fields", func(t *testing.T) {
		frame := buildFrame(codec, legacyHeader(0x0704, types.MsgBodyPropertySubPackageFlag, "13800138000", 1), nil)
		_, err := codec.Decode(frame)
		if !errors.Is(err, ErrInvalidMessage) {
			t.Fatalf("Expected ErrInvalidMessage, got %v", err)
		}
	})

	t.Run("2019 flag with legacy phone length", func(t *testing.T) {
		header := []byte{0x00, 0x02}
		header = appendUint16(header, types.MsgBodyPropertyVersionFlag)
		header = append(header, 0x02)
		header = append(header, codec.encodeBCD("13800138000", terminalPhoneBCDLengthLegacy)...)
		header = appendUint16(header, 1)

		_, err := codec.Decode(buildFrame(codec, header, nil))
		if !errors.Is(err, ErrInvalidMessage) {
			t.Fatalf("Expected ErrInvalidMessage, got %v", err)
		}
	})
}

// testVersion2011 测试2011版本
func testVersion2011(t *testing.T, codec *Codec) {
	// 2011版本消息头（12字节）
	// 消息ID(2) + 消息体属性(2) + 终端手机号(6) + 消息流水号(2)
	msg := &Message{
		Header: &MessageHeader{
			MsgID:           0x0200, // 位置信息汇报
			MsgBodyProperty: 0x001C, // 消息体长度28字节
			PhoneNo:         "13800138000",
			MsgFlowNo:       1,
		},
		Body: buildLocationBody(),
	}

	// 编码
	encoded, err := codec.Encode(msg)
	if err != nil {
		t.Fatalf("Failed to encode message: %v", err)
	}

	// 验证编码结果
	if len(encoded) == 0 {
		t.Fatal("Encoded message is empty")
	}

	// 验证标志位
	if encoded[0] != 0x7E || encoded[len(encoded)-1] != 0x7E {
		t.Fatal("Invalid flag bytes")
	}

	// 解码
	decoded, err := codec.Decode(encoded)
	if err != nil {
		t.Fatalf("Failed to decode message: %v", err)
	}

	// 验证解码结果
	if decoded.Header.MsgID != msg.Header.MsgID {
		t.Errorf("MsgID mismatch: got %x, want %x", decoded.Header.MsgID, msg.Header.MsgID)
	}

	// BCD编码解码后手机号会补0，所以期望值是12位
	expectedPhone := "138001380000"
	if decoded.Header.PhoneNo != expectedPhone {
		t.Errorf("PhoneNo mismatch: got %s, want %s", decoded.Header.PhoneNo, expectedPhone)
	}

	if len(decoded.Body) != len(msg.Body) {
		t.Errorf("Body length mismatch: got %d, want %d", len(decoded.Body), len(msg.Body))
	}

	t.Log("Version 2011 test passed")
}

// testVersion2013 测试2013版本
func testVersion2013(t *testing.T, codec *Codec) {
	// 2013版本与2011版本结构相同，但增加了新的消息类型
	msg := &Message{
		Header: &MessageHeader{
			MsgID:           0x0200, // 位置信息汇报
			MsgBodyProperty: 0x001C, // 消息体长度28字节
			PhoneNo:         "13800138000",
			MsgFlowNo:       1,
		},
		Body: buildLocationBody(),
	}

	// 编码
	encoded, err := codec.Encode(msg)
	if err != nil {
		t.Fatalf("Failed to encode message: %v", err)
	}

	// 解码
	decoded, err := codec.Decode(encoded)
	if err != nil {
		t.Fatalf("Failed to decode message: %v", err)
	}

	// 验证解码结果
	if decoded.Header.MsgID != msg.Header.MsgID {
		t.Errorf("MsgID mismatch: got %x, want %x", decoded.Header.MsgID, msg.Header.MsgID)
	}

	// 测试位置信息解析
	report, err := ParseLocationReport(decoded.Body)
	if err != nil {
		t.Fatalf("Failed to parse location report: %v", err)
	}

	// 验证位置信息（纬度：3990420/100000 = 39.9042）
	if report.Latitude < 39.9 || report.Latitude > 40.0 {
		t.Errorf("Latitude out of range: got %f", report.Latitude)
	}

	if report.Longitude < 116.4 || report.Longitude > 116.5 {
		t.Errorf("Longitude out of range: got %f", report.Longitude)
	}

	t.Log("Version 2013 test passed")
}

// testVersion2019 测试2019版本
func testVersion2019(t *testing.T, codec *Codec) {
	// 2019版本增加了协议版本号字段，终端手机号字段为10字节BCD
	msg := &Message{
		Header: &MessageHeader{
			MsgID:           0x0200, // 位置信息汇报
			ProtocolVersion: 0x02,   // 2019版本
			PhoneNo:         "13800138000",
			MsgFlowNo:       1,
		},
		Body: buildLocationBody(),
	}

	// 编码
	encoded, err := codec.Encode(msg)
	if err != nil {
		t.Fatalf("Failed to encode message: %v", err)
	}

	// 解码
	decoded, err := codec.Decode(encoded)
	if err != nil {
		t.Fatalf("Failed to decode message: %v", err)
	}

	// 验证解码结果
	if decoded.Header.MsgID != msg.Header.MsgID {
		t.Errorf("MsgID mismatch: got %x, want %x", decoded.Header.MsgID, msg.Header.MsgID)
	}

	if decoded.Header.ProtocolVersion != 0x02 {
		t.Errorf("ProtocolVersion mismatch: got %d, want 2", decoded.Header.ProtocolVersion)
	}

	expectedPhone := "13800138000000000000"
	if decoded.Header.PhoneNo != expectedPhone {
		t.Errorf("PhoneNo mismatch: got %s, want %s", decoded.Header.PhoneNo, expectedPhone)
	}

	// 测试位置信息解析
	report, err := ParseLocationReport(decoded.Body)
	if err != nil {
		t.Fatalf("Failed to parse location report: %v", err)
	}

	// 验证位置信息（纬度：3990420/100000 = 39.9042）
	if report.Latitude < 39.9 || report.Latitude > 40.0 {
		t.Errorf("Latitude out of range: got %f", report.Latitude)
	}

	if report.Longitude < 116.4 || report.Longitude > 116.5 {
		t.Errorf("Longitude out of range: got %f", report.Longitude)
	}

	t.Log("Version 2019 test passed")
}

// TestTerminalRegister 测试终端注册消息
func TestTerminalRegister(t *testing.T) {
	codec := NewCodec()

	// 构造终端注册消息体
	body := buildTerminalRegisterBody()

	msg := &Message{
		Header: &MessageHeader{
			MsgID:           0x0100, // 终端注册
			MsgBodyProperty: uint16(len(body)),
			PhoneNo:         "13800138000",
			MsgFlowNo:       1,
		},
		Body: body,
	}

	// 编码
	encoded, err := codec.Encode(msg)
	if err != nil {
		t.Fatalf("Failed to encode message: %v", err)
	}

	// 解码
	decoded, err := codec.Decode(encoded)
	if err != nil {
		t.Fatalf("Failed to decode message: %v", err)
	}

	// 验证解码结果
	if decoded.Header.MsgID != 0x0100 {
		t.Errorf("MsgID mismatch: got %x, want 0x0100", decoded.Header.MsgID)
	}

	// 解析终端注册信息
	reg, err := ParseTerminalRegister(decoded.Body)
	if err != nil {
		t.Fatalf("Failed to parse terminal register: %v", err)
	}

	// 验证终端注册信息
	if reg.ManufacturerID != "TEST0" {
		t.Errorf("ManufacturerID mismatch: got %s, want TEST0", reg.ManufacturerID)
	}

	if reg.TerminalType != "TEST_TERMINAL" {
		t.Errorf("TerminalType mismatch: got %s, want TEST_TERMINAL", reg.TerminalType)
	}

	if reg.PlateColor != 1 {
		t.Errorf("PlateColor mismatch: got %d, want 1", reg.PlateColor)
	}

	if reg.PlateNo != "A12345" {
		t.Errorf("PlateNo mismatch: got %s, want A12345", reg.PlateNo)
	}

	t.Log("Terminal register test passed")
}

// TestAlarmReport 测试报警消息
func TestAlarmReport(t *testing.T) {
	codec := NewCodec()

	// 构造报警消息体
	body := buildAlarmReportBody()

	msg := &Message{
		Header: &MessageHeader{
			MsgID:           0x0200, // 位置信息汇报（包含报警）
			MsgBodyProperty: uint16(len(body)),
			PhoneNo:         "13800138000",
			MsgFlowNo:       1,
		},
		Body: body,
	}

	// 编码
	encoded, err := codec.Encode(msg)
	if err != nil {
		t.Fatalf("Failed to encode message: %v", err)
	}

	// 解码
	decoded, err := codec.Decode(encoded)
	if err != nil {
		t.Fatalf("Failed to decode message: %v", err)
	}

	// 解析位置信息（包含报警）
	report, err := ParseLocationReport(decoded.Body)
	if err != nil {
		t.Fatalf("Failed to parse location report: %v", err)
	}

	// 验证报警标志
	if report.AlarmFlag&0x00000001 == 0 {
		t.Error("SOS alarm flag should be set")
	}

	if report.AlarmFlag&0x00000002 == 0 {
		t.Error("Over speed alarm flag should be set")
	}

	t.Log("Alarm report test passed")
}

// buildLocationBody 构造位置消息体
func buildLocationBody() []byte {
	body := make([]byte, 28)
	// 报警标志
	body[0] = 0x00
	body[1] = 0x00
	body[2] = 0x00
	body[3] = 0x00
	// 状态
	body[4] = 0x00
	body[5] = 0x00
	body[6] = 0x00
	body[7] = 0x02 // 已定位
	// 纬度（39.9042 * 100000 = 3990420 = 0x003CED94）
	body[8] = 0x00
	body[9] = 0x3C
	body[10] = 0xED
	body[11] = 0x94
	// 经度（116.4074 * 100000 = 11640740 = 0x00B1A0A4）
	body[12] = 0x00
	body[13] = 0xB1
	body[14] = 0xA0
	body[15] = 0xA4
	// 海拔
	body[16] = 0x00
	body[17] = 0x32
	// 速度
	body[18] = 0x00
	body[19] = 0x00
	// 方向
	body[20] = 0x00
	body[21] = 0x5A
	// 时间（2024-01-01 12:00:00）
	body[22] = 0x24
	body[23] = 0x01
	body[24] = 0x01
	body[25] = 0x12
	body[26] = 0x00
	body[27] = 0x00
	return body
}

// buildTerminalRegisterBody 构造终端注册消息体
func buildTerminalRegisterBody() []byte {
	// 终端注册消息体：省域ID(2) + 市县域ID(2) + 制造商ID(5) + 终端型号(30) + 终端ID(7) + 车牌颜色(1) + 车牌号(变长)
	body := make([]byte, 54) // 2+2+5+30+7+1+7 = 54
	// 省域ID
	body[0] = 0x00
	body[1] = 0x01
	// 市县域ID
	body[2] = 0x00
	body[3] = 0x01
	// 制造商ID
	copy(body[4:9], "TEST0")
	// 终端型号
	copy(body[9:39], "TEST_TERMINAL")
	// 终端ID
	copy(body[39:46], "1234567")
	// 车牌颜色
	body[46] = 0x01
	// 车牌号
	copy(body[47:53], "A12345")
	return body
}

// buildAlarmReportBody 构造报警消息体
func buildAlarmReportBody() []byte {
	body := make([]byte, 28)
	// 报警标志（SOS + 超速）
	body[0] = 0x00
	body[1] = 0x00
	body[2] = 0x00
	body[3] = 0x03
	// 状态
	body[4] = 0x00
	body[5] = 0x00
	body[6] = 0x00
	body[7] = 0x02
	// 纬度（39.9042 * 100000 = 3990420 = 0x003CED94）
	body[8] = 0x00
	body[9] = 0x3C
	body[10] = 0xED
	body[11] = 0x94
	// 经度（116.4074 * 100000 = 11640740 = 0x00B1A0A4）
	body[12] = 0x00
	body[13] = 0xB1
	body[14] = 0xA0
	body[15] = 0xA4
	// 海拔
	body[16] = 0x00
	body[17] = 0x32
	// 速度
	body[18] = 0x00
	body[19] = 0x00
	// 方向
	body[20] = 0x00
	body[21] = 0x5A
	// 时间
	body[22] = 0x24
	body[23] = 0x01
	body[24] = 0x01
	body[25] = 0x12
	body[26] = 0x00
	body[27] = 0x00
	return body
}

func legacyHeader(msgID, property uint16, phone string, flowNo uint16) []byte {
	codec := NewCodec()
	header := appendUint16(nil, msgID)
	header = appendUint16(header, property)
	header = append(header, codec.encodeBCD(phone, terminalPhoneBCDLengthLegacy)...)
	header = appendUint16(header, flowNo)
	return header
}

func version2019Header(msgID, property uint16, phone string, flowNo uint16) []byte {
	codec := NewCodec()
	header := appendUint16(nil, msgID)
	header = appendUint16(header, property|types.MsgBodyPropertyVersionFlag)
	header = append(header, 0x02)
	header = append(header, codec.encodeBCD(phone, terminalPhoneBCDLength2019)...)
	header = appendUint16(header, flowNo)
	return header
}

func appendPackageFields(header []byte, total, sequence uint16) []byte {
	header = appendUint16(header, total)
	header = appendUint16(header, sequence)
	return header
}

func appendUint16(dst []byte, value uint16) []byte {
	return append(dst, byte(value>>8), byte(value))
}

func buildFrame(codec *Codec, header, body []byte) []byte {
	checksum := codec.calculateChecksum(header, body)
	frame := []byte{0x7E}
	frame = append(frame, codec.escape(header)...)
	frame = append(frame, codec.escape(body)...)
	frame = append(frame, codec.escape([]byte{checksum})...)
	frame = append(frame, 0x7E)
	return frame
}

// TestEncodeDecode 测试编解码
func TestEncodeDecode(t *testing.T) {
	codec := NewCodec()

	// 测试消息
	msg := &Message{
		Header: &MessageHeader{
			MsgID:           0x0002, // 心跳
			MsgBodyProperty: 0x0000,
			PhoneNo:         "13800138000",
			MsgFlowNo:       1,
		},
		Body: []byte{},
	}

	// 编码
	encoded, err := codec.Encode(msg)
	if err != nil {
		t.Fatalf("Failed to encode message: %v", err)
	}

	// 解码
	decoded, err := codec.Decode(encoded)
	if err != nil {
		t.Fatalf("Failed to decode message: %v", err)
	}

	// 验证
	if decoded.Header.MsgID != msg.Header.MsgID {
		t.Errorf("MsgID mismatch: got %x, want %x", decoded.Header.MsgID, msg.Header.MsgID)
	}

	// BCD编码解码后手机号会补0，所以期望值是12位
	expectedPhone := "138001380000"
	if decoded.Header.PhoneNo != expectedPhone {
		t.Errorf("PhoneNo mismatch: got %s, want %s", decoded.Header.PhoneNo, expectedPhone)
	}

	t.Log("Encode/decode test passed")
}

// TestChecksum 测试校验码
func TestChecksum(t *testing.T) {
	codec := NewCodec()

	// 测试消息
	msg := &Message{
		Header: &MessageHeader{
			MsgID:           0x0002,
			MsgBodyProperty: 0x0000,
			PhoneNo:         "13800138000",
			MsgFlowNo:       1,
		},
		Body: []byte{},
	}

	// 编码
	encoded, err := codec.Encode(msg)
	if err != nil {
		t.Fatalf("Failed to encode message: %v", err)
	}

	// 修改校验码
	encoded[len(encoded)-2] ^= 0xFF

	// 解码应该失败
	_, err = codec.Decode(encoded)
	if err == nil {
		t.Error("Expected checksum error, got nil")
	}

	t.Log("Checksum test passed")
}

func TestChecksumEscapedInFrame(t *testing.T) {
	codec := NewCodec()

	msg := &Message{
		Header: &MessageHeader{
			MsgID:     0x0002,
			PhoneNo:   "13800138000",
			MsgFlowNo: 1,
		},
		Body: []byte{0xD6},
	}

	encoded, err := codec.Encode(msg)
	if err != nil {
		t.Fatalf("Failed to encode message: %v", err)
	}

	for i, b := range encoded[1 : len(encoded)-1] {
		if b == 0x7E {
			t.Fatalf("Frame body contains unescaped flag at index %d: %X", i, encoded)
		}
	}

	decoded, err := codec.Decode(encoded)
	if err != nil {
		t.Fatalf("Failed to decode message with escaped checksum: %v", err)
	}
	if len(decoded.Body) != 1 || decoded.Body[0] != 0xD6 {
		t.Fatalf("Decoded body mismatch: %X", decoded.Body)
	}
}

func TestDecodeRejectsInvalidEscape(t *testing.T) {
	codec := NewCodec()

	_, err := codec.Decode([]byte{0x7E, 0x7D, 0x03, 0x7E})
	if !errors.Is(err, ErrInvalidEscape) {
		t.Fatalf("Expected ErrInvalidEscape, got %v", err)
	}
}

func TestDecodeRejectsBodyLengthMismatch(t *testing.T) {
	codec := NewCodec()

	headerBytes, err := codec.encodeHeader(&MessageHeader{
		MsgID:           0x0002,
		MsgBodyProperty: 0x0002,
		PhoneNo:         "13800138000",
		MsgFlowNo:       1,
	})
	if err != nil {
		t.Fatalf("Failed to encode header: %v", err)
	}

	body := []byte{0x01}
	checksum := codec.calculateChecksum(headerBytes, body)
	frame := []byte{0x7E}
	frame = append(frame, codec.escape(headerBytes)...)
	frame = append(frame, codec.escape(body)...)
	frame = append(frame, codec.escape([]byte{checksum})...)
	frame = append(frame, 0x7E)

	_, err = codec.Decode(frame)
	if !errors.Is(err, ErrInvalidMessage) {
		t.Fatalf("Expected ErrInvalidMessage, got %v", err)
	}
}

// TestEscape 测试转义
func TestEscape(t *testing.T) {
	codec := NewCodec()

	// 测试数据包含需要转义的字节
	data := []byte{0x7E, 0x7D, 0x01, 0x02}

	// 转义
	escaped := codec.escape(data)

	// 验证转义结果
	expected := []byte{0x7D, 0x02, 0x7D, 0x01, 0x01, 0x02}
	if len(escaped) != len(expected) {
		t.Errorf("Escaped length mismatch: got %d, want %d", len(escaped), len(expected))
	}

	for i, b := range escaped {
		if b != expected[i] {
			t.Errorf("Escaped byte mismatch at %d: got %x, want %x", i, b, expected[i])
		}
	}

	// 反转义
	unescaped, err := codec.unescape(escaped)
	if err != nil {
		t.Fatalf("Failed to unescape data: %v", err)
	}

	// 验证反转义结果
	if len(unescaped) != len(data) {
		t.Errorf("Unescaped length mismatch: got %d, want %d", len(unescaped), len(data))
	}

	for i, b := range unescaped {
		if b != data[i] {
			t.Errorf("Unescaped byte mismatch at %d: got %x, want %x", i, b, data[i])
		}
	}

	t.Log("Escape test passed")
}

// TestBCD 测试BCD编码
func TestBCD(t *testing.T) {
	codec := NewCodec()

	// 测试BCD编码
	phone := "13800138000"
	encoded := codec.encodeBCD(phone, 6)

	// 验证编码结果（11位手机号，BCD编码6字节，末尾补0）
	expected := []byte{0x13, 0x80, 0x01, 0x38, 0x00, 0x00}
	if len(encoded) != len(expected) {
		t.Errorf("Encoded length mismatch: got %d, want %d", len(encoded), len(expected))
	}

	for i, b := range encoded {
		if b != expected[i] {
			t.Errorf("Encoded byte mismatch at %d: got %x, want %x", i, b, expected[i])
		}
	}

	// 测试BCD解码（11位手机号，BCD解码后为12位，末尾补0）
	decoded := codec.decodeBCD(encoded)

	// 验证解码结果
	expectedPhone := "138001380000" // BCD解码后为12位
	if decoded != expectedPhone {
		t.Errorf("Decoded mismatch: got %s, want %s", decoded, expectedPhone)
	}

	t.Log("BCD test passed")
}

// TestParseBCDTime 测试BCD时间解析
func TestParseBCDTime(t *testing.T) {
	// 测试时间解析（BCD编码：24-01-01-12-00-00）
	data := []byte{0x24, 0x01, 0x01, 0x12, 0x00, 0x00}
	parsed := parseBCDTime(data)

	// 验证解析结果
	expected := time.Date(2024, 1, 1, 12, 0, 0, 0, time.Local)
	if !parsed.Equal(expected) {
		t.Errorf("Parsed time mismatch: got %v, want %v", parsed, expected)
	}

	t.Log("BCD time test passed")
}

func TestParseTerminalRegisterRejectsShortBody(t *testing.T) {
	_, err := ParseTerminalRegister(make([]byte, terminalRegisterMinLength-1))
	if err == nil {
		t.Fatal("Expected error for short terminal register body")
	}
}
