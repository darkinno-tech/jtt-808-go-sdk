package protocol

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/darkinno-tech/jtt-808-go-sdk/core"
	"github.com/darkinno-tech/jtt-808-go-sdk/protocol/types"
)

var (
	// ErrInvalidMessage 无效消息
	ErrInvalidMessage = errors.New("invalid message")
	// ErrInvalidChecksum 无效校验码
	ErrInvalidChecksum = errors.New("invalid checksum")
	// ErrMessageTooLong 消息过长
	ErrMessageTooLong = errors.New("message too long")
	// ErrInvalidFlag 无效标志位
	ErrInvalidFlag = errors.New("invalid flag")
	// ErrInvalidEscape 无效转义序列
	ErrInvalidEscape = errors.New("invalid escape sequence")
)

const (
	terminalPhoneBCDLengthLegacy = 6
	terminalPhoneBCDLength2019   = 10
	terminalRegisterMinLength    = 47
)

// Message 消息结构（类型别名，指向 core.Message）
type Message = core.Message

// MessageHeader 消息头结构（类型别名，指向 core.MessageHeader）
type MessageHeader = core.MessageHeader

// Codec 编解码器
type Codec struct {
	pool sync.Pool
}

// NewCodec 创建编解码器
func NewCodec() *Codec {
	return &Codec{
		pool: sync.Pool{
			New: func() interface{} {
				return bytes.NewBuffer(make([]byte, 0, 4096))
			},
		},
	}
}

// Encode 编码消息
func (c *Codec) Encode(msg *Message) ([]byte, error) {
	if msg == nil || msg.Header == nil {
		return nil, ErrInvalidMessage
	}

	buf := c.pool.Get().(*bytes.Buffer)
	defer func() {
		buf.Reset()
		c.pool.Put(buf)
	}()

	// 编码消息头
	headerBytes, err := c.encodeHeader(msg.Header)
	if err != nil {
		return nil, err
	}

	// 计算消息体属性
	bodyLen := len(msg.Body)
	if bodyLen > 1023 {
		return nil, ErrMessageTooLong
	}

	// 更新消息体属性
	msg.Header.MsgBodyProperty = uint16(bodyLen) & types.MsgBodyPropertyLengthMask
	if msg.Header.SubPackage {
		msg.Header.MsgBodyProperty |= types.MsgBodyPropertySubPackageFlag
	}
	msg.Header.MsgBodyProperty |= uint16(msg.Header.Encryption&0x07) << 10
	// 保留协议版本标志位
	if msg.Header.ProtocolVersion > 0 {
		msg.Header.MsgBodyProperty |= types.MsgBodyPropertyVersionFlag
	}

	// 重新编码消息头（属性可能已更新）
	headerBytes, err = c.encodeHeader(msg.Header)
	if err != nil {
		return nil, err
	}

	// 计算校验码
	checksum := c.calculateChecksum(headerBytes, msg.Body)

	// 组装消息
	buf.WriteByte(types.ProtocolFlag)
	buf.Write(c.escape(headerBytes))
	buf.Write(c.escape(msg.Body))
	buf.Write(c.escape([]byte{checksum}))
	buf.WriteByte(types.ProtocolFlag)

	result := make([]byte, buf.Len())
	copy(result, buf.Bytes())
	return result, nil
}

// Decode 解码消息
func (c *Codec) Decode(data []byte) (*Message, error) {
	if len(data) < 2 {
		return nil, ErrInvalidMessage
	}

	// 检查标志位
	if data[0] != types.ProtocolFlag || data[len(data)-1] != types.ProtocolFlag {
		return nil, ErrInvalidFlag
	}

	// 去除标志位并反转义
	inner := data[1 : len(data)-1]
	unescaped, err := c.unescape(inner)
	if err != nil {
		return nil, err
	}

	// 检查最小长度（消息头12字节 + 校验码1字节）
	if len(unescaped) < 13 {
		return nil, ErrInvalidMessage
	}

	// 验证校验码
	expectedChecksum := unescaped[len(unescaped)-1]
	actualChecksum := c.calculateChecksum(unescaped[:len(unescaped)-1], nil)
	if expectedChecksum != actualChecksum {
		return nil, ErrInvalidChecksum
	}

	// 解析消息头
	header, headerLen, err := c.decodeHeader(unescaped[:len(unescaped)-1])
	if err != nil {
		return nil, err
	}

	// 解析消息体
	bodyStart := headerLen
	bodyEnd := len(unescaped) - 1
	if bodyStart > bodyEnd {
		return nil, ErrInvalidMessage
	}
	if bodyEnd-bodyStart != int(header.MsgBodyLength) {
		return nil, ErrInvalidMessage
	}
	body := make([]byte, bodyEnd-bodyStart)
	copy(body, unescaped[bodyStart:bodyEnd])

	return &Message{
		Header: header,
		Body:   body,
		Raw:    data,
	}, nil
}

// encodeHeader 编码消息头
func (c *Codec) encodeHeader(header *MessageHeader) ([]byte, error) {
	buf := c.pool.Get().(*bytes.Buffer)
	defer func() {
		buf.Reset()
		c.pool.Put(buf)
	}()

	// 消息ID（2字节）
	if err := binary.Write(buf, binary.BigEndian, header.MsgID); err != nil {
		return nil, err
	}

	// 消息体属性（2字节）
	if err := binary.Write(buf, binary.BigEndian, header.MsgBodyProperty); err != nil {
		return nil, err
	}

	// 协议版本号（1字节，2019版本）
	if header.ProtocolVersion > 0 {
		if err := buf.WriteByte(header.ProtocolVersion); err != nil {
			return nil, err
		}
	}

	phoneLen := terminalPhoneBCDLengthLegacy
	if header.ProtocolVersion > 0 {
		phoneLen = terminalPhoneBCDLength2019
	}

	// 终端手机号（BCD编码）
	phoneBytes := c.encodeBCD(header.PhoneNo, phoneLen)
	if _, err := buf.Write(phoneBytes); err != nil {
		return nil, err
	}

	// 消息流水号（2字节）
	if err := binary.Write(buf, binary.BigEndian, header.MsgFlowNo); err != nil {
		return nil, err
	}

	// 消息包封装（如果分包）
	if header.SubPackage {
		// 消息包总数（2字节）
		if err := binary.Write(buf, binary.BigEndian, uint16(1)); err != nil {
			return nil, err
		}
		// 包序号（2字节）
		if err := binary.Write(buf, binary.BigEndian, uint16(1)); err != nil {
			return nil, err
		}
	}

	result := make([]byte, buf.Len())
	copy(result, buf.Bytes())
	return result, nil
}

// decodeHeader 解码消息头
func (c *Codec) decodeHeader(data []byte) (*MessageHeader, int, error) {
	if len(data) < 12 {
		return nil, 0, ErrInvalidMessage
	}

	header := &MessageHeader{}
	offset := 0

	// 消息ID（2字节）
	header.MsgID = binary.BigEndian.Uint16(data[offset : offset+2])
	offset += 2

	// 消息体属性（2字节）
	header.MsgBodyProperty = binary.BigEndian.Uint16(data[offset : offset+2])
	offset += 2

	// 检查是否包含协议版本号（2019版本）
	if header.MsgBodyProperty&types.MsgBodyPropertyVersionFlag != 0 {
		if len(data) < offset+1 {
			return nil, 0, ErrInvalidMessage
		}
		header.ProtocolVersion = data[offset]
		offset++
	}

	phoneLen := terminalPhoneBCDLengthLegacy
	if header.ProtocolVersion > 0 {
		phoneLen = terminalPhoneBCDLength2019
	}

	if len(data) < offset+phoneLen+2 {
		return nil, 0, ErrInvalidMessage
	}

	// 终端手机号（BCD编码）
	header.PhoneNo = c.decodeBCD(data[offset : offset+phoneLen])
	offset += phoneLen

	// 消息流水号（2字节）
	header.MsgFlowNo = binary.BigEndian.Uint16(data[offset : offset+2])
	offset += 2

	// 解析消息体属性
	header.MsgBodyLength = header.MsgBodyProperty & types.MsgBodyPropertyLengthMask
	header.Encryption = uint8((header.MsgBodyProperty & types.MsgBodyPropertyEncryptionMask) >> 10)
	header.SubPackage = header.MsgBodyProperty&types.MsgBodyPropertySubPackageFlag != 0

	// 如果分包，跳过消息包封装
	if header.SubPackage {
		if len(data) < offset+4 {
			return nil, 0, ErrInvalidMessage
		}
		offset += 4 // 消息包总数(2) + 包序号(2)
	}

	return header, offset, nil
}

// calculateChecksum 计算校验码
func (c *Codec) calculateChecksum(data ...[]byte) byte {
	var checksum byte
	for _, d := range data {
		for _, b := range d {
			checksum ^= b
		}
	}
	return checksum
}

// escape 转义
func (c *Codec) escape(data []byte) []byte {
	buf := c.pool.Get().(*bytes.Buffer)
	defer func() {
		buf.Reset()
		c.pool.Put(buf)
	}()

	for _, b := range data {
		if escaped, ok := types.EscapeMap[b]; ok {
			buf.WriteByte(types.ProtocolEscape)
			buf.WriteByte(escaped)
		} else {
			buf.WriteByte(b)
		}
	}

	result := make([]byte, buf.Len())
	copy(result, buf.Bytes())
	return result
}

// unescape 反转义
func (c *Codec) unescape(data []byte) ([]byte, error) {
	buf := c.pool.Get().(*bytes.Buffer)
	defer func() {
		buf.Reset()
		c.pool.Put(buf)
	}()

	for i := 0; i < len(data); i++ {
		if data[i] == types.ProtocolEscape {
			if i+1 >= len(data) {
				return nil, ErrInvalidEscape
			}
			if unescaped, ok := types.UnescapeMap[data[i+1]]; ok {
				buf.WriteByte(unescaped)
				i++ // 跳过下一个字节
			} else {
				return nil, ErrInvalidEscape
			}
		} else {
			buf.WriteByte(data[i])
		}
	}

	result := make([]byte, buf.Len())
	copy(result, buf.Bytes())
	return result, nil
}

// encodeBCD BCD编码
func (c *Codec) encodeBCD(s string, length int) []byte {
	result := make([]byte, length)
	for i := 0; i < length && i*2 < len(s); i++ {
		high := byte(0)
		low := byte(0)
		if i*2 < len(s) {
			high = s[i*2] - '0'
		}
		if i*2+1 < len(s) {
			low = s[i*2+1] - '0'
		}
		result[i] = (high << 4) | low
	}
	return result
}

// decodeBCD BCD解码
func (c *Codec) decodeBCD(data []byte) string {
	result := make([]byte, 0, len(data)*2)
	for _, b := range data {
		high := (b >> 4) & 0x0F
		low := b & 0x0F
		if high <= 9 {
			result = append(result, '0'+high)
		}
		if low <= 9 {
			result = append(result, '0'+low)
		}
	}
	return string(result)
}

// ParseLocationReport 解析位置信息上报
func ParseLocationReport(body []byte) (*core.LocationReport, error) {
	if len(body) < 28 {
		return nil, fmt.Errorf("invalid location report body length: %d", len(body))
	}

	report := &core.LocationReport{}
	offset := 0

	// 报警标志（4字节）
	report.AlarmFlag = binary.BigEndian.Uint32(body[offset : offset+4])
	offset += 4

	// 状态（4字节）
	report.Status = binary.BigEndian.Uint32(body[offset : offset+4])
	offset += 4

	// 纬度（4字节，单位：1/100000度）
	lat := binary.BigEndian.Uint32(body[offset : offset+4])
	report.Latitude = float64(lat) / 100000.0
	offset += 4

	// 经度（4字节，单位：1/100000度）
	lng := binary.BigEndian.Uint32(body[offset : offset+4])
	report.Longitude = float64(lng) / 100000.0
	offset += 4

	// 海拔高度（2字节，单位：米）
	report.Altitude = binary.BigEndian.Uint16(body[offset : offset+2])
	offset += 2

	// 速度（2字节，单位：1/10公里/小时）
	report.Speed = binary.BigEndian.Uint16(body[offset : offset+2])
	offset += 2

	// 方向（2字节，0-359，正北为0，顺时针）
	report.Direction = binary.BigEndian.Uint16(body[offset : offset+2])
	offset += 2

	// 时间（6字节，BCD编码，YY-MM-DD-hh-mm-ss）
	report.Time = parseBCDTime(body[offset : offset+6])

	return report, nil
}

// ParseTerminalRegister 解析终端注册
func ParseTerminalRegister(body []byte) (*core.TerminalRegister, error) {
	if len(body) < terminalRegisterMinLength {
		return nil, fmt.Errorf("invalid terminal register body length: %d", len(body))
	}

	reg := &core.TerminalRegister{}
	offset := 0

	// 省域ID（2字节）
	reg.ProvinceID = binary.BigEndian.Uint16(body[offset : offset+2])
	offset += 2

	// 市县域ID（2字节）
	reg.CityID = binary.BigEndian.Uint16(body[offset : offset+2])
	offset += 2

	// 制造商ID（5字节）
	reg.ManufacturerID = string(body[offset : offset+5])
	offset += 5

	// 终端型号（30字节）
	reg.TerminalType = string(bytes.TrimRight(body[offset:offset+30], "\x00"))
	offset += 30

	// 终端ID（7字节）
	reg.TerminalID = string(bytes.TrimRight(body[offset:offset+7], "\x00"))
	offset += 7

	// 车牌颜色（1字节）
	reg.PlateColor = body[offset]
	offset++

	// 车牌号（变长）
	if offset < len(body) {
		reg.PlateNo = string(bytes.TrimRight(body[offset:], "\x00"))
	}

	return reg, nil
}

// parseBCDTime 解析BCD时间
func parseBCDTime(data []byte) time.Time {
	if len(data) < 6 {
		return time.Time{}
	}

	// BCD编码：高4位是十位，低4位是个位
	year := 2000 + int(data[0]>>4)*10 + int(data[0]&0x0F)
	month := time.Month(int(data[1]>>4)*10 + int(data[1]&0x0F))
	day := int(data[2]>>4)*10 + int(data[2]&0x0F)
	hour := int(data[3]>>4)*10 + int(data[3]&0x0F)
	minute := int(data[4]>>4)*10 + int(data[4]&0x0F)
	second := int(data[5]>>4)*10 + int(data[5]&0x0F)

	return time.Date(year, month, day, hour, minute, second, 0, time.Local)
}
