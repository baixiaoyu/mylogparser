package column

import (
	"bytes"
	"reflect"
	"strconv"
	"strings"

	"golang.org/x/text/encoding"
	"golang.org/x/text/encoding/charmap"
	"golang.org/x/text/encoding/simplifiedchinese"
)

type charsetEncoding map[string]encoding.Encoding

var charsetEncodingMap charsetEncoding

func init() {
	charsetEncodingMap = make(map[string]encoding.Encoding)
	// Begin mappings
	charsetEncodingMap["latin1"] = charmap.Windows1252
	charsetEncodingMap["gbk"] = simplifiedchinese.GBK
}

const maxMediumintUnsigned int32 = 16777215

type TimezoneConversion struct {
	ToTimezone string
}

type ColumnType int

const (
	UnknownColumnType ColumnType = iota
	TimestampColumnType
	DateTimeColumnType
	EnumColumnType
	MediumIntColumnType
	JSONColumnType
	FloatColumnType
	BinaryColumnType
)

type Column struct {
	Name       string
	IsUnsigned bool
	Charset    string
	Type       ColumnType

	// add Octet length for binary type, fix bytes with suffix "00" get clipped in mysql binlog.
	// https://github.com/github/gh-ost/issues/909
	BinaryOctetLength  uint
	timezoneConversion *TimezoneConversion
}

type ColumnsMap map[string]int

func NewEmptyColumnsMap() ColumnsMap {
	columnsMap := make(map[string]int)
	return ColumnsMap(columnsMap)
}

func NewColumnsMap(orderedColumns []Column) ColumnsMap {
	columnsMap := NewEmptyColumnsMap()
	for i, column := range orderedColumns {
		columnsMap[column.Name] = i
	}
	return columnsMap
}

type ColumnList struct {
	Columns  []Column
	Ordinals ColumnsMap
}

func NewColumns(names []string) []Column {
	result := make([]Column, len(names))
	for i := range names {
		result[i].Name = names[i]
	}
	return result
}

func ParseColumns(names string) []Column {
	namesArray := strings.Split(names, ",")
	return NewColumns(namesArray)
}

// NewColumnList creates an object given ordered list of column names
func NewColumnList(names []string) *ColumnList {
	result := &ColumnList{
		Columns: NewColumns(names),
	}
	result.Ordinals = NewColumnsMap(result.Columns)
	return result
}

// ParseColumnList parses a comma delimited list of column names
func ParseColumnList(names string) *ColumnList {
	result := &ColumnList{
		Columns: ParseColumns(names),
	}
	result.Ordinals = NewColumnsMap(result.Columns)
	return result
}

func (this *ColumnList) GetColumns() []Column {
	return this.Columns
}

func (this *ColumnList) Names() []string {
	names := make([]string, len(this.Columns))
	for i := range this.Columns {
		names[i] = this.Columns[i].Name
	}
	return names
}

func (this *ColumnList) GetColumn(columnName string) *Column {
	if ordinal, ok := this.Ordinals[columnName]; ok {
		return &this.Columns[ordinal]
	}
	return nil
}

func (this *ColumnList) SetUnsigned(columnName string) {
	this.GetColumn(columnName).IsUnsigned = true
}

func (this *ColumnList) IsUnsigned(columnName string) bool {
	return this.GetColumn(columnName).IsUnsigned
}

func (this *ColumnList) SetCharset(columnName string, charset string) {
	this.GetColumn(columnName).Charset = charset
}

func (this *ColumnList) GetCharset(columnName string) string {
	return this.GetColumn(columnName).Charset
}

func (this *ColumnList) SetColumnType(columnName string, columnType ColumnType) {
	this.GetColumn(columnName).Type = columnType
}

func (this *ColumnList) GetColumnType(columnName string) ColumnType {
	return this.GetColumn(columnName).Type
}

func (this *ColumnList) SetConvertDatetimeToTimestamp(columnName string, toTimezone string) {
	this.GetColumn(columnName).timezoneConversion = &TimezoneConversion{ToTimezone: toTimezone}
}

func (this *ColumnList) HasTimezoneConversion(columnName string) bool {
	return this.GetColumn(columnName).timezoneConversion != nil
}

func (this *ColumnList) String() string {
	return strings.Join(this.Names(), ",")
}

func (this *ColumnList) Equals(other *ColumnList) bool {
	return reflect.DeepEqual(this.Columns, other.Columns)
}

func (this *ColumnList) EqualsByNames(other *ColumnList) bool {
	return reflect.DeepEqual(this.Names(), other.Names())
}

// IsSubsetOf returns 'true' when column names of this list are a subset of
// another list, in arbitrary order (order agnostic)
func (this *ColumnList) IsSubsetOf(other *ColumnList) bool {
	for _, column := range this.Columns {
		if _, exists := other.Ordinals[column.Name]; !exists {
			return false
		}
	}
	return true
}

func (this *ColumnList) Len() int {
	return len(this.Columns)
}

func (this *Column) ConvertArg(arg interface{}, isUniqueKeyColumn bool) interface{} {
	if s, ok := arg.(string); ok {
		// string, charset conversion
		if encoding, ok := charsetEncodingMap[this.Charset]; ok {
			arg, _ = encoding.NewDecoder().String(s)
		}

		if this.Type == BinaryColumnType && isUniqueKeyColumn {
			arg2Bytes := []byte(arg.(string))
			size := len(arg2Bytes)
			if uint(size) < this.BinaryOctetLength {
				buf := bytes.NewBuffer(arg2Bytes)
				for i := uint(0); i < (this.BinaryOctetLength - uint(size)); i++ {
					buf.Write([]byte{0})
				}
				arg = buf.String()
			}
		}

		return arg
	}

	if this.IsUnsigned {
		if i, ok := arg.(int8); ok {
			return uint8(i)
		}
		if i, ok := arg.(int16); ok {
			return uint16(i)
		}
		if i, ok := arg.(int32); ok {
			if this.Type == MediumIntColumnType {
				// problem with mediumint is that it's a 3-byte type. There is no compatible golang type to match that.
				// So to convert from negative to positive we'd need to convert the value manually
				if i >= 0 {
					return i
				}
				return uint32(maxMediumintUnsigned + i + 1)
			}
			return uint32(i)
		}
		if i, ok := arg.(int64); ok {
			return strconv.FormatUint(uint64(i), 10)
		}
		if i, ok := arg.(int); ok {
			return uint(i)
		}
	}
	return arg
}
