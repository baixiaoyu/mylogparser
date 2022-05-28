package tosql

import (
	"context"
	"database/sql"
	"fmt"
	"mylogparser/builder"
	"mylogparser/column"
	"mylogparser/util"
	"strconv"
	"strings"
	"time"

	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/replication"
	_ "github.com/go-sql-driver/mysql"
	"github.com/openark/golib/sqlutils"
)

var TimeFormat = "2006-01-02 15:04:05"
var SchemaName string
var TableName string

type BinLog2SQL struct {
	Host           string
	Port           int
	User           string
	Passwd         string
	Start_file     string
	Start_pos      int
	End_file       string
	End_pos        int
	Start_time     string
	Stop_time      string
	table          string
	No_pk          bool
	Flashback      bool
	Stop_never     bool
	Sleep_interval int
	Only_dml       bool
	Sql_type       string
	BinLogList     []string
	ServerId       int64
	DB             *sql.DB
	ColumnList     *column.ColumnList
	Mode           string
}

func NewBinLog2SQL(Host string, Port int, User string, Passwd string, Start_file string, Start_pos int, End_file string, End_pos int, Start_time string, Stop_time string, table string, No_pk bool, Flashback bool, Stop_never bool, Sleep_interval int, Only_dml bool, Sql_type string, ServerID int64, mode string) (*BinLog2SQL, error) {

	var (
		binLogIndex []string
		binLogList  []string
		// binLog      string
		// size        int
	)
	Connection_settings := fmt.Sprintf("%s:%s@tcp(%s:%d)/%s?&autocommit=true&charset=utf8mb4,utf8,latin1", User, Passwd, Host, Port, "mysql")
	// Connection_settings := fmt.Sprintf("%s:%s@%s:%d/%s", User, Passwd, Host, Port, "mysql")
	if Start_time != "" && Start_pos != 4 {
		return nil, fmt.Errorf("start_time and start_pos can't use at same time")
	}
	if End_pos != 0 && Stop_time != "" {
		return nil, fmt.Errorf("stop_time and end_pos can't use at same time")
	}

	fmt.Println(Connection_settings)

	db, err := sql.Open("mysql", Connection_settings)
	if err != nil {
		return nil, fmt.Errorf("connect db err %v", err)
	}
	// defer db.Close()

	getBinLogs := "show master logs"
	sqlutils.QueryRowsMap(db, getBinLogs, func(m sqlutils.RowMap) error {

		binlog := m.GetString("Log_name")

		binLogIndex = append(binLogIndex, binlog)
		return nil
	})

	if !util.IsContain(binLogIndex, Start_file) {
		return nil, fmt.Errorf("start file not in  mysql ")
	}

	if End_file != "" {
		if !util.IsContain(binLogIndex, End_file) {
			return nil, fmt.Errorf("end file not in  mysql ")
		}
	}

	startLogNumber, _ := strconv.Atoi(strings.Split(Start_file, ".")[1])
	if End_file == "" {
		binLogList = append(binLogList, Start_file)
	} else {
		endLogNumber, _ := strconv.Atoi(strings.Split(End_file, ".")[1])
		for _, log := range binLogIndex {
			logNumber, _ := strconv.Atoi(strings.Split(log, ".")[1])
			if startLogNumber <= logNumber && logNumber <= endLogNumber {
				binLogList = append(binLogList, log)
			}
		}
	}

	binlog2sql := &BinLog2SQL{
		Host:           Host,
		Port:           Port,
		User:           User,
		Passwd:         Passwd,
		Start_file:     Start_file,
		Start_pos:      Start_pos,
		End_file:       End_file,
		End_pos:        End_pos,
		Start_time:     Start_time,
		Stop_time:      Stop_time,
		table:          table,
		No_pk:          No_pk,
		Flashback:      Flashback,
		Stop_never:     Stop_never,
		Sleep_interval: Sleep_interval,
		Only_dml:       Only_dml,
		Sql_type:       Sql_type,
		BinLogList:     binLogList,
		ServerId:       ServerID,
		DB:             db,
		Mode:           mode,
	}
	return binlog2sql, nil
}

// Version 0 written from MySQL 5.1.0 to 5.1.15 Version 1 written from MySQL 5.1.15 to 5.6.x Version 2 written from MySQL 5.6.x
func (binlog2sql *BinLog2SQL) concatSqlFromEvent(event *replication.BinlogEvent) error {

	// event.Dump(os.Stdout)

	if event.Header.EventType == replication.QUERY_EVENT {

		queryEvent := event.Event.(*replication.QueryEvent)

		if string(queryEvent.Query) != "BEGIN" {
			fmt.Printf("the  ddl sql is:%v \n", string(queryEvent.Query))
		}

	} else {
		template, values, err := binlog2sql.generateSQLPattern(event)
		if err != nil {
			return err
		}
		sqls, err := binlog2sql.generateSpecificSQL(event, template, values)
		if err != nil {
			return err
		}
		for _, sql := range sqls {
			fmt.Println(sql)
		}

	}
	return nil
}

func literalInt(b builder.SQLBuilder, i int64) {

	b.WriteStrings(strconv.FormatInt(i, 10))
}

func literalString(b builder.SQLBuilder, s string) {

	b.WriteRunes(builder.StringQuote)
	for _, char := range s {
		if e, ok := builder.EscapedRunes[char]; ok {
			b.Write(e)
		} else {
			b.WriteRunes(char)
		}
	}
	b.WriteRunes(builder.StringQuote)
}

func literalBool(b builder.SQLBuilder, bl bool) {
	if bl {
		b.Write(builder.True)
	} else {
		b.Write(builder.False)
	}
}

func literalNil(b builder.SQLBuilder) {

	b.Write(builder.Null)
}

// Generates SQL for a Float Value
func literalFloat(b builder.SQLBuilder, f float64) {

	b.WriteStrings(strconv.FormatFloat(f, 'f', -1, 64))
}
func EscapeName(name string) string {
	if unquoted, err := strconv.Unquote(name); err == nil {
		name = unquoted
	}
	return fmt.Sprintf("`%s`", name)
}

func (binlog2sql *BinLog2SQL) writeValueToBuilder(sb builder.SQLBuilder, val interface{}) {
	switch v := val.(type) {
	case int:
		literalInt(sb, int64(v))
	case int32:
		literalInt(sb, int64(v))
	case int64:
		literalInt(sb, v)
	case float32:
		literalFloat(sb, float64(v))
	case float64:
		literalFloat(sb, v)
	case string:
		literalString(sb, v)
	case bool:
		literalBool(sb, v)
	case time.Time:
		fmt.Printf("%v is time\n", val)
	case nil:
		literalNil(sb)
	default:
		fmt.Printf("unknow %v\n", v)

	}
}
func (binlog2sql *BinLog2SQL) generateSpecificSQL(event *replication.BinlogEvent, template string, values [][]interface{}) ([]string, error) {
	var (
		sql string
		err error
		res []string
	)

	if event.Header.EventType == replication.WRITE_ROWS_EVENTv2 {
		if binlog2sql.Flashback {
			c := binlog2sql.ColumnList.Columns
			// 事务开始，table map不会被修改
			for _, val := range values {
				sb := builder.NewSQLBuilder(false)
				sb.WriteStrings(template)
				rowlen := len(val)
				for pos := 0; pos < rowlen; pos++ {
					sb.WriteStrings(EscapeName(c[pos].Name))
					sb.WriteRunes(builder.Equals)
					binlog2sql.writeValueToBuilder(sb, val[pos])
					if pos < rowlen-1 {
						sb.WriteStrings(string(builder.And))
					}
				}
				sql, _, err = sb.ToSQL()
				res = append(res, sql)
			}
		} else {
			for _, value := range values {
				sb := builder.NewSQLBuilder(false)
				sb.WriteStrings(template)
				rowLen := len(values[0])
				if len(value) != rowLen {
					return nil, fmt.Errorf("mismatch error")
				}
				sb.WriteRunes(builder.LeftParenRune)
				for pos, val := range value {
					binlog2sql.writeValueToBuilder(sb, val)
					if pos < rowLen-1 {
						sb.WriteRunes(builder.CommaRune, builder.SpaceRune)
					}
				}
				sb.WriteRunes(builder.RightParenRune)
				sql, _, err = sb.ToSQL()
				res = append(res, sql)
			}
		}

	} else if event.Header.EventType == replication.DELETE_ROWS_EVENTv2 {
		if binlog2sql.Flashback {
			for _, value := range values {
				sb := builder.NewSQLBuilder(false)
				sb.WriteStrings(template)
				rowLen := len(values[0])
				if len(value) != rowLen {
					return nil, fmt.Errorf("mismatch error")
				}
				sb.WriteRunes(builder.LeftParenRune)
				for pos, val := range value {
					binlog2sql.writeValueToBuilder(sb, val)
					if pos < rowLen-1 {
						sb.WriteRunes(builder.CommaRune, builder.SpaceRune)
					}
				}
				sb.WriteRunes(builder.RightParenRune)
				sql, _, err = sb.ToSQL()
				res = append(res, sql)
			}
		} else {
			c := binlog2sql.ColumnList.Columns
			// 事务开始，table map不会被修改
			for _, val := range values {
				sb := builder.NewSQLBuilder(false)
				sb.WriteStrings(template)
				rowlen := len(val)
				for pos := 0; pos < rowlen; pos++ {
					sb.WriteStrings(EscapeName(c[pos].Name))
					sb.WriteRunes(builder.Equals)
					binlog2sql.writeValueToBuilder(sb, val[pos])
					if pos < rowlen-1 {
						sb.WriteStrings(string(builder.And))
					}
				}
				sql, _, err = sb.ToSQL()
				res = append(res, sql)
			}
		}

	} else if event.Header.EventType == replication.UPDATE_ROWS_EVENTv2 {
		sb := builder.NewSQLBuilder(false)
		sb.WriteStrings(template)
		c := binlog2sql.ColumnList.Columns
		rowline := len(values[0])
		for pos := 0; pos < rowline; pos++ {
			sb.WriteStrings(EscapeName(c[pos].Name))
			sb.WriteRunes(builder.Equals)
			if binlog2sql.Flashback {
				binlog2sql.writeValueToBuilder(sb, values[0][pos])
			} else {
				binlog2sql.writeValueToBuilder(sb, values[1][pos])
			}

			if pos < rowline-1 {
				sb.WriteStrings(string(builder.And))
			}
		}
		sb.WriteStrings(string(builder.Where))
		sb.WriteStrings(string(builder.SpaceRune))

		for pos := 0; pos < rowline; pos++ {
			sb.WriteStrings(c[pos].Name)
			sb.WriteRunes(builder.Equals)
			if binlog2sql.Flashback {
				binlog2sql.writeValueToBuilder(sb, values[1][pos])
			} else {
				binlog2sql.writeValueToBuilder(sb, values[0][pos])
			}
			if pos < rowline-1 {
				sb.WriteStrings(string(builder.And))
			}
		}
		sql, _, err = sb.ToSQL()
		res = append(res, sql)
	}

	return res, err

}
func (binlog2sql *BinLog2SQL) generateSQLPattern(event *replication.BinlogEvent) (template string, rows [][]interface{}, err error) {

	SchemaName = EscapeName(SchemaName)
	TableName = EscapeName(TableName)

	if event.Header.EventType == replication.WRITE_ROWS_EVENTv2 {
		if rowEvent, ok := event.Event.(*replication.RowsEvent); ok {
			rows = rowEvent.Rows
			if binlog2sql.Flashback {
				template, _, err = BuildDMLDeleteQuery(SchemaName, TableName, binlog2sql.ColumnList, rows[0])
				if err != nil {
					return "", nil, err
				}
			} else {
				template, _, err = BuildDMLInsertQuery(SchemaName, TableName, binlog2sql.ColumnList, rows[0])
				if err != nil {
					return "", nil, err
				}
			}

		}
	} else if event.Header.EventType == replication.DELETE_ROWS_EVENTv2 {
		if rowEvent, ok := event.Event.(*replication.RowsEvent); ok {
			rows = rowEvent.Rows
			if binlog2sql.Flashback {
				template, _, err = BuildDMLInsertQuery(SchemaName, TableName, binlog2sql.ColumnList, rows[0])
				if err != nil {
					return "", nil, err
				}
			} else {
				template, _, err = BuildDMLDeleteQuery(SchemaName, TableName, binlog2sql.ColumnList, rows[0])
				if err != nil {
					return "", nil, err
				}
			}

		}
	} else if event.Header.EventType == replication.UPDATE_ROWS_EVENTv2 {
		if rowEvent, ok := event.Event.(*replication.RowsEvent); ok {
			rows = rowEvent.Rows
			template, _, err = BuildDMLUpdateQuery(SchemaName, TableName, binlog2sql.ColumnList, rows[0])
			if err != nil {
				return "", nil, err
			}
		}
	}
	return template, rows, nil
}

func duplicateNames(names []string) []string {
	duplicate := make([]string, len(names))
	copy(duplicate, names)
	return duplicate
}

func BuildDMLInsertQuery(SchemaName, TableName string, columnList *column.ColumnList, row []interface{}) (result string, sharedArgs []interface{}, err error) {
	for _, column := range columnList.GetColumns() {

		tableOrdinal := columnList.Ordinals[column.Name] - 1

		arg := column.ConvertArg(row[tableOrdinal], false)
		sharedArgs = append(sharedArgs, arg)
	}

	mappedSharedColumnNames := duplicateNames(columnList.Names())
	for i := range mappedSharedColumnNames {
		mappedSharedColumnNames[i] = EscapeName(mappedSharedColumnNames[i])
	}

	result = fmt.Sprintf(`insert into %s.%s (%s) values `, SchemaName, TableName, strings.Join(mappedSharedColumnNames, ", "))

	return result, sharedArgs, nil
}

func BuildDMLDeleteQuery(SchemaName, TableName string, columnList *column.ColumnList, row []interface{}) (result string, sharedArgs []interface{}, err error) {

	result = fmt.Sprintf(`delete from  %s.%s  where  `, SchemaName, TableName)

	return result, sharedArgs, nil
}

func BuildDMLUpdateQuery(SchemaName, TableName string, columnList *column.ColumnList, row []interface{}) (result string, sharedArgs []interface{}, err error) {
	result = fmt.Sprintf(`update %s.%s  set  `, SchemaName, TableName)

	return result, sharedArgs, nil
}

func (binlog2sql *BinLog2SQL) getColumnByName(schemaName string, tableName string) (*column.ColumnList, error) {
	CL := &column.ColumnList{}
	columnList := []column.Column{}
	columnMap := column.ColumnsMap{}
	query := `
	select
			*
		from
			information_schema.columns
		where
			table_schema=?
			and table_name=?
	`
	err := sqlutils.QueryRowsMap(binlog2sql.DB, query, func(m sqlutils.RowMap) error {
		col := &column.Column{}
		columnName := m.GetString("COLUMN_NAME")
		columnType := m.GetString("COLUMN_TYPE")
		position := m.GetString("ORDINAL_POSITION")
		pos, err := strconv.Atoi(position)
		if err != nil {
			return err
		}
		columnMap[columnName] = pos
		columnOctetLength := m.GetUint("CHARACTER_OCTET_LENGTH")
		col.Name = columnName

		if strings.Contains(columnType, "unsigned") {
			col.IsUnsigned = true
		}
		if strings.Contains(columnType, "mediumint") {
			col.Type = column.MediumIntColumnType
		}
		if strings.Contains(columnType, "timestamp") {
			col.Type = column.TimestampColumnType
		}
		if strings.Contains(columnType, "datetime") {
			col.Type = column.DateTimeColumnType
		}
		if strings.Contains(columnType, "json") {
			col.Type = column.JSONColumnType
		}
		if strings.Contains(columnType, "float") {
			col.Type = column.FloatColumnType
		}
		if strings.HasPrefix(columnType, "enum") {
			col.Type = column.EnumColumnType
		}
		if strings.HasPrefix(columnType, "binary") {
			col.Type = column.BinaryColumnType
			col.BinaryOctetLength = columnOctetLength
		}
		if charset := m.GetString("CHARACTER_SET_NAME"); charset != "" {
			col.Charset = charset
		}
		columnList = append(columnList, *col)
		return nil
	}, schemaName, tableName)
	CL.Columns = columnList
	CL.Ordinals = columnMap
	return CL, err
}
func (binlog2sql *BinLog2SQL) Process_binlog() error {
	var (
		flag_last_event bool
		filename        string
	)

	columnList, err := binlog2sql.getColumnByName(SchemaName, TableName)

	if err != nil {
		return err
	}
	binlog2sql.ColumnList = columnList

	cfg := replication.BinlogSyncerConfig{
		ServerID: uint32(binlog2sql.ServerId),
		Flavor:   "mysql",
		Host:     binlog2sql.Host,
		Port:     uint16(binlog2sql.Port),
		User:     binlog2sql.User,
		Password: binlog2sql.Passwd,
	}
	syncer := replication.NewBinlogSyncer(cfg)

	streamer, _ := syncer.StartSync(mysql.Position{Name: binlog2sql.Start_file, Pos: uint32(binlog2sql.Start_pos)})

	filename = binlog2sql.Start_file
	flag_last_event = false

	for {
		ev, err := streamer.GetEvent(context.Background())
		if err != nil {
			return err
		}
		// ev.Dump(os.Stdout)
		timestamp := ev.Header.Timestamp
		logPos := ev.Header.LogPos

		if logPos < uint32(binlog2sql.Start_pos) {
			continue
		}

		tm := time.Unix(int64(timestamp), 0)
		event_time := tm.Format(TimeFormat)

		tevent_time, err := time.Parse(TimeFormat, event_time)
		if err != nil {
			return err
		}
		if binlog2sql.Stop_time != "" {
			tstop_time, _ := time.Parse(TimeFormat, binlog2sql.Stop_time)
			if tstop_time.Before(tevent_time) {
				break
			}
		}
		if binlog2sql.Start_time != "" {
			tstart_time, err := time.Parse(TimeFormat, binlog2sql.Start_time)
			if err != nil {
				return err
			}

			if tevent_time.Before(tstart_time) {
				continue
			}
		}

		if binlog2sql.End_file != "" {
			if filename == binlog2sql.End_file && ev.Header.LogPos == uint32(binlog2sql.End_pos) {
				// flag_last_event = true
				break
			}
		} else {
			if logPos > uint32(binlog2sql.End_pos) {
				break
			}
		}

		// 如果开始的点位在map event之后,就找下一个事务的tablemap
		if ev.Header.EventType == replication.TABLE_MAP_EVENT {
			queryEvent := ev.Event.(*replication.TableMapEvent)

			table := string(queryEvent.Table)
			schema := string(queryEvent.Schema)
			if SchemaName != schema && TableName != table {
				continue
			}
		} else {
			continue
		}
		if ev.Header.EventType == replication.WRITE_ROWS_EVENTv2 || ev.Header.EventType == replication.DELETE_ROWS_EVENTv2 || ev.Header.EventType == replication.UPDATE_ROWS_EVENTv2 || ev.Header.EventType == replication.QUERY_EVENT {

			err = binlog2sql.concatSqlFromEvent(ev)
			if err != nil {
				return err
			}
		}

		if ev.Header.EventType == replication.ROTATE_EVENT {
			rotateEvent := ev.Event.(*replication.RotateEvent)
			filename = string(rotateEvent.NextLogName)
		}
		if flag_last_event {
			break
		}

	}
	return nil

}
