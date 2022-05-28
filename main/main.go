package main

import (
	"flag"
	"fmt"
	"mylogparser/tosql"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

func main() {
	var (
		host   string
		port   int
		user   string
		passwd string

		tables         string
		start_file     string
		start_pos      int
		end_file       string
		end_pos        int
		start_time     string
		stop_time      string
		no_pk          bool
		flashback      bool
		stop_never     bool
		sleep_interval int
		only_dml       bool
		sql_type       string
		serverid       int64
		mode           string
	)

	flag.IntVar(&port, "port", 3306, "specify port to use.  defaults to 3306.")
	flag.StringVar(&host, "host", "127.0.0.1", "specify host to use.  defaults to 127.0.0.1.")
	flag.StringVar(&user, "user", "", "specify user.")
	flag.StringVar(&passwd, "passwd", "", "specify password.")
	flag.StringVar(&tables, "tables", "db1.user,db2.test", "specify tables.")
	flag.StringVar(&start_file, "start-file", "", "specify start file.")
	flag.IntVar(&start_pos, "start-pos", 4, "specify start position, start pos should before table map event,else we find next transaction's map event.")
	flag.StringVar(&end_file, "end-file", "", "specify end file.")
	flag.IntVar(&end_pos, "end-pos", -1, "specify end position.")
	flag.StringVar(&start_time, "start-time", "", "specify start time.")
	flag.StringVar(&stop_time, "stop-time", "", "specify stop time.")
	flag.BoolVar(&no_pk, "nopk", false, "whether to parse nopk tabl")
	flag.BoolVar(&flashback, "flashback", false, "specify flashback sql or normal parse sql.")
	flag.StringVar(&mode, "mode", "online", "specify mode of binlog parsing.")
	flag.Int64Var(&serverid, "serverid", 111, "specify fake slave serverid .")

	flag.Parse()

	if end_pos == -1 && stop_time == "2999-12-31 00:00:00" {
		fmt.Println("you must input one of  end_pos or stop_time")
	}
	if start_pos >= end_pos {
		fmt.Println("start pos greater than end pos")
		return
	}

	if start_time != "" {
		_, err := time.Parse(tosql.TimeFormat, start_time)
		if err != nil {
			fmt.Println("input start time format is wrong")
			return
		}
	}

	if stop_time != "" {
		_, err := time.Parse(tosql.TimeFormat, stop_time)
		if err != nil {
			fmt.Println("input end time format is wrong")
			return
		}
	}

	if start_file == "" {
		fmt.Println("start file need to be set")
		return
	}

	// if end_file == "" {
	// 	fmt.Println("end file need to be set")
	// 	return
	// }
	startFileNumber, err := strconv.Atoi(strings.Split(start_file, ".")[1])
	if err != nil {
		fmt.Println("input start file format is wrong")
		return
	}
	if end_file != "" {
		endFileNumber, err := strconv.Atoi(strings.Split(end_file, ".")[1])
		if err != nil {
			fmt.Println("input end file format is wrong")
			return
		}

		if startFileNumber > endFileNumber {
			fmt.Println("start file number greater than stop file number")
			return
		}
	}

	tableList := strings.Split(tables, ",")
	var wg sync.WaitGroup
	for _, table := range tableList {
		wg.Add(1)
		go func() {
			defer wg.Done()

			binlog2sql, err := tosql.NewBinLog2SQL(host, port, user, passwd, start_file, start_pos, end_file, end_pos, start_time, stop_time, table, no_pk, flashback, stop_never, sleep_interval, only_dml, sql_type, atomic.AddInt64(&serverid, 1), mode)
			if err != nil {
				fmt.Println(err)
				return
			}
			schemaTable := strings.Split(table, ".")
			tosql.SchemaName = schemaTable[0]
			tosql.TableName = schemaTable[1]
			err = binlog2sql.Process_binlog()
			if err != nil {
				fmt.Println(err)
				return
			}
		}()

	}
	wg.Wait()
	fmt.Println("done")

}
