package main

import (
	"bufio"
	"flag"
	"fmt"
	"github.com/ewangplay/jzlconfig"
	"github.com/outmana/log4jzl"
	"io"
	"io/ioutil"
	"os"
	"os/signal"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
	"syscall"
)

const (
	SHOW_TABLES_SQL              string = "show tables"
	SHOW_CREATE_TABLE_PREFIX_SQL string = "show create table"
)

var g_logger *log4jzl.Log4jzl
var g_config jzlconfig.JZLConfig
var g_srcMysqlAdaptor *MysqlDBAdaptor
var g_destMysqlAdaptor *MysqlDBAdaptor
var g_waitgroup *sync.WaitGroup

func Usage() {
	fmt.Fprintln(os.Stderr, "Usage of ", os.Args[0], " [--config path_to_config_file]")
	flag.PrintDefaults()
	fmt.Fprintln(os.Stderr)
	os.Exit(0)
}

func main() {
	var err error

	//parse command line
	var configFile string
	flag.Usage = Usage
	flag.StringVar(&configFile, "config", "db_struct_sync.conf", "specified config filename")
	flag.Parse()

	fmt.Println("config file: ", configFile)

	//read config file
	err = g_config.Read(configFile)
	if err != nil {
		fmt.Println("Read config file fail: %v", err)
		return
	}

	//init logger
	g_logger, err = log4jzl.New("db_struct_sync")
	if err != nil {
		fmt.Println("Open log file fail: %v", err)
		return
	}

	//init log level object
	g_logLevel, err = NewLogLevel()
	if err != nil {
		LOG_ERROR("创建NewLogLevel对象失败，失败原因: %v", err)
		return
	}

	//init source mysql db adaptor
	var host, port, user, pass, dbname, charset string
	host, _ = g_config.Get("mysql_src.host")
	port, _ = g_config.Get("mysql_src.port")
	user, _ = g_config.Get("mysql_src.username")
	pass, _ = g_config.Get("mysql_src.password")
	dbname, _ = g_config.Get("mysql_src.dbname")
	charset, _ = g_config.Get("mysql_src.charset")

	fmt.Printf("Source Mysql: %v:%v\n", host, dbname)

	g_srcMysqlAdaptor, err = NewMysqlDBAdaptor(host, port, user, pass, dbname, charset)
	if err != nil {
		LOG_ERROR("create MysqlDBAdaptor object fail.", err)
		return
	}
	defer g_srcMysqlAdaptor.Release()

	//init destination mysql db adaptor
	host, _ = g_config.Get("mysql_dest.host")
	port, _ = g_config.Get("mysql_dest.port")
	user, _ = g_config.Get("mysql_dest.username")
	pass, _ = g_config.Get("mysql_dest.password")
	dbname, _ = g_config.Get("mysql_dest.dbname")
	charset, _ = g_config.Get("mysql_dest.charset")

	fmt.Printf("Destination Mysql: %v:%v\n", host, dbname)

	g_destMysqlAdaptor, err = NewMysqlDBAdaptor(host, port, user, pass, dbname, charset)
	if err != nil {
		LOG_ERROR("create MysqlDBAdaptor object fail.", err)
		return
	}
	defer g_destMysqlAdaptor.Release()

	//get data dir contains sql files
	data_dir, _ := g_config.Get("data.dir")
	if data_dir == "" {
		LOG_ERROR("data dir not set")
		return
	}

	//First Step: building the sql files automatically
	err = BuildSqlFiles(data_dir)
	if err != nil {
		return
	}

	LOG_INFO("=====================================================================")
	LOG_INFO("第一阶段完成!!!")
	LOG_INFO("请 *务必* 人工检查%v目录下生成的sql文件!!!", data_dir)
	LOG_INFO("在未确认的情况的继续执行，可能会对数据库造成灾难性的后果!!!")
	LOG_INFO("取消并终止程序请按Ctrl+C，继续请按Ctrl+\\")
	LOG_INFO("=====================================================================")

	//wait here to confirm manually
	g_waitgroup = &sync.WaitGroup{}
	g_waitgroup.Add(1)
	go func() {
		c := make(chan os.Signal)
		//拦截指定的系统信号
		signal.Notify(c, syscall.SIGINT, syscall.SIGQUIT)
		for {
			select {
			case s := <-c:
				switch s {
				case syscall.SIGINT:
					LOG_INFO("program terminated!")
					os.Exit(1)
				case syscall.SIGQUIT:
					g_waitgroup.Done()
				}
			}
		}
	}()
	g_waitgroup.Wait()

	//Second Step: travel to handle the sql file
	err = TravelSqlFiles(data_dir)
	if err != nil {
		return
	}

	LOG_INFO("success! ^_^")
}

func BuildSqlFiles(data_dir string) error {
	var err error

	//get src db struct
	err = PullDBStruct(data_dir, true, g_srcMysqlAdaptor)
	if err != nil {
		return err
	}

	//get dest db struct
	err = PullDBStruct(data_dir, false, g_destMysqlAdaptor)
	if err != nil {
		return err
	}

	//compare the src and dest mysql struct to build sql files
	err = DiffDBStruct(data_dir)
	if err != nil {
		return err
	}

	return nil
}

//根据src和dest数据库的结构差异生成增量sql文件
func DiffDBStruct(data_dir string) error {
	var err error

	src_tmp_dir := filepath.Join(data_dir, "src_mysql_tmp")
	dest_tmp_dir := filepath.Join(data_dir, "dest_mysql_tmp")

	src_db_struct, err := EnumFilesInDir(src_tmp_dir, ".sql")
	if err != nil {
		return err
	}

	dest_db_struct, err := EnumFilesInDir(dest_tmp_dir, ".sql")
	if err != nil {
		return err
	}

	//场景1
	//条件：src_sqlfile_list中有的文件在dest_sqlfile_list中没有
	//推论：说明增加了新表
	//操作：直接把该sql文件放到data_dir目录下
	for src_table_name, src_table_struct := range src_db_struct {
		dest_table_struct, table_found := dest_db_struct[src_table_name]
		if !table_found {
			//move the create table sql file in src_tmp_dir to data_dir
			src_file := filepath.Join(src_tmp_dir, src_table_name)
			src_file = fmt.Sprintf("%v.sql", src_file)
			dest_file := filepath.Join(data_dir, src_table_name)
			dest_file = fmt.Sprintf("%v.sql", dest_file)

			_, err = CopyFile(src_file, dest_file)
			if err != nil {
				return err
			}
		} else {
			src_fields_struct := src_table_struct["fields"]
			src_keys_struct := src_table_struct["keys"]
			dest_fields_struct := dest_table_struct["fields"]
			dest_keys_struct := dest_table_struct["keys"]

			//场景3
			//条件：文件在src_sqlfile_list和dest_sqlfile_list中都有，对比字段：某一字段在src_sqlfile中有但是在dest_sqlfile中没有
			//推论：说明该表增加了该字段
			//操作：在对应的sql文件中追加一条sql语句：添加字段
			for src_field_name, src_field_attr := range src_fields_struct {
				dest_field_attr, field_found := dest_fields_struct[src_field_name]
				if !field_found {
					err = MakeAddFieldSql(data_dir, src_table_name, src_field_name, src_field_attr)
					if err != nil {
						return err
					}
				} else {
					//场景5
					//条件：文件在src_sqlfile_list和dest_sqlfile_list中都有，对比字段：某一字段在src_sqlfile和dest_sqlfile中都存在，但字段类型不同
					//推论：说明该字段被修改了
					//操作：在对应的sql文件中追加一条sql语句：修改字段类型
					if src_field_attr != dest_field_attr {
						err = MakeModifyFieldSql(data_dir, src_table_name, src_field_name, src_field_attr)
						if err != nil {
							return err
						}
					}
				}
			}

			//场景4
			//条件：文件在src_sqlfile_list和dest_sqlfile_list中都有，对比字段：某一字段在dest_sqlfile中有但是在src_sqlfile中没有
			//推论：说明该表删除了该字段
			//操作：在对应的sql文件中追加一条sql语句：删除字段
			for dest_field_name, _ := range dest_fields_struct {
				_, field_found := src_fields_struct[dest_field_name]
				if !field_found {
					err = MakeRemoveFieldSql(data_dir, src_table_name, dest_field_name)
					if err != nil {
						return err
					}
				}
			}

			//场景5
			//条件：文件在src_sqlfile_list和dest_sqlfile_list中都有，对比索引：某一索引在src_sqlfile中有但是在dest_sqlfile中没有
			//推论：说明该表增加了该索引
			//操作：在对应的sql文件中追加一条sql语句：添加索引
			for src_key_name, src_key_attr := range src_keys_struct {
				dest_key_attr, key_found := dest_keys_struct[src_key_name]
				if !key_found {
					err = MakeAddIndexSql(data_dir, src_table_name, src_key_name, src_key_attr)
					if err != nil {
						return err
					}
				} else {
					//场景7
					//条件：文件在src_sqlfile_list和dest_sqlfile_list中都有，对比索引：某一索引在src_sqlfile和dest_sqlfile中都存在，但索引类型不同
					//推论：说明该索引被修改了
					//操作：在对应的sql文件中追加一条sql语句：修改索引
					if src_key_attr != dest_key_attr {
						err = MakeModifyIndexSql(data_dir, src_table_name, src_key_name, src_key_attr)
						if err != nil {
							return err
						}
					}
				}
			}

			//场景6
			//条件：文件在src_sqlfile_list和dest_sqlfile_list中都有，对比索引：某一索引在dest_sqlfile中有但是在src_sqlfile中没有
			//推论：说明该表删除了该索引
			//操作：在对应的sql文件中追加一条sql语句：删除索引
			for dest_key_name, _ := range dest_keys_struct {
				_, key_found := src_keys_struct[dest_key_name]
				if !key_found {
					err = MakeRemoveIndexSql(data_dir, src_table_name, dest_key_name)
					if err != nil {
						return err
					}
				}
			}
		}
	}

	//场景2
	//条件：dest_sqlfile_list中有的文件在src_sqlfile_list中没有
	//推论：说明删除了已有的表
	//操作：在data_dir目录下生成一个以该表命名的sql文件，里面的sql语句是删除该表
	for dest_table_name, _ := range dest_db_struct {
		_, table_found := src_db_struct[dest_table_name]
		if !table_found {
			err = MakeDropTableSql(data_dir, dest_table_name)
			if err != nil {
				return err
			}
		}
	}

	return nil
}

/*
CREATE TABLE `jzl_campaign` (
  `id` bigint(20) NOT NULL AUTO_INCREMENT,
  `cid` bigint(20) NOT NULL DEFAULT '0',
  `creator_id` bigint(20) NOT NULL DEFAULT '0',
  `last_editor_id` bigint(20) NOT NULL DEFAULT '0',
  `campaign_id` bigint(20) NOT NULL DEFAULT '0',
  `campaign_name` varchar(128) NOT NULL DEFAULT '',
  `create_time` datetime NOT NULL DEFAULT '1900-01-01 00:00:00',
  `last_modify_time` datetime NOT NULL DEFAULT '1900-01-01 00:00:00',
  `is_delete` int(4) NOT NULL DEFAULT '0',
  `delete_time` datetime NOT NULL DEFAULT '1900-01-01 00:00:00',
  PRIMARY KEY (`id`),
  KEY `cid_delete_time` (`cid`,`is_delete`,`create_time`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

{
    "jzl_campaign": {
        "fields": {
            "cid": "bigint(20) NOT NULL DEFAULT '0'"
        },
        "keys": {
            "cid_delete_time": "(`cid`,`is_delete`,`create_time`)"
        },
    },
}

map[string]map[string]map[string]string
*/

func EnumFilesInDir(tmp_dir string, suffix string) (db_struct map[string]map[string]map[string]string, err error) {
	files, err := ioutil.ReadDir(tmp_dir)
	if err != nil {
		LOG_ERROR("ReadDir %v error: %v", tmp_dir, err)
		return
	}

	db_struct = make(map[string]map[string]map[string]string)

	for _, file := range files {
		if file.IsDir() {
			continue
		}

		if !strings.HasSuffix(file.Name(), suffix) {
			continue
		}

		filename := filepath.Join(tmp_dir, file.Name())
		table_struct := make(map[string]map[string]string)

		err = ParseTableStruct(filename, table_struct)
		if err != nil {
			continue
		}

		db_struct[strings.TrimSuffix(file.Name(), ".sql")] = table_struct
	}

	return
}

func ParseTableStruct(sql_file string, table_struct map[string]map[string]string) error {
	f, err := os.Open(sql_file)
	if err != nil {
		LOG_ERROR("open sql file[%v] fail: %v", sql_file, err)
		return err
	}

	table_struct["fields"] = make(map[string]string)
	table_struct["keys"] = make(map[string]string)

	buf := bufio.NewReader(f)
	for {
		line, err := buf.ReadString('\n')
		if err != nil {
			if err == io.EOF {
				break
			}
			return err
		}

		line = strings.TrimSpace(line)

		//omit empty line
		if line == "" {
			continue
		}

		//omit comment line
		if strings.HasPrefix(line, "--") {
			continue
		}

		//omit CREATE TABLE head
		if strings.HasPrefix(line, "CREATE TABLE") {
			continue
		}

		//omit CREATE TABLE tail
		if strings.HasPrefix(line, ")") {
			continue
		}

		//omit primary key
		if strings.HasPrefix(line, "PRIMARY KEY") {
			continue
		}

		if strings.HasPrefix(line, "KEY") {
			//key handle
			err = ParseKeyStruct(line, table_struct["keys"])
		} else {
			//field handle
			err = ParseFieldStruct(line, table_struct["fields"])
		}
		if err != nil {
			continue
		}
	}

	return nil
}

func ParseFieldStruct(line string, fields_struct map[string]string) error {
	idx := strings.Index(line, " ")
	if idx == -1 {
		return fmt.Errorf("invalid field struct")
	}

	field_name := line[0:idx]
	field_attr := strings.TrimSuffix(line[idx+1:], ",")

	fields_struct[field_name] = field_attr

	return nil
}

func ParseKeyStruct(line string, keys_struct map[string]string) error {
	items := strings.Split(line, " ")
	if len(items) != 3 {
		return fmt.Errorf("invalid key struct")
	}

	keys_struct[items[1]] = strings.TrimSuffix(items[2], ",")

	return nil
}

func PullDBStruct(data_dir string, is_src bool, dbAdaptor *MysqlDBAdaptor) error {
	var tmp_dir string
	if is_src {
		tmp_dir = filepath.Join(data_dir, "src_mysql_tmp")
	} else {
		tmp_dir = filepath.Join(data_dir, "dest_mysql_tmp")
	}
	if !IsDirExists(tmp_dir) {
		err := os.MkdirAll(tmp_dir, os.ModePerm)
		if err != nil {
			return err
		}
	}

	//get the db table list
	rows, err := dbAdaptor.Query(SHOW_TABLES_SQL)
	if err != nil {
		LOG_ERROR("get tables list error: %v", err)
		return err
	}

	var table_list []string
	for rows.Next() {
		var table_name string
		err = rows.Scan(&table_name)
		if err != nil {
			LOG_ERROR("scan table name error: %v", err)
			return err
		}
		table_list = append(table_list, table_name)
	}

	rows.Close()

	//get the create table info
	for _, table := range table_list {
		queryStr := fmt.Sprintf("%v %v", SHOW_CREATE_TABLE_PREFIX_SQL, table)
		rows, err = dbAdaptor.Query(queryStr)
		if err != nil {
			LOG_ERROR("query create table info for %v error: %v", table, err)
			return err
		}

		rows_columns, err := rows.Columns()
		if err != nil {
			continue
		}
		if len(rows_columns) != 2 {
			continue
		}

		for rows.Next() {
			var table_name, create_table_sql string
			err = rows.Scan(&table_name, &create_table_sql)
			if err != nil {
				LOG_ERROR("scan create table info for %v error: %v", table, err)
				return err
			}

			//因为测试环境下的表已经有测试数据，所以Create Table info中的AUTO_INCREMENT的值不为1
			//所以需要把该值修改为1
			regExp := regexp.MustCompile(`AUTO_INCREMENT=\w+`)
			create_table_sql = regExp.ReplaceAllStringFunc(create_table_sql, func(s string) string {
				return "AUTO_INCREMENT=1"
			})

			if !strings.HasSuffix(create_table_sql, ";") {
				create_table_sql += ";"
			}

			err = CreateSqlFile(tmp_dir, table_name, create_table_sql)
			if err != nil {
				return err
			}
		}

		rows.Close()
	}

	return nil
}

func TravelSqlFiles(data_dir string) error {
	files, err := ioutil.ReadDir(data_dir)
	if err != nil {
		LOG_ERROR("get sql file under data dir error: %v", err)
		return err
	}

	for _, file := range files {
		if file.IsDir() {
			continue
		}

		if !strings.HasSuffix(file.Name(), ".sql") {
			continue
		}

		sql_file := filepath.Join(data_dir, file.Name())

		err = ExecSqlFile(sql_file)
		if err != nil {
			continue
		}

		//rename the sql file
		new_sql_file := fmt.Sprintf("%v.PASS", sql_file)
		os.Rename(sql_file, new_sql_file)
	}

	return nil
}

func ExecSqlFile(sql_file string) error {
	//read the sql file content
	f, err := os.Open(sql_file)
	if err != nil {
		LOG_ERROR("open sql file[%v] fail: %v", sql_file, err)
		return err
	}

	var content string
	buf := bufio.NewReader(f)
	for {
		line, err := buf.ReadString('\n')
		if err != nil {
			if err == io.EOF {
				break
			}
			return err
		}

		line = strings.TrimSpace(line)

		//omit empty line
		if line == "" {
			continue
		}

		//omit comment line
		if strings.HasPrefix(line, "--") {
			continue
		}

		content += line
	}

	sqls := strings.Split(content, ";")
	for _, sql := range sqls {
		if sql == "" {
			continue
		}

		err = g_destMysqlAdaptor.Exec(sql)
		if err != nil {
			LOG_ERROR("exec [%v] error: %v", sql, err)
			return err
		}
	}

	return nil
}

func IsDirExists(path string) bool {
	fi, err := os.Stat(path)
	if err != nil {
		return os.IsExist(err)
	}
	return fi.IsDir()
}

func CopyFile(src, dest string) (w int64, err error) {
	srcFile, err := os.Open(src)
	if err != nil {
		LOG_ERROR("open %v file error: %v", src, err)
		return
	}
	defer srcFile.Close()

	destFile, err := os.Create(dest)
	if err != nil {
		LOG_ERROR("open %v file error: %v", src, err)
		return
	}
	defer destFile.Close()

	return io.Copy(destFile, srcFile)
}

func MakeDropTableSql(data_dir string, table_name string) error {

	drop_table_sql := fmt.Sprintf("DROP TABLE %v;", table_name)

	return CreateSqlFile(data_dir, table_name, drop_table_sql)
}

func MakeAddFieldSql(data_dir string, table_name string, field_name string, field_attr string) error {

	add_field_sql := fmt.Sprintf("ALTER TABLE %v ADD %v %v;", table_name, field_name, field_attr)

	return AppendSqlFile(data_dir, table_name, add_field_sql)
}

func MakeRemoveFieldSql(data_dir string, table_name string, field_name string) error {

	drop_field_sql := fmt.Sprintf("ALTER TABLE %v DROP %v;", table_name, field_name)

	return AppendSqlFile(data_dir, table_name, drop_field_sql)
}

func MakeModifyFieldSql(data_dir string, table_name string, field_name string, field_attr string) error {
	modify_field_sql := fmt.Sprintf("ALTER TABLE %v MODIFY %v %v;", table_name, field_name, field_attr)

	return AppendSqlFile(data_dir, table_name, modify_field_sql)
}

func MakeAddIndexSql(data_dir string, table_name string, key_name string, key_attr string) error {
	add_index_sql := fmt.Sprintf("ALTER TABLE %v ADD INDEX %v %v;", table_name, key_name, key_attr)

	return AppendSqlFile(data_dir, table_name, add_index_sql)
}

func MakeRemoveIndexSql(data_dir string, table_name string, key_name string) error {
	drop_index_sql := fmt.Sprintf("ALTER TABLE %v DROP INDEX %v;", table_name, key_name)

	return AppendSqlFile(data_dir, table_name, drop_index_sql)
}

func MakeModifyIndexSql(data_dir string, table_name string, key_name string, key_attr string) error {
	err := MakeRemoveIndexSql(data_dir, table_name, key_name)
	if err != nil {
		return err
	}
	err = MakeAddIndexSql(data_dir, table_name, key_name, key_attr)
	if err != nil {
		return err
	}
	return nil
}

func CreateSqlFile(data_dir string, table_name string, sql_stat string) error {
	filename := filepath.Join(data_dir, table_name)
	filename = fmt.Sprintf("%v.sql", filename)

	f, err := os.Create(filename)
	if err != nil {
		LOG_ERROR("create %v file error: %v", filename, err)
		return err
	}
	defer f.Close()

	_, err = f.WriteString(sql_stat)
	if err != nil {
		LOG_ERROR("[CREATE] write sql[%v] to file[%v] error: %v", sql_stat, filename, err)
		return err
	}
	f.WriteString("\n")

	return nil
}

func AppendSqlFile(data_dir string, table_name string, sql_stat string) error {
	filename := filepath.Join(data_dir, table_name)
	filename = fmt.Sprintf("%v.sql", filename)

	f, err := os.OpenFile(filename, os.O_CREATE|os.O_APPEND|os.O_RDWR, 0666)
	if err != nil {
		LOG_ERROR("Open %v file error: %v", filename, err)
		return err
	}
	defer f.Close()

	_, err = f.WriteString(sql_stat)
	if err != nil {
		LOG_ERROR("[APPEND] write sql[%v] to file[%v] error: %v", sql_stat, filename, err)
		return err
	}
	f.WriteString("\n")

	return nil
}
