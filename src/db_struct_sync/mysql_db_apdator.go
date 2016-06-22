package main

import (
	"database/sql"
	"fmt"
	_ "github.com/go-sql-driver/mysql"
)

type MysqlDBAdaptor struct {
	db *sql.DB
}

func NewMysqlDBAdaptor(host, port, user, pass, dbname, charset string) (*MysqlDBAdaptor, error) {
	dbAdaptor := &MysqlDBAdaptor{}

	conn_str := fmt.Sprintf("%s:%s@tcp(%s:%s)/%s?charset=%s", user, pass, host, port, dbname, charset)

	var err error
	dbAdaptor.db, err = sql.Open("mysql", conn_str)
	if err != nil {
		return nil, err
	}

	err = dbAdaptor.db.Ping()
	if err != nil {
		return nil, err
	}

	return dbAdaptor, nil
}

func (this *MysqlDBAdaptor) Release() {
	if this.db != nil {
		this.db.Close()
		this.db = nil
	}
}

func (this *MysqlDBAdaptor) Query(query string) (*sql.Rows, error) {
	if this.db == nil {
		return nil, fmt.Errorf("database object invalid")
	}

	rows, err := this.db.Query(query)
	if err != nil {
		return nil, err
	}

	return rows, nil
}

func (this *MysqlDBAdaptor) QueryRow(query string) (*sql.Row, error) {
	if this.db == nil {
		return nil, fmt.Errorf("database object invalid")
	}

	row := this.db.QueryRow(query)

	return row, nil
}

func (this *MysqlDBAdaptor) Exec(query string) error {
	if this.db == nil {
		return fmt.Errorf("database object invalid")
	}

	_, err := this.db.Exec(query)
	if err != nil {
		return err
	}

	return nil
}

func (this *MysqlDBAdaptor) ExecFormat(query string, args ...interface{}) error {
	if this.db == nil {
		return fmt.Errorf("database object invalid")
	}

	_, err := this.db.Exec(query, args...)
	if err != nil {
		return err
	}

	return nil
}

func (this *MysqlDBAdaptor) QueryFormat(query string, args ...interface{}) (*sql.Rows, error) {
	if this.db == nil {
		return nil, fmt.Errorf("database object invalid")
	}

	rows, err := this.db.Query(query, args...)
	if err != nil {
		return nil, err
	}

	return rows, nil
}

func (this *MysqlDBAdaptor) QueryRowFormat(query string, args ...interface{}) (*sql.Row, error) {
	if this.db == nil {
		return nil, fmt.Errorf("database object invalid")
	}

	return this.db.QueryRow(query, args...), nil

}

func (this *MysqlDBAdaptor) BeginTransaction() (*sql.Tx, error) {
	if this.db == nil {
		return nil, fmt.Errorf("database object invalid")
	}

	tx, err := this.db.Begin()
	if err != nil {
		return nil, err
	}

	return tx, nil
}

func (this *MysqlDBAdaptor) ExecTransaction(tx *sql.Tx, query string, args ...interface{}) error {
	if this.db == nil {
		return fmt.Errorf("database object invalid")
	}

	smt, err := tx.Prepare(query)
	if err != nil {
		return err
	}
	defer smt.Close()

	_, err = smt.Exec(args...)
	if err != nil {
		return err
	}

	return nil

}

func (this *MysqlDBAdaptor) CommitTransaction(tx *sql.Tx) error {
	if this.db == nil {
		return fmt.Errorf("database object invalid")
	}

	err := tx.Commit()
	if err != nil {
		return err
	}

	return nil
}

func (this *MysqlDBAdaptor) RollbackTransaction(tx *sql.Tx) error {

	if this.db == nil {
		return fmt.Errorf("database object invalid")
	}

	err := tx.Rollback()
	if err != nil {
		return err
	}

	return nil

}
