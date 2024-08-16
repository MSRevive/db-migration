package main

import (
	"fmt"
	"os"
	"errors"
	"database/sql"
	"io"
	"context"

	_ "modernc.org/sqlite"
	"github.com/spf13/pflag"
)

type flags struct {
	originDB string
	originDBFile string

	destDB string
	destDBFile string
}

func doFlags(args []string) *flags {
	flgs := &flags{}

	flagSet := pflag.NewFlagSet(args[0], pflag.ExitOnError)
	flagSet.StringVar(&flgs.originDB, "origindb", "sqlite", "Database to migrate from.")
	flagSet.StringVar(&flgs.originDBFile, "origindbfile", "", "Original database file.")

	flagSet.StringVar(&flgs.destDB, "destdb", "bbolt", "Database to migrate to.")
	flagSet.StringVar(&flgs.destDBFile, "destdbfile", "", "What should the destination database file be name.")

	return flgs
}

func main() {
	fmt.Println("Starting database migration operation...")

	// Handle argument flags
	flags := doFlags(os.Args)

	if flags.originDBFile == "" {
		panic("no origin database file supplied!")
	}

	if flags.destDBFile == "" {
		panic("no destination database file supplied!")
	}

	if _, err := os.Stat(flags.originDBFile); errors.Is(err, os.ErrNotExist) {
		panic("original database file doesn't exist!")
	}

	if flags.originDB == "sqlite" && flags.destDB == "bbolt" {
		/////////////////////////////////////////////////
		/////////// Prepare DB files
		// 1. Copy current DB file (prevents irreversible changes)
		// 2. Rename current DB file (prevents conflicts with new DB)
		dbBakFileName := flags.originDBFile + ".bak"
		oldDbFileName := "./runtime/old_database.db"

		currentDbFile, err := os.Open(flags.originDBFile)
		if err != nil {
			panic(fmt.Sprintf("failed to open current DB file: %v", err))
		}

		backupDbFile, err := os.Create(dbBakFileName)
		if err != nil {
			panic(fmt.Sprintf("failed to create backup DB file: %v", err))
		}

		_, err = io.Copy(backupDbFile, currentDbFile)
		if err != nil {
			panic(fmt.Sprintf("failed to backup DB file: %v", err))
		}
		currentDbFile.Close()
		backupDbFile.Close()

		if err := os.Rename(flags.originDBFile, oldDbFileName); err != nil {
			panic(fmt.Sprintf("failed to rename original DB file: %v", err))
		}

		fmt.Println("Created original database backup.")

		// Open SQLite connection
		sqliteConnStr := "file:" + oldDbFileName + "?cache=shared&mode=rwc&_fk=1"
		db, err := sql.Open("sqlite", sqliteConnStr)
		if err != nil {
			panic(err)
		}
		defer db.Close()

		// PLACEHOLDER
		stmt, err := db.PrepareContext(context.TODO(), `SELECT * FROM 'characters'`)
		if err != nil {
			panic(err)
		}
		defer stmt.Close()
	}else{
		panic("unsupported database!")
	}
}