package main

import (
	"fmt"
	"os"
	"errors"
	"database/sql"
	"io"
	//"context"
	"time"

	"github.com/msrevive/nexus2/db-migration/internal/migrate"
	"github.com/msrevive/nexus2/db-migration/internal/migrate/bboltdb"
	"github.com/msrevive/nexus2/db-migration/internal/migrate/badgerdb"

	_ "modernc.org/sqlite"
	"github.com/spf13/pflag"
)

type flags struct {
	originDB string
	originDBFile string

	destDB string
	destDBFile string
}

type oldPlayer struct {
	ID uuid.UUID
	CreatedAt time.Time
	SteamID string
}

type oldChar struct {
	ID uuid.UUID
	CreatedAt time.Time
	Slot int
	Size int
	Data string
}

func doFlags(args []string) *flags {
	flgs := &flags{}

	flagSet := pflag.NewFlagSet(args[0], pflag.ExitOnError)
	flagSet.StringVar(&flgs.originDBFile, "origindbfile", "", "Original database file.")

	flagSet.StringVar(&flgs.destDB, "destdb", "bbolt", "Database to migrate to.")
	flagSet.StringVar(&flgs.destDBFile, "destdbfile", "", "What should the destination database file be name.")

	flagSet.Parse(args[1:])

	return flgs
}

func main() {
	// Handle argument flags
	flags := doFlags(os.Args)

	fmt.Printf("Starting application with arguements %s\n", os.Args[1:])

	if flags.originDBFile == "" {
		fmt.Println("ERROR: no origin DB supplied!")
		os.Exit(1)
	}

	if flags.destDBFile == "" {
		fmt.Println("ERROR: no destination DB supplied!")
		os.Exit(1)
	}

	if _, err := os.Stat(flags.originDBFile); errors.Is(err, os.ErrNotExist) {
		fmt.Printf("ERROR: unable to find origin DB file! %s\n", flags.originDBFile)
		os.Exit(1)
	}

	// Handle migration stuff
	var migration migrate.Migrate
	
	switch flags.destDB {
	case "bbolt":
		migration = bboltdb.New()
	case "badger":
		migration = badgerdb.New()
	default:
		fmt.Printf("ERROR: destination DB type not supported %s\n", flags.destDB)
		os.Exit(1)
	}

	fmt.Printf("Beginning migration of DB to %s...\n", flags.destDB)
	start := time.Now()

	if err := migration.Migrate(flags.originDBFile, flags.destDBFile); err != nil {
		panic(err)
	}

	fmt.Printf("Migration finished, took %v\n", time.Since(start))
	os.Exit(0)
}