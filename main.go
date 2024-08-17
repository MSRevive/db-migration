package main

import (
	"fmt"
	"os"
	"errors"
	"database/sql"
	"io"
	//"context"
	"time"

	_ "modernc.org/sqlite"
	"github.com/spf13/pflag"
	"github.com/msrevive/nexus2/pkg/database/schema"
	"github.com/google/uuid"
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
		fmt.Println("Opening SQLite file")
		sqliteConnStr := "file:" + oldDbFileName + "?cache=shared&mode=rwc&_fk=1"
		db, err := sql.Open("sqlite", sqliteConnStr)
		if err != nil {
			panic(err)
		}
		defer db.Close()

		playerRows, err := db.Query("SELECT id, steamid, created_at FROM players")
		if err != nil {
			panic(err)
		}
		defer playerRows.Close()

		for playerRows.Next() {
			var oldPlayer oldPlayer

			if err := playerRows.Scan(&oldPlayer.ID, &oldPlayer.SteamID, &oldPlayer.CreatedAt); err != nil {
				panic(err)
			}

			var oldChar oldChar
			charRow := db.QueryRow("SELECT id, created_at, slot, size, data FROM characters WHERE player_id = ? AND version = ? LIMIT 1", oldPlayer.ID, 1)
			if err := charRow.Scan(&oldChar.ID, &oldChar.CreatedAt, &oldChar.Slot, &oldChar.Size, &oldChar.Data); err != nil {
				panic(err)
			}

			newUser := schema.User{
				ID: oldPlayer.SteamID,
				Characters: make(map[int]uuid.UUID),
				DeletedCharacters: make(map[int]uuid.UUID),
			}
			newUser.Characters[oldChar.Slot] = oldChar.ID

			// newCharData := schema.CharacterData {
			// 	CreatedAt: oldChar.CreatedAt,
			// 	Size: oldChar.Size,
			// 	Data: oldChar.Data,
			// }
			newChar := schema.Character{
				ID: oldChar.ID,
				SteamID: oldPlayer.SteamID,
				Slot: oldChar.Slot,
				CreatedAt: oldChar.CreatedAt,
				Data: schema.CharacterData {
					CreatedAt: oldChar.CreatedAt,
					Size: oldChar.Size,
					Data: oldChar.Data,
				},
			}

			fmt.Printf("Importing user %s", newUser.ID)
			if err := insertUser(newUser); err != nil {
				panic(err)
			}

			fmt.Printf("Importing character slot %d", newChar.Slot)
			if err := insertChar(newChar); err != nil {
				panic(err)
			}
		}
	}else{
		panic("unsupported database!")
	}
}

func insertUser(user schema.User) error {
	return nil
}

func insertChar(char schema.Character) error {
	return nil
}