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
	"github.com/msrevive/nexus2/pkg/database/bsoncoder"
	"github.com/google/uuid"
	"go.etcd.io/bbolt"
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

		fmt.Println("Opening connection to new database")
		newDB, err := bbolt.Open(flags.destDBFile, 0755, &bbolt.Options{Timeout: 15 * time.Second})
		if err != nil {
			panic(err)
		}
		defer newDB.Close()

		fmt.Println("Creating buckets for new database")
		if err := createBucket(newDB); err != nil {
			panic(err)
		}

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

			// we create the new user structure here so character's can be filled in when we get them.
			newUser := schema.User{
				ID: oldPlayer.SteamID,
				Characters: make(map[int]uuid.UUID),
				DeletedCharacters: make(map[int]uuid.UUID),
			}

			charRowSlot1 := db.QueryRow("SELECT id, created_at, size, data FROM characters WHERE player_id = ? AND version = 1 AND slot = ?", oldPlayer.ID, 0)
			if err := charRowSlot1.Err(); err == nil {
				var oldChar oldChar
				charRowSlot1.Scan(&oldChar.ID, &oldChar.CreatedAt, &oldChar.Size, &oldChar.Data)

				newUser.Characters[1] = oldChar.ID

				newChar := schema.Character{
					ID: oldChar.ID,
					SteamID: oldPlayer.SteamID,
					Slot: 1,
					CreatedAt: oldChar.CreatedAt,
					Data: schema.CharacterData {
						CreatedAt: oldChar.CreatedAt,
						Size: oldChar.Size,
						Data: oldChar.Data,
					},
				}

				fmt.Printf("Importing character slot %d", newChar.Slot)
				if err := insertChar(newDB, newChar); err != nil {
					panic(err)
				}
			}

			charRowSlot2 := db.QueryRow("SELECT id, created_at, size, data FROM characters WHERE player_id = ? AND version = 1 AND slot = ?", oldPlayer.ID, 1)
			if err := charRowSlot2.Err(); err == nil {
				var oldChar oldChar
				charRowSlot2.Scan(&oldChar.ID, &oldChar.CreatedAt, &oldChar.Size, &oldChar.Data)

				newUser.Characters[2] = oldChar.ID

				newChar := schema.Character{
					ID: oldChar.ID,
					SteamID: oldPlayer.SteamID,
					Slot: 2,
					CreatedAt: oldChar.CreatedAt,
					Data: schema.CharacterData {
						CreatedAt: oldChar.CreatedAt,
						Size: oldChar.Size,
						Data: oldChar.Data,
					},
				}

				fmt.Printf("Importing character slot %d", newChar.Slot)
				if err := insertChar(newDB, newChar); err != nil {
					panic(err)
				}
			}

			charRowSlot3 := db.QueryRow("SELECT id, created_at, size, data FROM characters WHERE player_id = ? AND version = 1 AND slot = ?", oldPlayer.ID, 2)
			if err := charRowSlot3.Err(); err == nil {
				var oldChar oldChar
				charRowSlot3.Scan(&oldChar.ID, &oldChar.CreatedAt, &oldChar.Size, &oldChar.Data)

				newUser.Characters[3] = oldChar.ID

				newChar := schema.Character{
					ID: oldChar.ID,
					SteamID: oldPlayer.SteamID,
					Slot: 3,
					CreatedAt: oldChar.CreatedAt,
					Data: schema.CharacterData {
						CreatedAt: oldChar.CreatedAt,
						Size: oldChar.Size,
						Data: oldChar.Data,
					},
				}

				fmt.Printf("Importing character slot %d", newChar.Slot)
				if err := insertChar(newDB, newChar); err != nil {
					panic(err)
				}
			}

			fmt.Printf("Importing user %s", newUser.ID)
			if err := insertUser(newDB, newUser); err != nil {
				panic(err)
			}
		}
	}else{
		panic("unsupported database!")
	}
}

func createBucket(db *bbolt.DB) error {
	if err := db.Update(func(tx *bbolt.Tx) error {
		_, err := tx.CreateBucketIfNotExists([]byte("users"))
		if err != nil {
			return fmt.Errorf("failed to create users bucket: %s", err)
		}

		_, err = tx.CreateBucketIfNotExists([]byte("characters"))
		if err != nil {
			return fmt.Errorf("failed to create characters bucket: %s", err)
		}

		return nil
	}); err != nil {
		return err
	}

	return nil
} 

func insertUser(db *bbolt.DB, user schema.User) error {
	userData, err := bsoncoder.Encode(&user)
	if err != nil {
		return fmt.Errorf("bson: failed to marshal user %v", err)
	}

	if err := db.Update(func(tx *bbolt.Tx) error {
		bUser := tx.Bucket([]byte("users"))
		if err := bUser.Put([]byte(user.ID), userData); err != nil {
			return fmt.Errorf("bbolt: failed to put in users", err)
		}

		return nil
	}); err != nil {
		return err
	}

	return nil
}

func insertChar(db *bbolt.DB, char schema.Character) error {
	charData, err := bsoncoder.Encode(&char)
	if err != nil {
		return fmt.Errorf("bson: failed to marshal character %v", err)
	}

	if err := db.Update(func(tx *bbolt.Tx) error {
		bChar := tx.Bucket([]byte("characters"))
		if err := bChar.Put([]byte(char.ID.String()), charData); err != nil {
			return fmt.Errorf("bbolt: failed to put in characters", err)
		}

		return nil
	}); err != nil {
		return err
	}

	return nil
}