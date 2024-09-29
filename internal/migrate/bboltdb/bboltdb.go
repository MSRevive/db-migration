package bboltdb

import (
	"fmt"
	"os"
	"errors"
	"database/sql"
	"io"
	"time"

	_ "modernc.org/sqlite"
	"github.com/spf13/pflag"
	"github.com/msrevive/nexus2/pkg/database/schema"
	"github.com/msrevive/nexus2/pkg/database/bsoncoder"
	"github.com/google/uuid"
	"go.etcd.io/bbolt"
)

type bboltDB struct {
	db *bbolt.DB
}

func New() *bboltDB {
	return &bboltDB{
		db: nil,
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

func (b *bboltDB) InsertChar(char schema.Character) error {
	if b.db == nil {
		return fmt.Errorf("DB object is nil!")
	}

	charData, err := bsoncoder.Encode(&char)
	if err != nil {
		return fmt.Errorf("bson: failed to marshal character %v", err)
	}

	if err := b.db.Update(func(tx *bbolt.Tx) error {
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

func (b *bboltDB) InsertUser(user schema.User) error {
	if b.db == nil {
		return fmt.Errorf("DB object is nil!")
	}
	
	userData, err := bsoncoder.Encode(&user)
	if err != nil {
		return fmt.Errorf("bson: failed to marshal user %v", err)
	}

	if err := b.db.Update(func(tx *bbolt.Tx) error {
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

func (b *bboltDB) Migrate(originDBFile string, destDBFile string) error {
	/////////////////////////////////////////////////
	/////////// Prepare DB files
	// 1. Copy current DB file (prevents irreversible changes)
	// 2. Rename current DB file (prevents conflicts with new DB)
	dbBakFileName := originDBFile + ".bak"
	oldDbFileName := "./runtime/old_database.db"

	currentDbFile, err := os.Open(originDBFile)
	if err != nil {
		return fmt.Errorf("failed to open current DB file: %v", err)
	}

	backupDbFile, err := os.Create(dbBakFileName)
	if err != nil {
		return fmt.Errorf("failed to create backup DB file: %v", err)
	}

	_, err = io.Copy(backupDbFile, currentDbFile)
	if err != nil {
		return fmt.Errorf("failed to backup DB file: %v", err)
	}
	currentDbFile.Close()
	backupDbFile.Close()

	if err := os.Rename(originDBFile, oldDbFileName); err != nil {
		return fmt.Errorf("failed to rename original DB file: %v", err)
	}

	fmt.Println("Opening connection to new database")
	b.db, err := bbolt.Open(destDBFile, 0755, &bbolt.Options{Timeout: 15 * time.Second})
	if err != nil {
		return fmt.Errorf("unable to open BBolt DB: %v", err)
	}
	defer b.db.Close()

	fmt.Println("Creating buckets for new database")
	if err := createBucket(b.db); err != nil {
		return fmt.Errorf("unable to create buckets: %v", err)
	}

	// Open SQLite connection
	fmt.Println("Opening SQLite file")
	sqliteConnStr := "file:" + oldDbFileName + "?cache=shared&mode=rwc&_fk=1"
	db, err := sql.Open("sqlite", sqliteConnStr)
	if err != nil {
		return fmt.Errorf("unable to open SQLite DB", err)
	}
	defer db.Close()

	playerRows, err := db.Query("SELECT id, steamid, created_at FROM players")
	if err != nil {
		return fmt.Errorf("unable to query players from SQLite DB", err)
	}
	defer playerRows.Close()

	for playerRows.Next() {
		var oldPlayer oldPlayer

		if err := playerRows.Scan(&oldPlayer.ID, &oldPlayer.SteamID, &oldPlayer.CreatedAt); err != nil {
			return fmt.Errorf("unable to scan player row from SQLite DB", err)
		}

		// skip if there's no steamID.
		if oldPlayer.SteamID == "" {
			continue
		}

		fmt.Printf("Starting migration for %s\n", oldPlayer.SteamID)
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

			fmt.Printf("Importing character slot %d for SteamID:%s - %s\n", newChar.Slot, oldPlayer.SteamID, newChar.ID)
			if err := b.InsertChar(newChar); err != nil {
				return err
			}
		}

		charRowSlot2 := db.QueryRow("SELECT id, created_at, size, data FROM characters WHERE player_id = ? AND version = 1 AND slot = ?", oldPlayer.ID, 1)
		if err := charRowSlot2.Err(); err == nil {
			var oldChar oldChar
			if err := charRowSlot2.Scan(&oldChar.ID, &oldChar.CreatedAt, &oldChar.Size, &oldChar.Data); err == nil {
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

				fmt.Printf("Importing character slot %d for SteamID:%s - %s\n", newChar.Slot, oldPlayer.SteamID, newChar.ID)
				if err := b.InsertChar(newChar); err != nil {
					return err
				}
			}
		}

		charRowSlot3 := db.QueryRow("SELECT id, created_at, size, data FROM characters WHERE player_id = ? AND version = 1 AND slot = ?", oldPlayer.ID, 2)
		if err := charRowSlot3.Err(); err == nil {
			var oldChar oldChar
			if err := charRowSlot3.Scan(&oldChar.ID, &oldChar.CreatedAt, &oldChar.Size, &oldChar.Data); err == nil {
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

				fmt.Printf("Importing character slot %d for SteamID:%s - %s\n", newChar.Slot, oldPlayer.SteamID, newChar.ID)
				if err := b.InsertChar(newChar); err != nil {
					return err
				}
			}
		}

		fmt.Printf("Importing user %s\n\n", newUser.ID)
		if err := b.InsertUser(newUser); err != nil {
			return err
		}
	}

	return nil
}