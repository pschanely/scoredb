package scoredb

import (
	"encoding/binary"
	"fmt"
	"github.com/boltdb/bolt"
)

func NewBoltIdDb(file string) (*BoltIdDb, error) {
	db, err := bolt.Open(file, 0600, nil)
	if err != nil {
		return nil, err
	}
	return &BoltIdDb{Db: db}, nil
}

type BoltIdDb struct {
	Db *bolt.DB
}

func encodeScoreId(id int64) []byte {
	var buf [9]byte
	slice := buf[:]
	sz := binary.PutVarint(slice, id)
	return slice[:sz]
}

var boltBucketName []byte = []byte("ScoreDbIds")

func (db *BoltIdDb) Put(scoreIds []int64, clientIds []string) error {
	return db.Db.Update(func(tx *bolt.Tx) error {
		b, err := tx.CreateBucketIfNotExists([]byte(boltBucketName))
		if err != nil {
			return err
		}
		for idx, scoreId := range scoreIds {
			err = b.Put(encodeScoreId(scoreId), []byte(clientIds[idx]))
			if err != nil {
				return err
			}
		}
		return nil
	})
}

func (db *BoltIdDb) Get(scoreIds []int64) ([]string, error) {
	result := make([]string, len(scoreIds))

	err := db.Db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(boltBucketName))
		for idx, scoreId := range scoreIds {
			clientIdBytes := b.Get(encodeScoreId(scoreId))
			if clientIdBytes == nil {
				return fmt.Errorf("Unable to find client id for internal id %d", scoreId)
			}
			result[idx] = string(clientIdBytes[:])
			//fmt.Printf(" ID %v %v %v %v\n", idx, scoreId, clientIdBytes, result[idx])
		}
		return nil
	})

	return result, err
}
