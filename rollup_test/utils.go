package main

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"log"

	"github.com/dgraph-io/badger/v4"
)

type delta struct {
	set_op, del_op bool
	values         []int32
}

func structToBytes(d delta) ([]byte, error) {
	buf := new(bytes.Buffer)

	err := binary.Write(buf, binary.LittleEndian, d.set_op)
	if err != nil {
		log.Printf("1. %s", err)
		return nil, err
	}

	err = binary.Write(buf, binary.LittleEndian, d.del_op)
	if err != nil {
		log.Printf("2. %s", err)
		return nil, err
	}

	err = binary.Write(buf, binary.LittleEndian, int32(len(d.values)))
	if err != nil {
		log.Printf("3. %s", err)
		return nil, err
	}

	for _, v := range d.values {
		err = binary.Write(buf, binary.LittleEndian, v)
		if err != nil {
			log.Printf("4. %s", err)
			return nil, err
		}
	}

	return buf.Bytes(), nil
}

func bytesToStruct(data []byte) (delta, error) {
	var d delta

	buf := bytes.NewReader(data)

	err := binary.Read(buf, binary.LittleEndian, &d.set_op)
	if err != nil {
		return delta{}, err
	}

	err = binary.Read(buf, binary.LittleEndian, &d.del_op)
	if err != nil {
		return delta{}, err
	}

	var valuesLen int32
	err = binary.Read(buf, binary.LittleEndian, &valuesLen)
	if err != nil {
		return delta{}, err
	}

	d.values = make([]int32, valuesLen)
	for i := 0; i < int(valuesLen); i++ {
		err = binary.Read(buf, binary.LittleEndian, &d.values[i])
		if err != nil {
			return delta{}, err
		}
	}

	return d, nil
}

func mergeDelta(existing, delta delta) delta {
	if delta.del_op {
		// fmt.Println("Del")
		list2Map := make(map[int32]bool)
		for _, num := range delta.values {
			list2Map[num] = true
		}

		i := 0
		for _, num := range existing.values {
			if !list2Map[num] {
				existing.values[i] = num
				i++
				fmt.Printf("%d ", num)
			}
		}

		existing.values = existing.values[:i]
		// fmt.Println("Del fin.")
	}

	if delta.set_op {
		// fmt.Println("Set")
		existing.values = append(existing.values, delta.values...)
	}

	return existing
}

func getList(key string, db *badger.DB) delta {
	var valCopy []byte
	db.View(func(txn *badger.Txn) error {
		item, err := txn.Get([]byte(key))
		if err != nil {
			return nil
		}
		valCopy, err = item.ValueCopy(nil)
		if err != nil {
			return nil
		}
		return nil
	})

	ret, _ := bytesToStruct(valCopy)
	return ret
}

func addTimestampToStringBytes(s string, timestamp int64) []byte {
	buf := new(bytes.Buffer)
	_ = binary.Write(buf, binary.BigEndian, timestamp)
	return append([]byte(s), buf.Bytes()...)
}

func byteToKeyAndTimestamp(b []byte) (string, int64) {
	length := len(b)
	ret_key := b[:length-8]

	buf := bytes.NewReader(b[length-8:])
	var ret_ts int64
	binary.Read(buf, binary.BigEndian, &ret_ts)

	ret := fmt.Sprintf("%s%d", ret_key, ret_ts)
	return ret, ret_ts
}
