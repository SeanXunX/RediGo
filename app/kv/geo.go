package kv

import "github.com/codecrafters-io/redis-starter-go/app/geospatial"

func (kv *KVStore) GEOADD(key string, member string, longitude, latitude float64) (int, error) {
	score, err := geospatial.GeohashEncode(longitude, latitude)
	if err != nil {
		return 0, err
	}
	isNew := kv.ZAdd(key, member, score)
	if isNew {
		return 1, nil
	} else {
		return 0, nil
	}
}
