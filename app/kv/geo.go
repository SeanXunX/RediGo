package kv

import (
	"fmt"

	"github.com/codecrafters-io/redis-starter-go/app/geospatial"
)

func (kv *KVStore) GEOADD(key string, member string, longitude, latitude float64) (int, error) {
	score, err := geospatial.GeohashEncode(longitude, latitude)
	if err != nil {
		return 0, err
	}
	isNew := kv.ZAdd(key, member, float64(score))
	if isNew {
		return 1, nil
	} else {
		return 0, nil
	}
}

func (kv *KVStore) GEOPOS(key string, member string) (float64, float64, error) {
	score := kv.ZScore(key, member)
	if score == nil {
		return -1, -1, fmt.Errorf("Not exist")
	}
	hash := score.(float64)
	longitude, latitude := geospatial.GeohashDecode(hash)
	return longitude, latitude, nil
}
