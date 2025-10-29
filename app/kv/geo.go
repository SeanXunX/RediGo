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

func (kv *KVStore) GEODIST(key string, m1, m2 string) float64 {
	s1 := kv.ZScore(key, m1)
	s2 := kv.ZScore(key, m2)

	h1, h2 := s1.(float64), s2.(float64)

	lon1, lat1 := geospatial.GeohashDecode(h1)
	lon2, lat2 := geospatial.GeohashDecode(h2)

	p1 := geospatial.NewPoint(lon1, lat1)
	p2 := geospatial.NewPoint(lon2, lat2)

	return geospatial.Distance(p1, p2)
}

// Search for locations within given radius (meters)
func (kv *KVStore) GEOSEARCH_FROMLONLAT_BYRADIUS(key string, lon, lat float64, radius float64) (res []string) {
	storeValAny, ok := kv.mp.Load(key)
	var zSet ZSetValue
	if !ok {
		return
	} else {
		zSet = storeValAny.(StoreValue).v.(ZSetValue)
	}

	tarPoint := geospatial.NewPoint(lon, lat)
	for _, elem := range zSet.scores {
		curLon, curLat := geospatial.GeohashDecode(elem.score)
		p := geospatial.NewPoint(curLon, curLat)
		if geospatial.Distance(p, tarPoint) <= radius {
			res = append(res, elem.member)
		}
	}

	return
}
