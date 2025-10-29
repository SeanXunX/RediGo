package geospatial

import "math"

const (
	earthRadiusM = 6372797.560856 // Earth's radius in meters
)

type Point struct {
	Lon float64
	Lat float64
}

func NewPoint(lon, lat float64) Point {
	return Point{lon, lat}
}

func degreesToRadians(d float64) float64 {
	return d * math.Pi / 180
}

func Distance(p1, p2 Point) float64 {
	lat1Rad := degreesToRadians(p1.Lat)
	lon1Rad := degreesToRadians(p1.Lon)
	lat2Rad := degreesToRadians(p2.Lat)
	lon2Rad := degreesToRadians(p2.Lon)

	dLat := lat2Rad - lat1Rad
	dLon := lon2Rad - lon1Rad

	a := math.Sin(dLat/2)*math.Sin(dLat/2) +
		math.Cos(lat1Rad)*math.Cos(lat2Rad)*
			math.Sin(dLon/2)*math.Sin(dLon/2)

	c := 2 * math.Atan2(math.Sqrt(a), math.Sqrt(1-a))

	return earthRadiusM * c
}

