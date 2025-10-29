package geospatial

import "fmt"

const (
	GEO_STEP_MAX int     = 26
	GEO_LAT_MIN  float64 = -85.05112878
	GEO_LAT_MAX  float64 = 85.05112878
	GEO_LONG_MIN float64 = -180
	GEO_LONG_MAX float64 = 180
)

/* Interleave lower bits of x and y, so the bits of x
 * are in the even positions and bits from y in the odd;
 * x and y must initially be less than 2**32 (4294967296).
 * From:  https://graphics.stanford.edu/~seander/bithacks.html#InterleaveBMN
 */
func Interleave64(xlo, ylo uint32) uint64 {
	B := []uint64{0x5555555555555555, 0x3333333333333333,
		0x0F0F0F0F0F0F0F0F, 0x00FF00FF00FF00FF, 0x0000FFFF0000FFFF}
	S := []uint{1, 2, 4, 8, 16}

	x, y := uint64(xlo), uint64(ylo)

	x = (x | (x << S[4])) & B[4]
	y = (y | (y << S[4])) & B[4]

	x = (x | (x << S[3])) & B[3]
	y = (y | (y << S[3])) & B[3]

	x = (x | (x << S[2])) & B[2]
	y = (y | (y << S[2])) & B[2]

	x = (x | (x << S[1])) & B[1]
	y = (y | (y << S[1])) & B[1]

	x = (x | (x << S[0])) & B[0]
	y = (y | (y << S[0])) & B[0]

	return x | (y << 1)
}

/* reverse the interleave process
 * derived from http://stackoverflow.com/questions/4909263
 */
func deinterleave64(interleaved uint64) (uint32, uint32) {
	x := interleaved & 0x5555555555555555
	y := (interleaved >> 1) & 0x5555555555555555

	x = (x | (x >> 1)) & 0x3333333333333333
	y = (y | (y >> 1)) & 0x3333333333333333

	x = (x | (x >> 2)) & 0x0f0f0f0f0f0f0f0f
	y = (y | (y >> 2)) & 0x0f0f0f0f0f0f0f0f

	x = (x | (x >> 4)) & 0x00ff00ff00ff00ff
	y = (y | (y >> 4)) & 0x00ff00ff00ff00ff

	x = (x | (x >> 8)) & 0x0000ffff0000ffff
	y = (y | (y >> 8)) & 0x0000ffff0000ffff

	x = (x | (x >> 16)) & 0x00000000ffffffff
	y = (y | (y >> 16)) & 0x00000000ffffffff

	return uint32(x), uint32(y)
}

func isValid(longitude, latitude float64) bool {
	if longitude < GEO_LONG_MIN || longitude > GEO_LONG_MAX ||
		latitude < GEO_LAT_MIN || latitude > GEO_LAT_MAX {
		return false
	}
	return true
}

func GeohashEncode(longitude, latitude float64) (uint64, error) {
	if !isValid(longitude, latitude) {
		return 0, fmt.Errorf("Invalid (longitude, latitude): Out of range.")
	}

	var lonBit, latBit uint
	var hash uint64

	lonRange := []float64{GEO_LONG_MIN, GEO_LONG_MAX}
	latRange := []float64{GEO_LAT_MIN, GEO_LAT_MAX}

	for i := 0; i < GEO_STEP_MAX; i++ {
		lonMid := (lonRange[0] + lonRange[1]) / 2
		if longitude >= lonMid {
			lonBit = 1
			lonRange[0] = lonMid
		} else {
			lonBit = 0
			lonRange[1] = lonMid
		}

		latMid := (latRange[0] + latRange[1]) / 2
		if latitude >= latMid {
			latBit = 1
			latRange[0] = latMid
		} else {
			latBit = 0
			latRange[1] = latMid
		}

		hash <<= 1
		hash |= uint64(lonBit)
		hash <<= 1
		hash |= uint64(latBit)
	}

	return hash, nil
}

func GeohashDecode(hashF64 float64) (longitude, latitude float64) {
	hash := uint64(hashF64)

	lonRange := []float64{GEO_LONG_MIN, GEO_LONG_MAX}
	latRange := []float64{GEO_LAT_MIN, GEO_LAT_MAX}

	for i := range GEO_STEP_MAX {
		lonBit := (hash >> (51 - (i * 2))) & 1
		latBit := (hash >> (50 - (i * 2))) & 1

		if lonBit == 1 {
			lonRange[0] = (lonRange[0] + lonRange[1]) / 2
		} else {
			lonRange[1] = (lonRange[0] + lonRange[1]) / 2
		}

		if latBit == 1 {
			latRange[0] = (latRange[0] + latRange[1]) / 2
		} else {
			latRange[1] = (latRange[0] + latRange[1]) / 2
		}
	}

	longitude = (lonRange[0] + lonRange[1]) / 2
	latitude = (latRange[0] + latRange[1]) / 2

	return
}

