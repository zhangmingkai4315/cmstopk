package cmstopk

import (
	"encoding/binary"
	"errors"
	"hash"
	"hash/fnv"
	"math"
)

type CountMinSketch struct {
	d uint
	w uint
	count [][]uint64
	hasher hash.Hash64
}
// NewCountMinSketch creates a new Count-Min Sketch
func NewCountMinSketch(d uint, w uint)(s *CountMinSketch, err error){
	if d <= 0 || w <= 0 {
		return nil, errors.New("values of d and w should both be greater than 0")
	}
	s = &CountMinSketch{
		d:      d,
		w:      w,
		hasher: fnv.New64(),
	}
	s.count = make([][]uint64, d)
	for r := uint(0); r < d; r++ {
		s.count[r] = make([]uint64, w)
	}

	return s, nil
}

// NewWithEstimates creates a new Count-Min Sketch with given error rate and confidence.
func NewCountMinSketchWithEstimates(epsilon, delta float64) (*CountMinSketch, error) {
	if epsilon <= 0 || epsilon >= 1 {
		return nil, errors.New("value of epsilon should be in range of (0, 1)")
	}
	if delta <= 0 || delta >= 1 {
		return nil, errors.New("value of delta should be in range of (0, 1)")
	}

	w := uint(math.Ceil(2 / epsilon))
	d := uint(math.Ceil(math.Log(1-delta) / math.Log(0.5)))
	return NewCountMinSketch(d, w)
}

// D returns the number of hashing functions
func (s *CountMinSketch) D() uint {
	return s.d
}

// Merge combines this CountMinSketch with another one
func (s *CountMinSketch) Merge(other *CountMinSketch) error {
	if s.d != other.d {
		return errors.New("matrix depth must match")
	}

	if s.w != other.w {
		return errors.New("matrix width must match")
	}

	for i := uint(0); i < s.d; i++ {
		for j := uint(0); j < s.w; j++ {
			s.count[i][j] += other.count[i][j]
		}
	}

	return nil
}

// get the two basic hash function values for data.
func (s *CountMinSketch) baseHashes(key []byte) (a uint32, b uint32) {
	s.hasher.Reset()
	s.hasher.Write(key)
	sum := s.hasher.Sum(nil)
	upper := sum[0:4]
	lower := sum[4:8]
	a = binary.BigEndian.Uint32(lower)
	b = binary.BigEndian.Uint32(upper)
	return
}


// Get the _w_ locations to update/Estimate
// Based on https://github.com/willf/bloom/blob/master/bloom.go
func (s *CountMinSketch) locations(key []byte) (locs []uint) {
	locs = make([]uint, s.d)
	a, b := s.baseHashes(key)
	ua := uint(a)
	ub := uint(b)
	for r := uint(0); r < s.d; r++ {
		locs[r] = (ua + ub*r) % s.w
	}
	return
}


// Update the frequency of a key
func (s *CountMinSketch) Update(key []byte, count uint64)uint64 {
	var min uint64 = 0
	for r, c := range s.locations(key) {
		s.count[r][c] += count
		if r == 0 || s.count[r][c] < min{
			min = s.count[r][c]
		}
	}
	return min
}

// UpdateString updates the frequency of a key
func (s *CountMinSketch) UpdateString(key string, count uint64)uint64 {
	return s.Update([]byte(key), count)
}

// Estimate the frequency of a key. It is point query.
func (s *CountMinSketch) Estimate(key []byte) uint64 {
	var min uint64
	for r, c := range s.locations(key) {
		if r == 0 || s.count[r][c] < min {
			min = s.count[r][c]
		}
	}
	return min
}

// EstimateString estimate the frequency of a key of string
func (s *CountMinSketch) EstimateString(key string) uint64 {
	return s.Estimate([]byte(key))
}


// W returns the size of hashing functions
func (s *CountMinSketch) W() uint {
	return s.w
}


func (s *CountMinSketch)Reset(){
	for i := uint(0); i < s.d; i++ {
		for j := uint(0); j < s.w; j++ {
			s.count[i][j] = 0
		}
	}
}
