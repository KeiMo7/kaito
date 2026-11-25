// Copyright (c) KAITO authors.
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package quota

import (
	"fmt"
)

// MilliGPUPerCard is the normalized weight of one whole GPU.
const MilliGPUPerCard int64 = 1000

// SliceDemand is the demand for one GPU-slice shape.
type SliceDemand struct {
	// Profile is the MIG/partition profile name. It is retained for messages.
	Profile string
	// Count is the number of slices of this shape.
	Count int
	// MilliGPUEach is the resolved weight of one slice. When non-zero it is
	// authoritative; callers normally resolve it from pkg/sku MIG geometry.
	MilliGPUEach int64
	// Granularity and SlotsPerSlice describe equal fractions when
	// MilliGPUEach is not supplied. A Granularity of 2 means half a card.
	Granularity   int
	SlotsPerSlice int
}

// MilliGPU returns the normalized occupancy of one slice. Explicit weights
// win. Equal-fraction geometry is rounded up so accounting never under-counts,
// and therefore never admits an overlapping card.
func (s SliceDemand) MilliGPU() int64 {
	if s.MilliGPUEach > 0 {
		return s.MilliGPUEach
	}
	if s.Granularity <= 0 {
		return 0
	}
	slots := s.SlotsPerSlice
	if slots <= 0 {
		slots = 1
	}
	num := MilliGPUPerCard * int64(slots)
	den := int64(s.Granularity)
	return (num + den - 1) / den
}

// Demand is the quota-relevant resource request of one Workspace.
type Demand struct {
	// Nodes comes from the existing node estimator/status.targetNodeCount.
	Nodes int
	// WholeGPUCards is the number of dedicated whole GPUs.
	WholeGPUCards int
	// Slices are fractional GPU requests.
	Slices []SliceDemand
}

// Validate rejects impossible/negative demand shapes.
func (d Demand) Validate() error {
	if d.Nodes < 0 || d.WholeGPUCards < 0 {
		return fmt.Errorf("quota demand must not be negative")
	}
	for i, s := range d.Slices {
		if s.Count < 0 || s.MilliGPUEach < 0 || s.Granularity < 0 || s.SlotsPerSlice < 0 {
			return fmt.Errorf("slice demand %d must not be negative", i)
		}
		if s.Count > 0 && s.MilliGPU() <= 0 {
			return fmt.Errorf("slice demand %d has no resolvable granularity", i)
		}
	}
	return nil
}

// SliceCount returns the total number of requested slices.
func (d Demand) SliceCount() int {
	total := 0
	for _, s := range d.Slices {
		total += s.Count
	}
	return total
}

// SliceMilliGPU returns total normalized slice occupancy.
func (d Demand) SliceMilliGPU() int64 {
	var total int64
	for _, s := range d.Slices {
		if s.Count <= 0 {
			continue
		}
		total += int64(s.Count) * s.MilliGPU()
	}
	return total
}

// GPUMilli returns total normalized GPU occupancy, keeping whole and slice
// amounts separable through WholeGPUCards and SliceMilliGPU.
func (d Demand) GPUMilli() int64 {
	return int64(d.WholeGPUCards)*MilliGPUPerCard + d.SliceMilliGPU()
}

// Card is a single physical GPU tracked by a CardLedger.
type Card struct {
	Name string
	// CapacitySlots is the partition granularity of the card. A whole GPU
	// consumes all slots.
	CapacitySlots int
	UsedSlots     int
	Whole         bool
}

func (c *Card) freeSlots() int { return c.CapacitySlots - c.UsedSlots }

// CardLedger is a small first-fit bin packer used to prove that whole-card and
// slice allocations never overlap the same physical card.
type CardLedger struct {
	Cards []*Card
}

// NewUniformCardLedger creates cardCount cards with identical granularity.
func NewUniformCardLedger(cardCount, granularity int) *CardLedger {
	if granularity <= 0 {
		granularity = 1
	}
	l := &CardLedger{Cards: make([]*Card, 0, maxInt(0, cardCount))}
	for i := 0; i < cardCount; i++ {
		l.Cards = append(l.Cards, &Card{Name: fmt.Sprintf("gpu-%d", i), CapacitySlots: granularity})
	}
	return l
}

func maxInt(a, b int) int {
	if a > b {
		return a
	}
	return b
}

// AllocateWhole places one whole GPU only on a completely unused card.
func (l *CardLedger) AllocateWhole() bool {
	for _, card := range l.Cards {
		if !card.Whole && card.UsedSlots == 0 {
			card.Whole = true
			card.UsedSlots = card.CapacitySlots
			return true
		}
	}
	return false
}

// AllocateSlice places one slice of slots on a non-whole card with enough room.
func (l *CardLedger) AllocateSlice(slots int) bool {
	if slots <= 0 {
		slots = 1
	}
	for _, card := range l.Cards {
		if card.Whole || card.freeSlots() < slots {
			continue
		}
		card.UsedSlots += slots
		return true
	}
	return false
}

// AllocateDemand allocates whole cards and slices. It stops and returns false
// without rolling back earlier placements; callers checking feasibility should
// use a copy or first call CanAllocateDemand.
func (l *CardLedger) AllocateDemand(d Demand) bool {
	for i := 0; i < d.WholeGPUCards; i++ {
		if !l.AllocateWhole() {
			return false
		}
	}
	for _, slice := range d.Slices {
		slots := slice.SlotsPerSlice
		if slots <= 0 {
			slots = 1
		}
		for i := 0; i < slice.Count; i++ {
			if !l.AllocateSlice(slots) {
				return false
			}
		}
	}
	return true
}

// CanAllocateDemand reports whether d fits without mutating the receiver.
func (l *CardLedger) CanAllocateDemand(d Demand) bool {
	copyLedger := &CardLedger{Cards: make([]*Card, len(l.Cards))}
	for i, c := range l.Cards {
		cp := *c
		copyLedger.Cards[i] = &cp
	}
	return copyLedger.AllocateDemand(d)
}
