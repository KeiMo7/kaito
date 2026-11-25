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

// Package quota contains the dependency-free admission arbitration logic used
// before a Workspace asks the cloud for more nodes.
package quota

import (
	"errors"
	"time"
)

// Source identifies the layer that supplied the effective limit.
type Source string

const (
	SourceNone           Source = ""
	SourceClusterDefault Source = "ClusterDefault"
	SourceTeam           Source = "Team"
	SourceWorkload       Source = "Workload"
	SourceLastKnown      Source = "LastKnown"
)

// Limit is the optional quota configuration at one layer. A nil pointer means
// that dimension is not configured at this layer and therefore does not limit.
type Limit struct {
	MaxNodes    *int
	MaxGPUMilli *int64
	MaxWait     time.Duration
}

// Layer is a resolved configuration layer. Err records a source that exists but
// could not be parsed; it must never be interpreted as an unlimited limit.
type Layer struct {
	Source Source
	Limit  Limit
	Err    error
}

// EffectiveLimit contains the tightest finite values across all layers.
// Negative values mean unlimited.
type EffectiveLimit struct {
	MaxNodes         int
	NodeSource       Source
	MaxGPUMilli      int64
	GPUSource        Source
	MaxWait          time.Duration
	WaitSource       Source
	Stale            bool
	ResolutionErrors []string
}

// Unlimited returns an effective limit that does not constrain demand.
func Unlimited() EffectiveLimit {
	return EffectiveLimit{MaxNodes: -1, NodeSource: SourceNone, MaxGPUMilli: -1, GPUSource: SourceNone}
}

// ResolutionError describes an unparsable quota source.
type ResolutionError struct {
	Source Source
	Err    error
}

func (e *ResolutionError) Error() string {
	return string(e.Source) + " quota is invalid: " + e.Err.Error()
}

func (e *ResolutionError) Unwrap() error { return e.Err }

// Resolve combines cluster, team and workload limits. When a layer is invalid
// and last is non-nil, the last known effective value is retained and marked
// stale. Without a last known value the returned error fails closed.
func Resolve(last *EffectiveLimit, layers ...Layer) (EffectiveLimit, error) {
	var errs []error
	var messages []string
	for _, layer := range layers {
		if layer.Err != nil {
			wrapped := &ResolutionError{Source: layer.Source, Err: layer.Err}
			errs = append(errs, wrapped)
			messages = append(messages, wrapped.Error())
		}
	}
	if len(errs) > 0 {
		if last != nil {
			retained := *last
			retained.Stale = true
			retained.ResolutionErrors = append(append([]string{}, last.ResolutionErrors...), messages...)
			return retained, errors.Join(errs...)
		}
		closed := Unlimited()
		closed.ResolutionErrors = messages
		return closed, errors.Join(errs...)
	}

	effective := Unlimited()
	for _, layer := range layers {
		if layer.Source == SourceNone {
			continue
		}
		if layer.Limit.MaxNodes != nil {
			v := *layer.Limit.MaxNodes
			if effective.MaxNodes < 0 || v < effective.MaxNodes {
				effective.MaxNodes = v
				effective.NodeSource = layer.Source
			}
		}
		if layer.Limit.MaxGPUMilli != nil {
			v := *layer.Limit.MaxGPUMilli
			if effective.MaxGPUMilli < 0 || v < effective.MaxGPUMilli {
				effective.MaxGPUMilli = v
				effective.GPUSource = layer.Source
			}
		}
		if layer.Limit.MaxWait > 0 {
			if effective.MaxWait == 0 || layer.Limit.MaxWait < effective.MaxWait {
				effective.MaxWait = layer.Limit.MaxWait
				effective.WaitSource = layer.Source
			}
		}
	}
	return effective, nil
}

// NodesLimited reports whether a finite node cap is active.
func (l EffectiveLimit) NodesLimited() bool { return l.MaxNodes >= 0 }

// GPULimited reports whether a finite normalized GPU cap is active.
func (l EffectiveLimit) GPULimited() bool { return l.MaxGPUMilli >= 0 }
