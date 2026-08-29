package wiggle

import (
	"regexp"
	"strconv"
)

// The epoch-aware instance id: {namespace}.e{epoch}.s{shard}.{ulid}. The id is the routing record --
// an instance's cell is a pure function of its id plus the placement policy (mirror of the Java
// core.IdCodec). Legacy ids (a bare wfi_... minted without a namespace) do not parse.
var idPattern = regexp.MustCompile(`^([^.]+)\.e(\d+)\.s(\d+)\.(.+)$`)

// Placement is a parsed epoch-aware id.
type Placement struct {
	Namespace string
	Epoch     int64
	Shard     int64
	Ulid      string
}

// ParseID parses an epoch-aware id; ok is false for a legacy id.
func ParseID(id string) (Placement, bool) {
	m := idPattern.FindStringSubmatch(id)
	if m == nil {
		return Placement{}, false
	}
	epoch, err1 := strconv.ParseInt(m[2], 10, 64)
	shard, err2 := strconv.ParseInt(m[3], 10, 64)
	if err1 != nil || err2 != nil {
		return Placement{}, false
	}
	return Placement{Namespace: m[1], Epoch: epoch, Shard: shard, Ulid: m[4]}, true
}

// IsLegacyID reports whether id is a legacy (non-epoch-aware) id.
func IsLegacyID(id string) bool {
	_, ok := ParseID(id)
	return !ok
}
