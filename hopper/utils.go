package hopper

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"reflect"
)

func uint64ToBytes(n uint64) []byte {
	b := make([]byte, 8)
	binary.LittleEndian.PutUint64(b, n)
	return b
}

func uint64FromBytes(b []byte) uint64 {
	return binary.LittleEndian.Uint64(b)
}

func anyToFloat(value any) (float64, error) {
	switch v := value.(type) {
	case float32:
		return float64(v), nil
	case float64:
		return v, nil
	case int:
		return float64(v), nil
	case int8:
		return float64(v), nil
	case int16:
		return float64(v), nil
	case int32:
		return float64(v), nil
	case int64:
		return float64(v), nil
	case uint:
		return float64(v), nil
	case uint8:
		return float64(v), nil
	case uint16:
		return float64(v), nil
	case uint32:
		return float64(v), nil
	case uint64:
		return float64(v), nil
	default:
		return 0, fmt.Errorf("conversion to float64 not supported for type %s", reflect.TypeOf(value))
	}
}

func UnmarshalKV(k, v []byte) (Map, error) {
	data := Map{
		"id": uint64FromBytes(k),
	}
	if err := json.Unmarshal(v, &data); err != nil {
		return nil, err
	}
	return data, nil
}
