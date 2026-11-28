package scs

import (
	"bytes"
	"encoding/binary"
	"encoding/gob"
	"errors"
	"hash/crc32"
	"reflect"
	"testing"
	"time"
)

// legacyPayload is the exact wire format written by older versions of this
// package. It is defined here so that compatibility tests do not depend on the
// production encoder.
type legacyPayload struct {
	Deadline time.Time
	Values   map[string]interface{}
}

func encodeLegacyTestPayload(t *testing.T, deadline time.Time, values map[string]interface{}) []byte {
	t.Helper()

	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(&legacyPayload{Deadline: deadline, Values: values}); err != nil {
		t.Fatalf("encoding legacy payload: %v", err)
	}
	return buf.Bytes()
}

func TestGobCodec_EncodeIsDeterministic(t *testing.T) {
	t.Parallel()

	codec := GobCodec{}
	deadline := time.Unix(1800000000, 123456789).UTC()
	values := map[string]interface{}{
		"alpha":   "one",
		"bravo":   int64(2),
		"charlie": []byte{1, 2, 3},
		"delta":   true,
		"echo":    3.5,
		"golf":    nil,
	}

	first, err := codec.Encode(deadline, values)
	if err != nil {
		t.Fatalf("unexpected encode error: %v", err)
	}

	for i := 0; i < 1000; i++ {
		next, err := codec.Encode(deadline, values)
		if err != nil {
			t.Fatalf("unexpected encode error: %v", err)
		}
		if !bytes.Equal(first, next) {
			t.Fatalf("encoding is not deterministic: %x != %x", first, next)
		}
	}

	// Extension fields must not make the encoding non-deterministic either.
	meta := PayloadMeta{Extensions: map[string]interface{}{"future": "value", "another": int64(9)}}
	firstExt, err := codec.EncodePayload(deadline, values, meta)
	if err != nil {
		t.Fatalf("unexpected encode error: %v", err)
	}
	for i := 0; i < 1000; i++ {
		nextExt, err := codec.EncodePayload(deadline, values, meta)
		if err != nil {
			t.Fatalf("unexpected encode error: %v", err)
		}
		if !bytes.Equal(firstExt, nextExt) {
			t.Fatalf("encoding with extensions is not deterministic")
		}
	}
}

func TestGobCodec_RoundTrip(t *testing.T) {
	t.Parallel()

	codec := GobCodec{}
	deadline := time.Unix(1800000000, 123456789).UTC()
	values := map[string]interface{}{
		"string": "value",
		"int":    int64(42),
		"bytes":  []byte{0, 1, 2, 255},
	}

	encoded, err := codec.Encode(deadline, values)
	if err != nil {
		t.Fatalf("unexpected encode error: %v", err)
	}

	gotDeadline, gotValues, err := codec.Decode(encoded)
	if err != nil {
		t.Fatalf("unexpected decode error: %v", err)
	}
	if !gotDeadline.Equal(deadline) {
		t.Errorf("deadline = %v; want %v", gotDeadline, deadline)
	}
	if !reflect.DeepEqual(gotValues, values) {
		t.Errorf("values = %#v; want %#v", gotValues, values)
	}
}

func TestGobCodec_DecodesLegacyPayload(t *testing.T) {
	t.Parallel()

	deadline := time.Unix(1800000000, 0).UTC()
	values := map[string]interface{}{"userID": "42", "count": int64(7)}

	legacy := encodeLegacyTestPayload(t, deadline, values)

	codec := GobCodec{}
	gotDeadline, gotValues, meta, err := codec.DecodePayload(legacy)
	if err != nil {
		t.Fatalf("unexpected decode error: %v", err)
	}
	if !gotDeadline.Equal(deadline) {
		t.Errorf("deadline = %v; want %v", gotDeadline, deadline)
	}
	if !reflect.DeepEqual(gotValues, values) {
		t.Errorf("values = %#v; want %#v", gotValues, values)
	}
	if meta.Version != PayloadVersionLegacy {
		t.Errorf("version = %d; want %d", meta.Version, PayloadVersionLegacy)
	}
	if len(meta.Extensions) != 0 {
		t.Errorf("extensions = %#v; want none", meta.Extensions)
	}
}

func TestGobCodec_LegacyPayloadWithoutDeadline(t *testing.T) {
	t.Parallel()

	// Payloads which predate the deadline field decode with the zero time,
	// which Load treats as a missing deadline rather than an expired session.
	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(&struct {
		Values map[string]interface{}
	}{Values: map[string]interface{}{"key": "value"}}); err != nil {
		t.Fatalf("encoding payload: %v", err)
	}

	codec := GobCodec{}
	deadline, values, err := codec.Decode(buf.Bytes())
	if err != nil {
		t.Fatalf("unexpected decode error: %v", err)
	}
	if !deadline.IsZero() {
		t.Errorf("deadline = %v; want zero time", deadline)
	}
	if values["key"] != "value" {
		t.Errorf("values = %#v; want key=value", values)
	}
}

func TestGobCodec_WriteLegacyReadableByLegacyDecoder(t *testing.T) {
	t.Parallel()

	deadline := time.Unix(1800000000, 0).UTC()
	values := map[string]interface{}{"key": "value"}

	encoded, err := GobCodec{WriteLegacy: true}.Encode(deadline, values)
	if err != nil {
		t.Fatalf("unexpected encode error: %v", err)
	}

	// An older decoder, which knows only about the legacy struct, must be able
	// to read the payload.
	var decoded legacyPayload
	if err := gob.NewDecoder(bytes.NewReader(encoded)).Decode(&decoded); err != nil {
		t.Fatalf("legacy decoder could not read payload: %v", err)
	}
	if !decoded.Deadline.Equal(deadline) {
		t.Errorf("deadline = %v; want %v", decoded.Deadline, deadline)
	}
	if !reflect.DeepEqual(decoded.Values, values) {
		t.Errorf("values = %#v; want %#v", decoded.Values, values)
	}

	// Our own decoder must read it too.
	gotDeadline, gotValues, err := (GobCodec{}).Decode(encoded)
	if err != nil {
		t.Fatalf("unexpected decode error: %v", err)
	}
	if !gotDeadline.Equal(deadline) || !reflect.DeepEqual(gotValues, values) {
		t.Errorf("round trip mismatch: %v %#v", gotDeadline, gotValues)
	}
}

func TestGobCodec_WriteLegacyFallsBackForExtensions(t *testing.T) {
	t.Parallel()

	// Extension fields cannot be represented in the legacy format. They must
	// be preserved by falling back to the current format rather than dropped.
	encoded, err := GobCodec{WriteLegacy: true}.EncodePayload(
		time.Unix(1800000000, 0).UTC(),
		map[string]interface{}{"key": "value"},
		PayloadMeta{Extensions: map[string]interface{}{"future": "value"}},
	)
	if err != nil {
		t.Fatalf("unexpected encode error: %v", err)
	}

	_, _, meta, err := (GobCodec{}).DecodePayload(encoded)
	if err != nil {
		t.Fatalf("unexpected decode error: %v", err)
	}
	if meta.Version != PayloadVersionCurrent {
		t.Errorf("version = %d; want %d", meta.Version, PayloadVersionCurrent)
	}
	if meta.Extensions["future"] != "value" {
		t.Errorf("extensions = %#v; want future=value", meta.Extensions)
	}
}

func TestGobCodec_UnknownExtensionsArePreserved(t *testing.T) {
	t.Parallel()

	codec := GobCodec{}
	deadline := time.Unix(1800000000, 0).UTC()
	values := map[string]interface{}{"key": "value"}
	extensions := map[string]interface{}{
		"newField":      "value",
		"newerField":    int64(1),
		"unknownStruct": []byte{9, 8, 7},
	}

	encoded, err := codec.EncodePayload(deadline, values, PayloadMeta{Extensions: extensions})
	if err != nil {
		t.Fatalf("unexpected encode error: %v", err)
	}

	_, gotValues, meta, err := codec.DecodePayload(encoded)
	if err != nil {
		t.Fatalf("unexpected decode error: %v", err)
	}
	if !reflect.DeepEqual(meta.Extensions, extensions) {
		t.Errorf("extensions = %#v; want %#v", meta.Extensions, extensions)
	}

	// Re-encoding an unchanged payload must keep the extension fields.
	reencoded, err := codec.EncodePayload(deadline, gotValues, meta)
	if err != nil {
		t.Fatalf("unexpected re-encode error: %v", err)
	}
	if !bytes.Equal(encoded, reencoded) {
		t.Errorf("re-encoded payload differs from the original")
	}

	_, _, metaAfter, err := codec.DecodePayload(reencoded)
	if err != nil {
		t.Fatalf("unexpected decode error: %v", err)
	}
	if !reflect.DeepEqual(metaAfter.Extensions, extensions) {
		t.Errorf("extensions after re-encode = %#v; want %#v", metaAfter.Extensions, extensions)
	}
}

func TestGobCodec_RejectsInvalidDeadline(t *testing.T) {
	t.Parallel()

	codec := GobCodec{}

	for _, tt := range []struct {
		name     string
		deadline time.Time
	}{
		{"year below range", time.Date(-1, 1, 1, 0, 0, 0, 0, time.UTC)},
		{"year above range", time.Date(10000, 1, 1, 0, 0, 0, 0, time.UTC)},
	} {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			if _, err := codec.Encode(tt.deadline, map[string]interface{}{}); !errors.Is(err, ErrPayloadDeadlineInvalid) {
				t.Errorf("Encode() error = %v; want ErrPayloadDeadlineInvalid", err)
			}
		})
	}

	// An explicit deadline which is not a representable time must not be
	// silently accepted when decoding either.
	body := payloadBody{
		Deadline:    time.Date(10000, 1, 1, 0, 0, 0, 0, time.UTC),
		HasDeadline: true,
	}
	encoded := encodeVersionedTestPayload(t, &body)
	if _, _, _, err := codec.DecodePayload(encoded); !errors.Is(err, ErrPayloadDeadlineInvalid) {
		t.Errorf("DecodePayload() error = %v; want ErrPayloadDeadlineInvalid", err)
	}
}

func TestGobCodec_RejectsZeroDeadlineWithFlag(t *testing.T) {
	t.Parallel()

	// A payload which claims to carry a deadline but contains the zero time is
	// corrupt, not a payload with a missing deadline.
	encoded := encodeVersionedTestPayload(t, &payloadBody{HasDeadline: true})
	_, _, _, err := (GobCodec{}).DecodePayload(encoded)
	if !errors.Is(err, ErrPayloadDeadlineInvalid) {
		t.Errorf("DecodePayload() error = %v; want ErrPayloadDeadlineInvalid", err)
	}
}

func TestGobCodec_TruncatedPayload(t *testing.T) {
	t.Parallel()

	codec := GobCodec{}
	encoded, err := codec.Encode(time.Unix(1800000000, 0).UTC(), map[string]interface{}{"key": "value"})
	if err != nil {
		t.Fatalf("unexpected encode error: %v", err)
	}

	for n := 0; n < len(encoded); n++ {
		deadline, values, err := codec.Decode(encoded[:n])
		if err == nil {
			t.Fatalf("cut at %d bytes: expected an error", n)
		}
		if deadline != (time.Time{}) {
			t.Errorf("cut at %d bytes: deadline = %v; want zero", n, deadline)
		}
		if values != nil {
			t.Errorf("cut at %d bytes: values = %#v; want nil", n, values)
		}
		if !errors.Is(err, ErrPayloadTruncated) {
			t.Errorf("cut at %d bytes: error = %v; want ErrPayloadTruncated", n, err)
		}
	}
}

func TestGobCodec_LengthMismatch(t *testing.T) {
	t.Parallel()

	codec := GobCodec{}
	encoded, err := codec.Encode(time.Unix(1800000000, 0).UTC(), map[string]interface{}{"key": "value"})
	if err != nil {
		t.Fatalf("unexpected encode error: %v", err)
	}

	// Appending bytes makes the payload longer than the header declares.
	extended := append(append([]byte{}, encoded...), 0x00, 0x01, 0x02)
	if _, values, err := codec.Decode(extended); !errors.Is(err, ErrPayloadLengthMismatch) {
		t.Errorf("Decode() error = %v; want ErrPayloadLengthMismatch", err)
	} else if values != nil {
		t.Errorf("Decode() values = %#v; want nil", values)
	}

	// A header which declares a longer body than is present is truncation.
	truncatedHeader := append([]byte{}, encoded...)
	binary.BigEndian.PutUint32(truncatedHeader[8:12], uint32(len(encoded)))
	if _, _, err := codec.Decode(truncatedHeader); !errors.Is(err, ErrPayloadTruncated) {
		t.Errorf("Decode() error = %v; want ErrPayloadTruncated", err)
	}
}

func TestGobCodec_ChecksumMismatch(t *testing.T) {
	t.Parallel()

	codec := GobCodec{}
	encoded, err := codec.Encode(time.Unix(1800000000, 0).UTC(), map[string]interface{}{"key": "value"})
	if err != nil {
		t.Fatalf("unexpected encode error: %v", err)
	}

	corrupt := append([]byte{}, encoded...)
	corrupt[len(corrupt)-1] ^= 0xff

	deadline, values, err := codec.Decode(corrupt)
	if !errors.Is(err, ErrPayloadChecksumMismatch) {
		t.Errorf("Decode() error = %v; want ErrPayloadChecksumMismatch", err)
	}
	if deadline != (time.Time{}) || values != nil {
		t.Errorf("Decode() returned partial session: %v %#v", deadline, values)
	}
}

func TestGobCodec_UnsupportedVersion(t *testing.T) {
	t.Parallel()

	body := payloadBody{HasDeadline: true, Deadline: time.Unix(1800000000, 0).UTC()}
	encoded := encodeVersionedTestPayload(t, &body)
	encoded[4] = 99 // unknown payload version

	if _, _, err := (GobCodec{}).Decode(encoded); !errors.Is(err, ErrPayloadVersionUnsupported) {
		t.Errorf("Decode() error = %v; want ErrPayloadVersionUnsupported", err)
	}

	// Unknown feature flags are also a version we do not understand.
	encoded = encodeVersionedTestPayload(t, &body)
	encoded[5] = 0x01

	if _, _, err := (GobCodec{}).Decode(encoded); !errors.Is(err, ErrPayloadVersionUnsupported) {
		t.Errorf("Decode() error = %v; want ErrPayloadVersionUnsupported", err)
	}
}

func TestGobCodec_LegacyTruncationIsClassified(t *testing.T) {
	t.Parallel()

	encoded := encodeLegacyTestPayload(t, time.Unix(1800000000, 0).UTC(), map[string]interface{}{"key": "value"})

	// Cut the legacy payload in half: it must be reported as truncated, not as
	// a generic decode failure, and no values may be returned.
	deadline, values, err := (GobCodec{}).Decode(encoded[:len(encoded)/2])
	if !errors.Is(err, ErrPayloadTruncated) {
		t.Errorf("Decode() error = %v; want ErrPayloadTruncated", err)
	}
	if deadline != (time.Time{}) || values != nil {
		t.Errorf("Decode() returned partial session: %v %#v", deadline, values)
	}
}

func TestGobCodec_LegacyTrailingBytes(t *testing.T) {
	t.Parallel()

	encoded := encodeLegacyTestPayload(t, time.Unix(1800000000, 0).UTC(), map[string]interface{}{"key": "value"})
	encoded = append(encoded, 'x', 'y')

	if _, _, err := (GobCodec{}).Decode(encoded); !errors.Is(err, ErrPayloadLengthMismatch) {
		t.Errorf("Decode() error = %v; want ErrPayloadLengthMismatch", err)
	}
}

func TestGobCodec_EmptyPayloadIsTruncated(t *testing.T) {
	t.Parallel()

	if _, _, err := (GobCodec{}).Decode(nil); !errors.Is(err, ErrPayloadTruncated) {
		t.Errorf("Decode(nil) error = %v; want ErrPayloadTruncated", err)
	}
}

func TestGobCodec_DecodeIntoFreshMaps(t *testing.T) {
	t.Parallel()

	// Callers may write to the returned values map even when the payload had
	// none, so it must never be nil.
	codec := GobCodec{}
	encoded, err := codec.Encode(time.Unix(1800000000, 0).UTC(), nil)
	if err != nil {
		t.Fatalf("unexpected encode error: %v", err)
	}

	_, values, err := codec.Decode(encoded)
	if err != nil {
		t.Fatalf("unexpected decode error: %v", err)
	}
	if values == nil {
		t.Fatal("decoded values map is nil")
	}
	values["key"] = "value"
}

// encodeVersionedTestPayload builds a current-format payload around body,
// computing a correct header and checksum.
func encodeVersionedTestPayload(t *testing.T, body *payloadBody) []byte {
	t.Helper()

	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(body); err != nil {
		t.Fatalf("encoding payload body: %v", err)
	}
	encoded := buf.Bytes()

	payload := make([]byte, payloadHeaderSize+len(encoded))
	copy(payload[0:4], payloadMagic[:])
	payload[4] = byte(PayloadVersionCurrent)
	binary.BigEndian.PutUint32(payload[8:12], uint32(len(encoded)))
	binary.BigEndian.PutUint32(payload[12:16], crc32.Checksum(encoded, payloadChecksumTable))
	copy(payload[payloadHeaderSize:], encoded)

	return payload
}
