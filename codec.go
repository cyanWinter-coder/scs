package scs

import (
	"bytes"
	"encoding/binary"
	"encoding/gob"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"sort"
	"time"
)

// PayloadVersion identifies the wire format used to encode a session payload.
type PayloadVersion uint8

const (
	// PayloadVersionLegacy is the original payload format. It is a gob-encoded
	// struct containing only a deadline and a map of session values. Payloads
	// in this format are still written by older versions of this package, so
	// they must remain decodable.
	PayloadVersionLegacy PayloadVersion = 1

	// PayloadVersionCurrent is the versioned payload format written by this
	// version of the package. It carries the deadline, the session values and
	// any forward-compatible extension fields in a deterministic, checksummed
	// envelope.
	PayloadVersionCurrent PayloadVersion = 2
)

// Errors returned when a session payload cannot be decoded. They are
// deliberately distinct so that callers can tell a payload which was cut short
// from one whose declared length or checksum does not match its contents.
var (
	// ErrPayloadTruncated is returned when a payload ends before all of its
	// declared bytes are present.
	ErrPayloadTruncated = errors.New("scs: session payload is truncated")

	// ErrPayloadLengthMismatch is returned when a payload contains more bytes
	// than its header declares, or trailing bytes after the encoded data.
	ErrPayloadLengthMismatch = errors.New("scs: session payload length does not match its header")

	// ErrPayloadChecksumMismatch is returned when the checksum in a payload
	// header does not match the payload contents.
	ErrPayloadChecksumMismatch = errors.New("scs: session payload checksum does not match")

	// ErrPayloadVersionUnsupported is returned when a payload uses a version,
	// feature flag or framing which this version of the package does not
	// understand.
	ErrPayloadVersionUnsupported = errors.New("scs: unsupported session payload version")

	// ErrPayloadCorrupt is returned when a payload is not a valid encoding for
	// its declared version.
	ErrPayloadCorrupt = errors.New("scs: session payload is corrupt")

	// ErrPayloadDeadlineInvalid is returned when a payload explicitly carries
	// a deadline which is not a representable time value.
	ErrPayloadDeadlineInvalid = errors.New("scs: session payload deadline is invalid")
)

// PayloadMeta contains the metadata which is carried alongside the deadline
// and values in a session payload.
type PayloadMeta struct {
	// Version is the version of the payload which was decoded. For payloads
	// produced by a Codec which does not implement ExtendedCodec it is zero.
	Version PayloadVersion

	// Extensions contains forward-compatible fields which are not interpreted
	// by this version of the package. They are retained when the payload is
	// decoded and written back out when it is next encoded, so that rolling
	// upgrades to a newer version do not discard data written by that version.
	Extensions map[string]interface{}
}

// ExtendedCodec is an optional interface for Codecs which can preserve
// payload-level metadata, such as forward-compatible extension fields, across a
// decode/encode cycle. SessionManager uses it automatically when the configured
// Codec implements it, and otherwise falls back to the plain Codec interface.
type ExtendedCodec interface {
	Codec

	// EncodePayload is like Encode, but also writes the supplied payload
	// metadata. Implementations should write the current payload version; the
	// Version field of meta is informational.
	EncodePayload(deadline time.Time, values map[string]interface{}, meta PayloadMeta) ([]byte, error)

	// DecodePayload is like Decode, but also returns the payload metadata.
	DecodePayload([]byte) (deadline time.Time, values map[string]interface{}, meta PayloadMeta, err error)
}

// Codec is the interface for encoding/decoding session data to and from a byte
// slice for use by the session store.
type Codec interface {
	Encode(deadline time.Time, values map[string]interface{}) ([]byte, error)
	Decode([]byte) (deadline time.Time, values map[string]interface{}, err error)
}

const (
	// payloadHeaderSize is the size of the fixed header which precedes the
	// gob-encoded body of a versioned payload.
	//
	//	offset 0..3: magic bytes
	//	offset 4:    payload version
	//	offset 5:    feature flags
	//	offset 6..7: reserved (must be zero)
	//	offset 8..11: body length, big endian
	//	offset 12..15: CRC-32C of the body, big endian
	payloadHeaderSize = 16

	// payloadFlagMask is the set of feature flags understood by this version of
	// the package. No flags are currently defined.
	payloadFlagMask = 0
)

// payloadMagic prefixes every versioned payload.
//
// A gob stream begins with the length of its first type name, encoded as an
// unsigned integer. Go's gob encoder writes that length as a single byte in the
// range 0x00-0x7f when it fits, and otherwise writes a leading byte in the
// range 0xf8-0xff. A first byte of 0x89 therefore can never start a valid gob
// stream, so a legacy payload, which is a bare gob stream, can never be
// misread as a versioned payload regardless of the type names it contains.
var payloadMagic = [4]byte{0x89, 'S', 'C', 'P'}

var payloadChecksumTable = crc32.MakeTable(crc32.Castagnoli)

// payloadEntry is a single key/value pair in a versioned payload. Values and
// extensions are encoded as slices of entries, sorted by key, rather than as
// maps: gob encodes maps in Go's randomised iteration order, which would make
// re-encoding a payload non-deterministic.
type payloadEntry struct {
	Key   string
	Value interface{}
}

// payloadBody is the gob-encoded body of a versioned payload.
type payloadBody struct {
	// Deadline is the absolute expiry time for the session. HasDeadline
	// distinguishes a missing deadline (which is filled in using the
	// SessionManager defaults when the payload is loaded) from an explicitly
	// supplied one. A payload with HasDeadline set must carry a valid,
	// non-zero time.
	Deadline    time.Time
	HasDeadline bool

	// Values holds the session values, sorted by key.
	Values []payloadEntry

	// Extensions holds payload-level fields which this version does not
	// interpret but which must be preserved when the payload is re-encoded.
	Extensions []payloadEntry
}

// GobCodec is used for encoding/decoding session data to and from a byte
// slice using the encoding/gob package.
//
// Decode accepts both the legacy payload format and the current, versioned
// format. Encode writes the current format unless WriteLegacy is set.
type GobCodec struct {
	// WriteLegacy makes Encode write the legacy payload format. This is useful
	// during a rolling upgrade: instances running this version can read
	// payloads written by older instances, and older instances can continue to
	// read the payloads written by this version until every instance has been
	// upgraded. Once all instances run a version which understands the current
	// payload format, WriteLegacy can be turned off.
	//
	// The legacy format cannot carry extension fields and, because it uses a
	// gob map, is not deterministically encoded. If extension fields are
	// present they are written using the current format regardless of this
	// setting.
	WriteLegacy bool
}

// Encode converts a session deadline and values into a byte slice.
func (c GobCodec) Encode(deadline time.Time, values map[string]interface{}) ([]byte, error) {
	return c.EncodePayload(deadline, values, PayloadMeta{})
}

// EncodePayload converts a session deadline, values and payload metadata into a
// byte slice.
func (c GobCodec) EncodePayload(deadline time.Time, values map[string]interface{}, meta PayloadMeta) ([]byte, error) {
	if err := validatePayloadDeadline(deadline); err != nil {
		return nil, err
	}

	// The legacy format has no place to store extension fields. Fall back to
	// the current format rather than silently dropping them.
	if c.WriteLegacy && len(meta.Extensions) == 0 {
		return encodeLegacyPayload(deadline, values)
	}

	body := payloadBody{
		Deadline:    deadline,
		HasDeadline: !deadline.IsZero(),
		Values:      sortedPayloadEntries(values),
		Extensions:  sortedPayloadEntries(meta.Extensions),
	}

	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(&body); err != nil {
		return nil, err
	}
	encoded := buf.Bytes()

	if uint64(len(encoded)) > uint64(^uint32(0)) {
		return nil, fmt.Errorf("scs: encoded session payload is too large: %d bytes", len(encoded))
	}

	payload := make([]byte, payloadHeaderSize+len(encoded))
	copy(payload[0:4], payloadMagic[:])
	payload[4] = byte(PayloadVersionCurrent)
	payload[5] = 0
	payload[6] = 0
	payload[7] = 0
	binary.BigEndian.PutUint32(payload[8:12], uint32(len(encoded)))
	binary.BigEndian.PutUint32(payload[12:16], crc32.Checksum(encoded, payloadChecksumTable))
	copy(payload[payloadHeaderSize:], encoded)

	return payload, nil
}

// Decode converts a byte slice into a session deadline and values. It accepts
// both legacy and current payloads and returns an error which wraps one of the
// ErrPayload* sentinels if the payload is truncated, malformed or fails its
// checksum. On error the returned deadline and values are always zero and nil;
// a partially decoded session is never returned.
func (c GobCodec) Decode(b []byte) (time.Time, map[string]interface{}, error) {
	deadline, values, _, err := c.DecodePayload(b)
	return deadline, values, err
}

// DecodePayload converts a byte slice into a session deadline, values and
// payload metadata. It accepts both legacy and current payloads.
func (GobCodec) DecodePayload(b []byte) (time.Time, map[string]interface{}, PayloadMeta, error) {
	if looksLikeVersionedPayload(b) {
		return decodeVersionedPayload(b)
	}
	return decodeLegacyPayload(b)
}

// encodeLegacyPayload writes the original payload format: a gob-encoded struct
// with a Deadline and a map of Values. This is the exact struct used by older
// versions of this package.
func encodeLegacyPayload(deadline time.Time, values map[string]interface{}) ([]byte, error) {
	aux := &struct {
		Deadline time.Time
		Values   map[string]interface{}
	}{
		Deadline: deadline,
		Values:   values,
	}

	var b bytes.Buffer
	if err := gob.NewEncoder(&b).Encode(&aux); err != nil {
		return nil, err
	}

	return b.Bytes(), nil
}

// decodeLegacyPayload decodes the original payload format. The deadline is
// returned as the zero time if the payload did not contain one, which the
// SessionManager treats as a missing deadline rather than an expired session.
func decodeLegacyPayload(b []byte) (time.Time, map[string]interface{}, PayloadMeta, error) {
	aux := &struct {
		Deadline time.Time
		Values   map[string]interface{}
	}{}

	r := bytes.NewReader(b)
	if err := gob.NewDecoder(r).Decode(aux); err != nil {
		return time.Time{}, nil, PayloadMeta{}, classifyGobDecodeError(err)
	}
	if r.Len() != 0 {
		return time.Time{}, nil, PayloadMeta{}, fmt.Errorf("%w: %d trailing bytes after legacy payload", ErrPayloadLengthMismatch, r.Len())
	}

	if err := validatePayloadDeadline(aux.Deadline); err != nil {
		return time.Time{}, nil, PayloadMeta{}, err
	}

	values := aux.Values
	if values == nil {
		values = make(map[string]interface{})
	}

	return aux.Deadline, values, PayloadMeta{Version: PayloadVersionLegacy}, nil
}

// decodeVersionedPayload decodes the versioned payload format.
func decodeVersionedPayload(b []byte) (time.Time, map[string]interface{}, PayloadMeta, error) {
	if len(b) < payloadHeaderSize {
		return time.Time{}, nil, PayloadMeta{}, fmt.Errorf("%w: got %d of %d header bytes", ErrPayloadTruncated, len(b), payloadHeaderSize)
	}

	version := PayloadVersion(b[4])
	flags := b[5]
	reserved := binary.BigEndian.Uint16(b[6:8])
	bodyLen := binary.BigEndian.Uint32(b[8:12])
	wantChecksum := binary.BigEndian.Uint32(b[12:16])

	if version != PayloadVersionCurrent {
		return time.Time{}, nil, PayloadMeta{}, fmt.Errorf("%w: %d", ErrPayloadVersionUnsupported, version)
	}
	if flags&^payloadFlagMask != 0 {
		return time.Time{}, nil, PayloadMeta{}, fmt.Errorf("%w: unknown feature flags %#02x", ErrPayloadVersionUnsupported, flags)
	}
	if reserved != 0 {
		return time.Time{}, nil, PayloadMeta{}, fmt.Errorf("%w: non-zero reserved header bytes", ErrPayloadCorrupt)
	}

	body := b[payloadHeaderSize:]
	switch {
	case uint64(len(body)) < uint64(bodyLen):
		return time.Time{}, nil, PayloadMeta{}, fmt.Errorf("%w: got %d of %d body bytes", ErrPayloadTruncated, len(body), bodyLen)
	case uint64(len(body)) > uint64(bodyLen):
		return time.Time{}, nil, PayloadMeta{}, fmt.Errorf("%w: got %d body bytes, header declares %d", ErrPayloadLengthMismatch, len(body), bodyLen)
	}

	if got := crc32.Checksum(body, payloadChecksumTable); got != wantChecksum {
		return time.Time{}, nil, PayloadMeta{}, fmt.Errorf("%w: got %#08x, expected %#08x", ErrPayloadChecksumMismatch, got, wantChecksum)
	}

	bodyReader := bytes.NewReader(body)
	var decoded payloadBody
	if err := gob.NewDecoder(bodyReader).Decode(&decoded); err != nil {
		return time.Time{}, nil, PayloadMeta{}, fmt.Errorf("%w: %v", ErrPayloadCorrupt, err)
	}
	if bodyReader.Len() != 0 {
		return time.Time{}, nil, PayloadMeta{}, fmt.Errorf("%w: %d trailing bytes after payload body", ErrPayloadCorrupt, bodyReader.Len())
	}

	if decoded.HasDeadline {
		if decoded.Deadline.IsZero() {
			return time.Time{}, nil, PayloadMeta{}, fmt.Errorf("%w: payload declares a deadline but carries the zero time", ErrPayloadDeadlineInvalid)
		}
		if err := validatePayloadDeadline(decoded.Deadline); err != nil {
			return time.Time{}, nil, PayloadMeta{}, err
		}
	} else if !decoded.Deadline.IsZero() {
		return time.Time{}, nil, PayloadMeta{}, fmt.Errorf("%w: payload does not declare a deadline but carries a non-zero one", ErrPayloadCorrupt)
	}

	values := payloadEntriesToMap(decoded.Values)
	extensions := payloadEntriesToMap(decoded.Extensions)

	meta := PayloadMeta{
		Version:    PayloadVersionCurrent,
		Extensions: extensions,
	}

	return decoded.Deadline, values, meta, nil
}

// looksLikeVersionedPayload reports whether b starts with the versioned payload
// magic. A short byte slice which is a prefix of the magic is also treated as a
// versioned payload so that it is reported as truncated rather than as a
// malformed legacy payload.
func looksLikeVersionedPayload(b []byte) bool {
	if len(b) >= len(payloadMagic) {
		return bytes.Equal(b[:len(payloadMagic)], payloadMagic[:])
	}
	return bytes.Equal(b, payloadMagic[:len(b)])
}

// classifyGobDecodeError converts a gob decoding error into one of the payload
// error sentinels. gob reports a payload which ends mid-value as
// io.ErrUnexpectedEOF and an empty payload as io.EOF; both are truncation.
// Direct comparison is used rather than errors.Is so that the package keeps
// working with the Go version declared in go.mod.
func classifyGobDecodeError(err error) error {
	if err == io.ErrUnexpectedEOF || err == io.EOF {
		return fmt.Errorf("%w: %v", ErrPayloadTruncated, err)
	}
	return fmt.Errorf("%w: %v", ErrPayloadCorrupt, err)
}

// validatePayloadDeadline reports whether t can be represented in a payload.
// The zero time is allowed and means "no deadline"; the SessionManager fills in
// a default for it. Any other time which cannot be serialised is rejected
// rather than being silently accepted.
func validatePayloadDeadline(t time.Time) error {
	if t.IsZero() {
		return nil
	}
	year := t.Year()
	if year < 0 || year > 9999 {
		return fmt.Errorf("%w: year %d is outside the supported range", ErrPayloadDeadlineInvalid, year)
	}
	if _, err := t.MarshalText(); err != nil {
		return fmt.Errorf("%w: %v", ErrPayloadDeadlineInvalid, err)
	}
	return nil
}

// sortedPayloadEntries converts a map into a slice of entries sorted by key.
// A nil or empty map is encoded as a nil slice so that logically equivalent
// payloads produce identical bytes.
func sortedPayloadEntries(m map[string]interface{}) []payloadEntry {
	if len(m) == 0 {
		return nil
	}

	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	entries := make([]payloadEntry, len(keys))
	for i, k := range keys {
		entries[i] = payloadEntry{Key: k, Value: m[k]}
	}
	return entries
}

// payloadEntriesToMap converts decoded entries back into a map. Duplicate keys
// are resolved by keeping the last value. Empty keys are permitted: the
// SessionManager does not forbid them, so a payload containing one must not
// become undecodable. The returned map is always non-nil so that callers can
// safely write to it.
func payloadEntriesToMap(entries []payloadEntry) map[string]interface{} {
	m := make(map[string]interface{}, len(entries))
	for _, e := range entries {
		m[e.Key] = e.Value
	}
	return m
}
