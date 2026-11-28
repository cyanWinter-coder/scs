package scs

import (
	"bytes"
	"context"
	"encoding/gob"
	"errors"
	"net/http"
	"net/http/httptest"
	"reflect"
	"testing"
	"time"
)

// TestLoadLegacyPayload covers loading a payload written by an older version of
// the package, including the deadline defaulting rule.
func TestLoadLegacyPayload(t *testing.T) {
	t.Parallel()

	t.Run("deadline is preserved", func(t *testing.T) {
		t.Parallel()

		s := New()
		deadline := time.Now().Add(2 * time.Hour).UTC().Truncate(time.Second)
		payload := encodeLegacyTestPayload(t, deadline, map[string]interface{}{"userID": "42"})

		if err := s.Store.Commit("token", payload, deadline); err != nil {
			t.Fatalf("committing payload: %v", err)
		}

		ctx, err := s.Load(context.Background(), "token")
		if err != nil {
			t.Fatalf("Load() error: %v", err)
		}
		if got := s.GetString(ctx, "userID"); got != "42" {
			t.Errorf("userID = %q; want %q", got, "42")
		}
		if got := s.Deadline(ctx); !got.Equal(deadline) {
			t.Errorf("Deadline() = %v; want %v", got, deadline)
		}
		if got := s.Status(ctx); got != Unmodified {
			t.Errorf("Status() = %v; want Unmodified", got)
		}
	})

	t.Run("missing deadline is filled in and written back", func(t *testing.T) {
		t.Parallel()

		s := New()
		s.Lifetime = 3 * time.Hour

		// A legacy payload with no deadline at all decodes to the zero time.
		var payload = encodeLegacyTestPayload(t, time.Time{}, map[string]interface{}{"userID": "42"})

		if err := s.Store.Commit("token", payload, time.Now().Add(time.Hour)); err != nil {
			t.Fatalf("committing payload: %v", err)
		}

		before := time.Now()
		ctx, err := s.Load(context.Background(), "token")
		if err != nil {
			t.Fatalf("Load() error: %v", err)
		}

		deadline := s.Deadline(ctx)
		if deadline.IsZero() {
			t.Fatal("Deadline() is still the zero time")
		}
		if deadline.Before(before.Add(s.Lifetime)) || deadline.After(before.Add(s.Lifetime).Add(time.Minute)) {
			t.Errorf("Deadline() = %v; want approximately %v", deadline, before.Add(s.Lifetime))
		}
		if got := s.Status(ctx); got != Modified {
			t.Errorf("Status() = %v; want Modified so the default deadline is written back", got)
		}
		if got := s.GetString(ctx, "userID"); got != "42" {
			t.Errorf("userID = %q; want %q", got, "42")
		}
	})

	t.Run("current payload without a declared deadline is defaulted", func(t *testing.T) {
		t.Parallel()

		s := New()
		s.Lifetime = 2 * time.Hour

		// An envelope with HasDeadline unset must be treated as a missing
		// deadline, not as an already-expired session.
		payload := encodeVersionedTestPayload(t, &payloadBody{
			Values: []payloadEntry{{Key: "userID", Value: "42"}},
		})
		if err := s.Store.Commit("token", payload, time.Now().Add(time.Hour)); err != nil {
			t.Fatalf("committing payload: %v", err)
		}

		ctx, err := s.Load(context.Background(), "token")
		if err != nil {
			t.Fatalf("Load() error: %v", err)
		}
		if s.Deadline(ctx).IsZero() {
			t.Fatal("Deadline() is still the zero time")
		}
		if s.Status(ctx) != Modified {
			t.Errorf("Status() = %v; want Modified", s.Status(ctx))
		}
		if got := s.GetString(ctx, "userID"); got != "42" {
			t.Errorf("userID = %q; want %q", got, "42")
		}
	})

	t.Run("missing deadline defaults are stable across load and commit", func(t *testing.T) {
		t.Parallel()

		s := New()
		s.Lifetime = 3 * time.Hour

		payload := encodeLegacyTestPayload(t, time.Time{}, map[string]interface{}{"userID": "42"})
		if err := s.Store.Commit("token", payload, time.Now().Add(time.Hour)); err != nil {
			t.Fatalf("committing payload: %v", err)
		}

		ctx, err := s.Load(context.Background(), "token")
		if err != nil {
			t.Fatalf("Load() error: %v", err)
		}
		wantDeadline := s.Deadline(ctx)

		token, _, err := s.Commit(ctx)
		if err != nil {
			t.Fatalf("Commit() error: %v", err)
		}

		reloaded, err := s.Load(context.Background(), token)
		if err != nil {
			t.Fatalf("Load() error: %v", err)
		}
		if got := s.Deadline(reloaded); !got.Equal(wantDeadline) {
			t.Errorf("Deadline() after reload = %v; want %v", got, wantDeadline)
		}
	})
}

// TestExtensionFieldsSurviveSessionCycle ensures extension fields decoded from
// a payload are retained when the session is committed again.
func TestExtensionFieldsSurviveSessionCycle(t *testing.T) {
	t.Parallel()

	s := New()
	deadline := time.Now().Add(time.Hour).UTC().Truncate(time.Second)
	extensions := map[string]interface{}{
		"newField":   "some value",
		"newerField": int64(7),
	}

	payload, err := s.Codec.(ExtendedCodec).EncodePayload(deadline, map[string]interface{}{"userID": "42"}, PayloadMeta{Extensions: extensions})
	if err != nil {
		t.Fatalf("encoding payload: %v", err)
	}
	if err := s.Store.Commit("token", payload, deadline); err != nil {
		t.Fatalf("committing payload: %v", err)
	}

	ctx, err := s.Load(context.Background(), "token")
	if err != nil {
		t.Fatalf("Load() error: %v", err)
	}

	// Modify the session so it is committed again, then check the extension
	// fields are still present in the stored payload.
	s.Put(ctx, "extra", "value")
	token, _, err := s.Commit(ctx)
	if err != nil {
		t.Fatalf("Commit() error: %v", err)
	}

	b, found, err := s.Store.Find(token)
	if err != nil || !found {
		t.Fatalf("Find() = %v, %v, %v", b, found, err)
	}
	_, values, meta, err := s.Codec.(ExtendedCodec).DecodePayload(b)
	if err != nil {
		t.Fatalf("DecodePayload() error: %v", err)
	}
	if !reflect.DeepEqual(meta.Extensions, extensions) {
		t.Errorf("extensions = %#v; want %#v", meta.Extensions, extensions)
	}
	if values["userID"] != "42" || values["extra"] != "value" {
		t.Errorf("values = %#v; want userID=42 and extra=value", values)
	}
}

// TestDecodeErrorProducesNoPartialSession ensures a corrupt payload never
// results in a usable session.
func TestDecodeErrorProducesNoPartialSession(t *testing.T) {
	t.Parallel()

	for _, tt := range []struct {
		name    string
		payload []byte
		target  error
	}{
		{
			name:    "truncated current payload",
			payload: mustEncodePayload(t)[:10],
			target:  ErrPayloadTruncated,
		},
		{
			name:    "checksum mismatch",
			payload: corruptChecksum(t, mustEncodePayload(t)),
			target:  ErrPayloadChecksumMismatch,
		},
		{
			name:    "trailing bytes",
			payload: append(mustEncodePayload(t), 0x00),
			target:  ErrPayloadLengthMismatch,
		},
		{
			name:    "unclassified legacy garbage",
			payload: mustEncodeGobValue(t, "not a session payload"),
			target:  ErrPayloadCorrupt,
		},
	} {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			s := New()
			if err := s.Store.Commit("token", tt.payload, time.Now().Add(time.Hour)); err != nil {
				t.Fatalf("committing payload: %v", err)
			}

			ctx, err := s.Load(context.Background(), "token")
			if !errors.Is(err, tt.target) {
				t.Fatalf("Load() error = %v; want %v", err, tt.target)
			}
			if ctx != nil {
				t.Errorf("Load() returned a context alongside the error")
			}
		})
	}
}

// TestWriteLegacyRollingUpgrade ensures payloads written with WriteLegacy can be
// read by an old decoder, and that the current decoder reads both formats.
func TestWriteLegacyRollingUpgrade(t *testing.T) {
	t.Parallel()

	s := New()
	s.Codec = GobCodec{WriteLegacy: true}
	s.Lifetime = time.Hour

	ctx := s.addSessionDataToContext(context.Background(), newSessionData(s.Lifetime, 0))
	s.Put(ctx, "userID", "42")

	token, _, err := s.Commit(ctx)
	if err != nil {
		t.Fatalf("Commit() error: %v", err)
	}

	b, found, err := s.Store.Find(token)
	if err != nil || !found {
		t.Fatalf("Find() = %v, %v, %v", b, found, err)
	}

	// The payload must be decodable by the old struct-only decoder.
	var decoded legacyPayload
	if err := gob.NewDecoder(bytes.NewReader(b)).Decode(&decoded); err != nil {
		t.Fatalf("legacy decoder could not read payload: %v", err)
	}
	if decoded.Values["userID"] != "42" {
		t.Errorf("legacy decoded values = %#v; want userID=42", decoded.Values)
	}

	// And by the current decoder.
	loaded, err := s.Load(context.Background(), token)
	if err != nil {
		t.Fatalf("Load() error: %v", err)
	}
	if got := s.GetString(loaded, "userID"); got != "42" {
		t.Errorf("userID = %q; want %q", got, "42")
	}
}

// TestTokenAndPayloadVersionsAreIndependent ensures changing the payload format
// does not invalidate existing session tokens: an old token keeps working after
// a new payload is written for it.
func TestTokenAndPayloadVersionsAreIndependent(t *testing.T) {
	t.Parallel()

	s := New()
	s.Lifetime = time.Hour

	deadline := time.Now().Add(time.Hour).UTC()
	legacy := encodeLegacyTestPayload(t, deadline, map[string]interface{}{"userID": "42"})
	if err := s.Store.Commit("existing-token", legacy, deadline); err != nil {
		t.Fatalf("committing legacy payload: %v", err)
	}

	ctx, err := s.Load(context.Background(), "existing-token")
	if err != nil {
		t.Fatalf("Load() error: %v", err)
	}

	s.Put(ctx, "extra", "value")
	token, _, err := s.Commit(ctx)
	if err != nil {
		t.Fatalf("Commit() error: %v", err)
	}
	if token != "existing-token" {
		t.Errorf("token = %q; want the existing token to be kept", token)
	}

	reloaded, err := s.Load(context.Background(), "existing-token")
	if err != nil {
		t.Fatalf("Load() error: %v", err)
	}
	if got := s.GetString(reloaded, "userID"); got != "42" {
		t.Errorf("userID = %q; want %q", got, "42")
	}
	if got := s.GetString(reloaded, "extra"); got != "value" {
		t.Errorf("extra = %q; want %q", got, "value")
	}
}

// TestMergeSessionPreservesExtensions ensures extension fields from the merged
// payload are not lost when the current session is committed.
func TestMergeSessionPreservesExtensions(t *testing.T) {
	t.Parallel()

	s := New()
	s.Lifetime = time.Hour

	deadline := time.Now().Add(time.Hour).UTC()
	payload, err := s.Codec.(ExtendedCodec).EncodePayload(
		deadline,
		map[string]interface{}{"fromOther": "yes"},
		PayloadMeta{Extensions: map[string]interface{}{"otherExtension": "value"}},
	)
	if err != nil {
		t.Fatalf("encoding payload: %v", err)
	}
	if err := s.Store.Commit("other-token", payload, deadline); err != nil {
		t.Fatalf("committing payload: %v", err)
	}

	ctx := s.addSessionDataToContext(context.Background(), newSessionData(s.Lifetime, 0))
	s.Put(ctx, "current", "value")
	s.SetToken(ctx, "current-token")

	if err := s.MergeSession(ctx, "other-token"); err != nil {
		t.Fatalf("MergeSession() error: %v", err)
	}
	if got := s.GetString(ctx, "fromOther"); got != "yes" {
		t.Errorf("fromOther = %q; want %q", got, "yes")
	}

	token, _, err := s.Commit(ctx)
	if err != nil {
		t.Fatalf("Commit() error: %v", err)
	}

	b, found, err := s.Store.Find(token)
	if err != nil || !found {
		t.Fatalf("Find() = %v, %v, %v", b, found, err)
	}
	_, _, meta, err := s.Codec.(ExtendedCodec).DecodePayload(b)
	if err != nil {
		t.Fatalf("DecodePayload() error: %v", err)
	}
	if meta.Extensions["otherExtension"] != "value" {
		t.Errorf("extensions = %#v; want otherExtension=value", meta.Extensions)
	}
}

func mustEncodePayload(t *testing.T) []byte {
	t.Helper()

	b, err := (GobCodec{}).Encode(time.Now().Add(time.Hour).UTC(), map[string]interface{}{"key": "value"})
	if err != nil {
		t.Fatalf("encoding payload: %v", err)
	}
	return b
}

// mustEncodeGobValue returns a syntactically valid gob stream for an arbitrary
// value, which is not a valid session payload.
func mustEncodeGobValue(t *testing.T, v interface{}) []byte {
	t.Helper()

	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(v); err != nil {
		t.Fatalf("encoding gob value: %v", err)
	}
	return buf.Bytes()
}

func corruptChecksum(t *testing.T, payload []byte) []byte {
	t.Helper()

	corrupt := append([]byte{}, payload...)
	corrupt[len(corrupt)-1] ^= 0xff
	return corrupt
}

// plainCodec is a Codec which implements only the base interface, with no
// payload metadata support. It exercises the fallback path in SessionManager.
type plainCodec struct{}

func (plainCodec) Encode(deadline time.Time, values map[string]interface{}) ([]byte, error) {
	return GobCodec{}.Encode(deadline, values)
}

func (plainCodec) Decode(b []byte) (time.Time, map[string]interface{}, error) {
	return GobCodec{}.Decode(b)
}

func TestSessionManager_PlainCodecFallback(t *testing.T) {
	t.Parallel()

	s := New()
	s.Codec = plainCodec{}

	ctx := s.addSessionDataToContext(context.Background(), newSessionData(time.Hour, 0))
	s.Put(ctx, "userID", "42")

	token, _, err := s.Commit(ctx)
	if err != nil {
		t.Fatalf("Commit() error: %v", err)
	}

	loaded, err := s.Load(context.Background(), token)
	if err != nil {
		t.Fatalf("Load() error: %v", err)
	}
	if got := s.GetString(loaded, "userID"); got != "42" {
		t.Errorf("userID = %q; want %q", got, "42")
	}
	if err := s.Iterate(context.Background(), func(context.Context) error { return nil }); err != nil {
		t.Fatalf("Iterate() error: %v", err)
	}
}

// TestIterateLegacyPayload ensures Iterate decodes legacy payloads and gives
// them the same default deadline as Load.
func TestIterateLegacyPayload(t *testing.T) {
	t.Parallel()

	s := New()
	s.Lifetime = 2 * time.Hour

	deadline := time.Now().Add(time.Hour).UTC()
	if err := s.Store.Commit("with-deadline", encodeLegacyTestPayload(t, deadline, map[string]interface{}{"key": "a"}), deadline); err != nil {
		t.Fatalf("committing payload: %v", err)
	}
	if err := s.Store.Commit("without-deadline", encodeLegacyTestPayload(t, time.Time{}, map[string]interface{}{"key": "b"}), time.Now().Add(time.Hour)); err != nil {
		t.Fatalf("committing payload: %v", err)
	}

	seen := map[string]bool{}
	err := s.Iterate(context.Background(), func(ctx context.Context) error {
		seen[s.GetString(ctx, "key")] = true
		if s.Deadline(ctx).IsZero() {
			t.Errorf("Deadline() is zero for a session surfaced by Iterate")
		}
		if !s.Deadline(ctx).Equal(s.Expiry(ctx)) {
			t.Errorf("Expiry() = %v; want it to equal Deadline() = %v", s.Expiry(ctx), s.Deadline(ctx))
		}
		return nil
	})
	if err != nil {
		t.Fatalf("Iterate() error: %v", err)
	}
	if !seen["a"] || !seen["b"] {
		t.Errorf("Iterate() saw %#v; want both sessions", seen)
	}
}

// TestLoadAndSaveLegacyCookie covers a client which holds a cookie and a stored
// legacy payload from before the upgrade: the request must keep working and the
// token must not change.
func TestLoadAndSaveLegacyCookie(t *testing.T) {
	t.Parallel()

	s := New()
	s.Lifetime = time.Hour

	deadline := time.Now().Add(time.Hour).UTC()
	if err := s.Store.Commit("client-token", encodeLegacyTestPayload(t, deadline, map[string]interface{}{"userID": "42"}), deadline); err != nil {
		t.Fatalf("committing payload: %v", err)
	}

	var gotUser string
	handler := s.LoadAndSave(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotUser = s.GetString(r.Context(), "userID")
	}))

	req := httptest.NewRequest(http.MethodGet, "/", nil)
	req.AddCookie(&http.Cookie{Name: s.Cookie.Name, Value: "client-token"})
	rec := httptest.NewRecorder()

	handler.ServeHTTP(rec, req)

	if gotUser != "42" {
		t.Errorf("handler saw userID = %q; want %q", gotUser, "42")
	}
	if got := rec.Header().Get("Set-Cookie"); got != "" {
		t.Errorf("Set-Cookie = %q; want no new cookie for an unmodified session", got)
	}
}
