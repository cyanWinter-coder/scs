package scs

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"fmt"
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

// Status represents the state of the session data during a request cycle.
type Status int

const (
	// Unmodified indicates that the session data hasn't been changed in the
	// current request cycle.
	Unmodified Status = iota

	// Modified indicates that the session data has been changed in the current
	// request cycle.
	Modified

	// Destroyed indicates that the session data has been destroyed in the
	// current request cycle.
	Destroyed

	// Committed indicates that the session data has been committed to the store
	// and no modifications have been made to it since.
	Committed
)

type sessionData struct {
	deadline time.Time
	expiry   time.Time
	status   Status
	// token is the working token for the current request cycle. It is not
	// guaranteed to exist in the session store until the session data has been
	// committed successfully.
	token string
	// originalToken is the token that existed in the session store when the
	// session was loaded. It is empty for new sessions, and is removed from the
	// store only after any replacement token has been committed successfully
	// (e.g. following a token rotation or a destroy-and-recreate cycle).
	originalToken string
	// mergedTokens holds tokens whose data has been merged into this session
	// during the request cycle. They are removed from the store by Commit only
	// after the merged data has been persisted successfully.
	mergedTokens []string
	values       map[string]interface{}
	mu           sync.Mutex
}

func newSessionData(lifetime, idleTimeout time.Duration) *sessionData {
	now := time.Now()
	deadline := now.Add(lifetime).UTC()

	sd := sessionData{
		deadline: deadline,
		expiry:   deadline,
		status:   Unmodified,
		values:   make(map[string]interface{}),
	}

	if idleTimeout > 0 {
		idleExpiry := time.Now().Add(idleTimeout).UTC()
		sd.expiry = minTime(sd.deadline, idleExpiry)
	}

	return &sd
}

func minTime(a, b time.Time) time.Time {
	if a.Before(b) {
		return a
	}
	return b
}

// Load retrieves the session data for the given token from the session store,
// and returns a new context.Context containing the session data. If no matching
// token is found then this will create a new session.
//
// Most applications will use the LoadAndSave() middleware and will not need to
// use this method.
func (s *SessionManager) Load(ctx context.Context, token string) (context.Context, error) {
	if _, ok := ctx.Value(s.ContextKey).(*sessionData); ok {
		return ctx, nil
	}

	if token == "" {
		return s.addSessionDataToContext(ctx, newSessionData(s.Lifetime, s.IdleTimeout)), nil
	}

	b, found, err := s.doStoreFind(ctx, token)
	if err != nil {
		return nil, err
	} else if !found {
		return s.addSessionDataToContext(ctx, newSessionData(s.Lifetime, s.IdleTimeout)), nil
	}

	sd := &sessionData{
		status:        Unmodified,
		token:         token,
		originalToken: token,
	}

	sd.deadline, sd.values, err = s.Codec.Decode(b)
	if err != nil {
		return nil, err
	}

	// If the deadline has already passed, do not restore the session. Returning
	// a fresh session here prevents an expired absolute deadline from being
	// accepted when the store still has the row.
	if !sd.deadline.After(time.Now()) {
		return s.addSessionDataToContext(ctx, newSessionData(s.Lifetime, s.IdleTimeout)), nil
	}

	// By default, set the expiry time to the deadline.
	sd.expiry = sd.deadline

	// If an idle timeout is being used, set the expiry to whichever comes first
	// between the session deadline and idleTimeout expiry. We also set the status
	// to Modified, which will force the session data to be re-committed to the
	// session store with the updated new expiry time.
	if s.IdleTimeout > 0 {
		idleExpiry := time.Now().Add(s.IdleTimeout).UTC()

		sd.expiry = minTime(sd.deadline, idleExpiry)
		sd.status = Modified
	}

	return s.addSessionDataToContext(ctx, sd), nil
}

// Commit saves the session data to the session store and returns the session
// token and expiry time.
//
// If the session data has already been committed and no modifications have been
// made since, then Commit is a no-op and the previously committed token and
// expiry time are returned without writing to the store again.
//
// If Commit fails, then no session data or token changes are considered
// persisted: the session status remains Modified (or Destroyed) and the token
// held by the request is left unchanged, so the caller can retry or report the
// error. In particular, a new token is never swapped in until the store commit
// has succeeded.
//
// Most applications will use the LoadAndSave() middleware and will not need to
// use this method.
func (s *SessionManager) Commit(ctx context.Context) (string, time.Time, error) {
	sd := s.getSessionDataFromContext(ctx)

	sd.mu.Lock()
	defer sd.mu.Unlock()

	// If nothing has changed since the last successful commit, do not write to
	// the store again.
	if sd.status == Committed {
		return sd.token, sd.expiry, nil
	}

	// Stage the token in a local variable so that a failed store commit does
	// not mutate the session data held in the request scope.
	token := sd.token
	if token == "" {
		var err error
		token, err = generateToken()
		if err != nil {
			return "", time.Time{}, err
		}
	}

	b, err := s.Codec.Encode(sd.deadline, sd.values)
	if err != nil {
		return "", time.Time{}, err
	}

	if err := s.doStoreCommit(ctx, token, b, sd.expiry); err != nil {
		// Leave the status and the request-scope token untouched so that the
		// failure remains observable and the change is never presented as
		// persisted.
		return "", time.Time{}, err
	}

	// The replacement token is now safely in the store. Remove the original
	// token last so that a failure here leaves the client's old cookie and
	// store row intact; the request still fails rather than silently switching
	// the client onto the new token.
	if sd.originalToken != "" && sd.originalToken != token {
		if err := s.doStoreDelete(ctx, sd.originalToken); err != nil {
			return "", time.Time{}, err
		}
	}

	// Tokens whose data was merged into this session can now be removed as
	// well. Any failure leaves the request uncommitted so that no merge is
	// presented as durable when it was not.
	for _, mergedToken := range sd.mergedTokens {
		if mergedToken == token {
			continue
		}
		if err := s.doStoreDelete(ctx, mergedToken); err != nil {
			return "", time.Time{}, err
		}
	}
	sd.mergedTokens = nil

	// Only swap the token in once every store operation has succeeded.
	sd.token = token
	sd.originalToken = token
	sd.status = Committed

	return sd.token, sd.expiry, nil
}

// Destroy marks the session data for destruction in the session store and sets
// the session status to Destroyed. Any further operations in the same request
// cycle will result in a new session being created.
//
// The store deletion is staged until the request is committed (either by the
// LoadAndSave middleware or a manual Commit call). Destroy itself does not
// touch the store, so it cannot leave the client holding a cookie for a session
// that has already been removed; if the eventual commit fails, the original
// session stays intact and the error is returned to the caller.
//
// Destroy should only be called once per request cycle, and must be called
// before the response headers are written.
func (s *SessionManager) Destroy(ctx context.Context) error {
	sd := s.getSessionDataFromContext(ctx)

	sd.mu.Lock()
	defer sd.mu.Unlock()

	sd.status = Destroyed

	// Reset everything else to defaults. The original token is retained in
	// originalToken so that the staged deletion can be carried out when the
	// request is committed.
	sd.token = ""
	sd.deadline = time.Now().Add(s.Lifetime).UTC()
	for key := range sd.values {
		delete(sd.values, key)
	}

	return nil
}

// Put adds a key and corresponding value to the session data. Any existing
// value for the key will be replaced. The session data status will be set to
// Modified.
func (s *SessionManager) Put(ctx context.Context, key string, val interface{}) {
	sd := s.getSessionDataFromContext(ctx)

	sd.mu.Lock()
	sd.values[key] = val
	sd.status = Modified
	sd.mu.Unlock()
}

// Get returns the value for a given key from the session data. The return
// value has the type interface{} so will usually need to be type asserted
// before you can use it. For example:
//
//	foo, ok := session.Get(r, "foo").(string)
//	if !ok {
//		return errors.New("type assertion to string failed")
//	}
//
// Also see the GetString(), GetInt(), GetBytes() and other helper methods which
// wrap the type conversion for common types.
func (s *SessionManager) Get(ctx context.Context, key string) interface{} {
	sd := s.getSessionDataFromContext(ctx)

	sd.mu.Lock()
	defer sd.mu.Unlock()

	return sd.values[key]
}

// Pop acts like a one-time Get. It returns the value for a given key from the
// session data and deletes the key and value from the session data. The
// session data status will be set to Modified. The return value has the type
// interface{} so will usually need to be type asserted before you can use it.
func (s *SessionManager) Pop(ctx context.Context, key string) interface{} {
	sd := s.getSessionDataFromContext(ctx)

	sd.mu.Lock()
	defer sd.mu.Unlock()

	val, exists := sd.values[key]
	if !exists {
		return nil
	}
	delete(sd.values, key)
	sd.status = Modified

	return val
}

// Remove deletes the given key and corresponding value from the session data.
// The session data status will be set to Modified. If the key is not present
// this operation is a no-op.
func (s *SessionManager) Remove(ctx context.Context, key string) {
	sd := s.getSessionDataFromContext(ctx)

	sd.mu.Lock()
	defer sd.mu.Unlock()

	_, exists := sd.values[key]
	if !exists {
		return
	}

	delete(sd.values, key)
	sd.status = Modified
}

// Clear removes all data for the current session. The session token and
// lifetime are unaffected. If there is no data in the current session this is
// a no-op.
func (s *SessionManager) Clear(ctx context.Context) error {
	sd := s.getSessionDataFromContext(ctx)

	sd.mu.Lock()
	defer sd.mu.Unlock()

	if len(sd.values) == 0 {
		return nil
	}

	for key := range sd.values {
		delete(sd.values, key)
	}
	sd.status = Modified
	return nil
}

// Exists returns true if the given key is present in the session data.
func (s *SessionManager) Exists(ctx context.Context, key string) bool {
	sd := s.getSessionDataFromContext(ctx)

	sd.mu.Lock()
	_, exists := sd.values[key]
	sd.mu.Unlock()

	return exists
}

// Keys returns a slice of all key names present in the session data, sorted
// alphabetically. If the data contains no data then an empty slice will be
// returned.
func (s *SessionManager) Keys(ctx context.Context) []string {
	sd := s.getSessionDataFromContext(ctx)

	sd.mu.Lock()
	keys := make([]string, len(sd.values))
	i := 0
	for key := range sd.values {
		keys[i] = key
		i++
	}
	sd.mu.Unlock()

	sort.Strings(keys)
	return keys
}

// RenewToken updates the session data to have a new session token while
// retaining the current session data. The session lifetime is also reset and
// the session data status will be set to Modified.
//
// The old session token and accompanying data are deleted from the session
// store only when the changes are committed, and only after the new token has
// been committed successfully first. If the commit fails, the original session
// is left untouched and the client can continue using its existing cookie.
//
// To mitigate the risk of session fixation attacks, it's important that you call
// RenewToken before making any changes to privilege levels (e.g. login and
// logout operations). See https://github.com/OWASP/CheatSheetSeries/blob/master/cheatsheets/Session_Management_Cheat_Sheet.md#renew-the-session-id-after-any-privilege-level-change
// for additional information.
//
// RenewToken should only be called once per request cycle, and must be called
// before the response headers are written.
func (s *SessionManager) RenewToken(ctx context.Context) error {
	sd := s.getSessionDataFromContext(ctx)

	sd.mu.Lock()
	defer sd.mu.Unlock()

	newToken, err := generateToken()
	if err != nil {
		return err
	}

	// Stage the rotation in the request scope. The original token is kept in
	// originalToken and only removed from the store by Commit after the new
	// token has been persisted successfully.
	sd.token = newToken
	sd.deadline = time.Now().Add(s.Lifetime).UTC()
	sd.status = Modified

	return nil
}

// MergeSession is used to merge in data from a different session in case strict
// session tokens are lost across an oauth or similar redirect flows. Use Clear()
// if no values of the new session are to be used.
func (s *SessionManager) MergeSession(ctx context.Context, token string) error {
	sd := s.getSessionDataFromContext(ctx)

	b, found, err := s.doStoreFind(ctx, token)
	if err != nil {
		return err
	} else if !found {
		return nil
	}

	deadline, values, err := s.Codec.Decode(b)
	if err != nil {
		return err
	}

	sd.mu.Lock()
	defer sd.mu.Unlock()

	// If it is the same session, nothing needs to be done.
	if sd.token == token {
		return nil
	}

	if deadline.After(sd.deadline) {
		sd.deadline = deadline
	}

	for k, v := range values {
		sd.values[k] = v
	}

	sd.status = Modified

	// Stage the removal of the merged-from token. It is only deleted once the
	// merged data has been committed successfully, so a failed commit never
	// destroys a session whose data could not be persisted.
	sd.mergedTokens = append(sd.mergedTokens, token)
	return nil
}

// Status returns the current status of the session data.
func (s *SessionManager) Status(ctx context.Context) Status {
	sd := s.getSessionDataFromContext(ctx)

	sd.mu.Lock()
	defer sd.mu.Unlock()

	return sd.status
}

// GetString returns the string value for a given key from the session data.
// The zero value for a string ("") is returned if the key does not exist or the
// value could not be type asserted to a string.
func (s *SessionManager) GetString(ctx context.Context, key string) string {
	val := s.Get(ctx, key)
	str, ok := val.(string)
	if !ok {
		return ""
	}
	return str
}

// GetBool returns the bool value for a given key from the session data. The
// zero value for a bool (false) is returned if the key does not exist or the
// value could not be type asserted to a bool.
func (s *SessionManager) GetBool(ctx context.Context, key string) bool {
	val := s.Get(ctx, key)
	b, ok := val.(bool)
	if !ok {
		return false
	}
	return b
}

// GetInt returns the int value for a given key from the session data. The
// zero value for an int (0) is returned if the key does not exist or the
// value could not be type asserted to an int.
func (s *SessionManager) GetInt(ctx context.Context, key string) int {
	val := s.Get(ctx, key)
	i, ok := val.(int)
	if !ok {
		return 0
	}
	return i
}

// GetInt64 returns the int64 value for a given key from the session data. The
// zero value for an int64 (0) is returned if the key does not exist or the
// value could not be type asserted to an int64.
func (s *SessionManager) GetInt64(ctx context.Context, key string) int64 {
	val := s.Get(ctx, key)
	i, ok := val.(int64)
	if !ok {
		return 0
	}
	return i
}

// GetInt32 returns the int value for a given key from the session data. The
// zero value for an int32 (0) is returned if the key does not exist or the
// value could not be type asserted to an int32.
func (s *SessionManager) GetInt32(ctx context.Context, key string) int32 {
	val := s.Get(ctx, key)
	i, ok := val.(int32)
	if !ok {
		return 0
	}
	return i
}

// GetFloat returns the float64 value for a given key from the session data. The
// zero value for an float64 (0) is returned if the key does not exist or the
// value could not be type asserted to a float64.
func (s *SessionManager) GetFloat(ctx context.Context, key string) float64 {
	val := s.Get(ctx, key)
	f, ok := val.(float64)
	if !ok {
		return 0
	}
	return f
}

// GetBytes returns the byte slice ([]byte) value for a given key from the session
// data. The zero value for a slice (nil) is returned if the key does not exist
// or could not be type asserted to []byte.
func (s *SessionManager) GetBytes(ctx context.Context, key string) []byte {
	val := s.Get(ctx, key)
	b, ok := val.([]byte)
	if !ok {
		return nil
	}
	return b
}

// GetTime returns the time.Time value for a given key from the session data. The
// zero value for a time.Time object is returned if the key does not exist or the
// value could not be type asserted to a time.Time. This can be tested with the
// time.IsZero() method.
func (s *SessionManager) GetTime(ctx context.Context, key string) time.Time {
	val := s.Get(ctx, key)
	t, ok := val.(time.Time)
	if !ok {
		return time.Time{}
	}
	return t
}

// PopString returns the string value for a given key and then deletes it from the
// session data. The session data status will be set to Modified. The zero
// value for a string ("") is returned if the key does not exist or the value
// could not be type asserted to a string.
func (s *SessionManager) PopString(ctx context.Context, key string) string {
	val := s.Pop(ctx, key)
	str, ok := val.(string)
	if !ok {
		return ""
	}
	return str
}

// PopBool returns the bool value for a given key and then deletes it from the
// session data. The session data status will be set to Modified. The zero
// value for a bool (false) is returned if the key does not exist or the value
// could not be type asserted to a bool.
func (s *SessionManager) PopBool(ctx context.Context, key string) bool {
	val := s.Pop(ctx, key)
	b, ok := val.(bool)
	if !ok {
		return false
	}
	return b
}

// PopInt returns the int value for a given key and then deletes it from the
// session data. The session data status will be set to Modified. The zero
// value for an int (0) is returned if the key does not exist or the value could
// not be type asserted to an int.
func (s *SessionManager) PopInt(ctx context.Context, key string) int {
	val := s.Pop(ctx, key)
	i, ok := val.(int)
	if !ok {
		return 0
	}
	return i
}

// PopFloat returns the float64 value for a given key and then deletes it from the
// session data. The session data status will be set to Modified. The zero
// value for an float64 (0) is returned if the key does not exist or the value
// could not be type asserted to a float64.
func (s *SessionManager) PopFloat(ctx context.Context, key string) float64 {
	val := s.Pop(ctx, key)
	f, ok := val.(float64)
	if !ok {
		return 0
	}
	return f
}

// PopBytes returns the byte slice ([]byte) value for a given key and then
// deletes it from the from the session data. The session data status will be
// set to Modified. The zero value for a slice (nil) is returned if the key does
// not exist or could not be type asserted to []byte.
func (s *SessionManager) PopBytes(ctx context.Context, key string) []byte {
	val := s.Pop(ctx, key)
	b, ok := val.([]byte)
	if !ok {
		return nil
	}
	return b
}

// PopTime returns the time.Time value for a given key and then deletes it from
// the session data. The session data status will be set to Modified. The zero
// value for a time.Time object is returned if the key does not exist or the
// value could not be type asserted to a time.Time.
func (s *SessionManager) PopTime(ctx context.Context, key string) time.Time {
	val := s.Pop(ctx, key)
	t, ok := val.(time.Time)
	if !ok {
		return time.Time{}
	}
	return t
}

// RememberMe controls whether the session cookie is persistent (i.e  whether it
// is retained after a user closes their browser). RememberMe only has an effect
// if you have set SessionManager.Cookie.Persist = false (the default is true) and
// you are using the standard LoadAndSave() middleware.
func (s *SessionManager) RememberMe(ctx context.Context, val bool) {
	s.Put(ctx, "__rememberMe", val)
}

// Iterate retrieves all active (i.e. not expired) sessions from the store and
// executes the provided function fn for each session. If the session store
// being used does not support iteration then Iterate will panic.
func (s *SessionManager) Iterate(ctx context.Context, fn func(context.Context) error) error {
	allSessions, err := s.doStoreAll(ctx)
	if err != nil {
		return err
	}

	for token, b := range allSessions {
		sd := &sessionData{
			status:        Unmodified,
			token:         token,
			originalToken: token,
		}

		sd.deadline, sd.values, err = s.Codec.Decode(b)
		if err != nil {
			return err
		}

		ctx = s.addSessionDataToContext(ctx, sd)

		err = fn(ctx)
		if err != nil {
			return err
		}
	}

	return nil
}

// Deadline returns the 'absolute' expiry time for the session. Please note
// that if you are using an idle timeout, it is possible that a session will
// expire due to non-use before the returned deadline.
func (s *SessionManager) Deadline(ctx context.Context) time.Time {
	sd := s.getSessionDataFromContext(ctx)

	sd.mu.Lock()
	defer sd.mu.Unlock()

	return sd.deadline
}

// SetDeadline updates the 'absolute' expiry time for the session. Please note
// that if you are using an idle timeout, it is possible that a session will
// expire due to non-use before the set deadline.
func (s *SessionManager) SetDeadline(ctx context.Context, expire time.Time) {
	sd := s.getSessionDataFromContext(ctx)

	sd.mu.Lock()
	defer sd.mu.Unlock()

	sd.deadline = expire
	if s.IdleTimeout > 0 {
		sd.expiry = minTime(expire, time.Now().Add(s.IdleTimeout).UTC())
	} else {
		sd.expiry = expire
	}
	sd.status = Modified
}

// Expiry returns the expiry time for the session. If you are not using an idle
// timeout, the value returned will be the same as calling the Deadline method.
func (s *SessionManager) Expiry(ctx context.Context) time.Time {
	sd := s.getSessionDataFromContext(ctx)

	sd.mu.Lock()
	defer sd.mu.Unlock()

	return sd.expiry
}

// Token returns the session token. Please note that this will return the
// empty string "" if it is called before the session has been committed to
// the store.
func (s *SessionManager) Token(ctx context.Context) string {
	sd := s.getSessionDataFromContext(ctx)

	sd.mu.Lock()
	defer sd.mu.Unlock()

	return sd.token
}

// SetToken changes the token for the session to a known value. Please take care
// when using this function to ensure that the token you are setting is an
// unguessable value from a trusted source. Most applications will not need to
// use this method.
func (s *SessionManager) SetToken(ctx context.Context, token string) {
	sd := s.getSessionDataFromContext(ctx)

	sd.mu.Lock()
	defer sd.mu.Unlock()

	sd.token = token
	sd.status = Modified
}

func (s *SessionManager) addSessionDataToContext(ctx context.Context, sd *sessionData) context.Context {
	return context.WithValue(ctx, s.ContextKey, sd)
}

func (s *SessionManager) getSessionDataFromContext(ctx context.Context) *sessionData {
	c, ok := ctx.Value(s.ContextKey).(*sessionData)
	if !ok {
		panic("scs: no session data in context")
	}
	return c
}

func generateToken() (string, error) {
	b := make([]byte, 32)
	_, err := rand.Read(b)
	if err != nil {
		return "", err
	}
	return base64.RawURLEncoding.EncodeToString(b), nil
}

func hashToken(token string) string {
	hash := sha256.Sum256([]byte(token))
	return base64.RawURLEncoding.EncodeToString(hash[:])
}

type contextKey string

var (
	contextKeyID      uint64
	contextKeyIDMutex = &sync.Mutex{}
)

func generateContextKey() contextKey {
	contextKeyIDMutex.Lock()
	defer contextKeyIDMutex.Unlock()
	atomic.AddUint64(&contextKeyID, 1)
	return contextKey(fmt.Sprintf("session.%d", contextKeyID))
}

func (s *SessionManager) doStoreDelete(ctx context.Context, token string) (err error) {
	if s.HashTokenInStore {
		token = hashToken(token)
	}
	c, ok := s.Store.(interface {
		DeleteCtx(context.Context, string) error
	})
	if ok {
		return c.DeleteCtx(ctx, token)
	}
	return s.Store.Delete(token)
}

func (s *SessionManager) doStoreFind(ctx context.Context, token string) (b []byte, found bool, err error) {
	if s.HashTokenInStore {
		token = hashToken(token)
	}
	c, ok := s.Store.(interface {
		FindCtx(context.Context, string) ([]byte, bool, error)
	})
	if ok {
		return c.FindCtx(ctx, token)
	}
	return s.Store.Find(token)
}

func (s *SessionManager) doStoreCommit(ctx context.Context, token string, b []byte, expiry time.Time) (err error) {
	if s.HashTokenInStore {
		token = hashToken(token)
	}
	c, ok := s.Store.(interface {
		CommitCtx(context.Context, string, []byte, time.Time) error
	})
	if ok {
		return c.CommitCtx(ctx, token, b, expiry)
	}
	return s.Store.Commit(token, b, expiry)
}

func (s *SessionManager) doStoreAll(ctx context.Context) (map[string][]byte, error) {
	cs, ok := s.Store.(IterableCtxStore)
	if ok {
		return cs.AllCtx(ctx)
	}

	is, ok := s.Store.(IterableStore)
	if ok {
		return is.All()
	}

	panic(fmt.Sprintf("type %T does not support iteration", s.Store))
}
