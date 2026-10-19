// Package releasekey is a release's key: Size random bytes, written once
// as hex to a file readable by its owner only. On a route with a
// confidential limit, the fingerprints of withheld values and the
// manifest's references to retained files are HMAC-SHA256 under it
// (Key.Sum), which cannot be matched against guessed values without the
// key; the schedule freezes each configuration as loaded by one.
package releasekey

import (
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
)

// Size is the key's length in bytes.
const Size = 32

// fileSize is the key file's length: the key in lowercase hex and a
// newline.
const fileSize = 2*Size + 1

// Key is a release key. It prints as "[release key]" under every fmt
// verb, and its fields are unexported, so encoding/json writes none of
// its bytes.
type Key struct{ b []byte }

// New returns a key of Size bytes from crypto/rand.
func New() (*Key, error) {
	b := make([]byte, Size)
	if _, err := io.ReadFull(rand.Reader, b); err != nil {
		return nil, fmt.Errorf("release key: %w", err)
	}
	return &Key{b: b}, nil
}

// Create writes a new key to path as lowercase hex and a newline,
// readable by its owner only. It refuses an existing file.
func Create(path string) error {
	k, err := New()
	if err != nil {
		return err
	}
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		return fmt.Errorf("release key: %w", err)
	}
	_, err = f.Write([]byte(hex.EncodeToString(k.b) + "\n"))
	if err = errors.Join(err, f.Close()); err != nil {
		os.Remove(path)
		return fmt.Errorf("release key: %w", err)
	}
	return nil
}

// Read reads the key file at path. It refuses a file other users can
// read and one that is not Size bytes in lowercase hex and a newline.
// Its errors never carry the file's content.
func Read(path string) (*Key, error) {
	info, err := os.Stat(path)
	if err != nil {
		return nil, fmt.Errorf("release key: %w", err)
	}
	if info.Mode().Perm()&0o077 != 0 {
		return nil, fmt.Errorf("release key %s: mode %#o lets other users read it; chmod 600 it", path, info.Mode().Perm())
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("release key: %w", err)
	}
	if len(raw) != fileSize || raw[fileSize-1] != '\n' || !lowerHex(raw[:fileSize-1]) {
		return nil, fmt.Errorf("release key %s: not %d lowercase hex digits and a newline", path, 2*Size)
	}
	b := make([]byte, Size)
	if _, err := hex.Decode(b, raw[:fileSize-1]); err != nil {
		return nil, fmt.Errorf("release key %s: not %d lowercase hex digits and a newline", path, 2*Size)
	}
	return &Key{b: b}, nil
}

func lowerHex(b []byte) bool {
	for _, c := range b {
		if !(c >= '0' && c <= '9' || c >= 'a' && c <= 'f') {
			return false
		}
	}
	return true
}

// Sum is the HMAC-SHA256 of b under k, in lowercase hex.
func (k *Key) Sum(b []byte) string {
	m := hmac.New(sha256.New, k.b)
	m.Write(b)
	return hex.EncodeToString(m.Sum(nil))
}

// Format prints the key's name and none of its bytes.
func (Key) Format(f fmt.State, _ rune) {
	io.WriteString(f, "[release key]") //nolint:errcheck
}
