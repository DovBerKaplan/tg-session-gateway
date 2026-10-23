// Package cryptoutil wraps Fernet encryption of stored bot tokens.
// Token blobs stay byte-compatible with the Python implementation
// (cryptography.fernet.Fernet), so an existing gateway.db migrates
// in place with the same GW_FERNET_KEY.
package cryptoutil

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"

	"github.com/fernet/fernet-go"
)

// Box is a Fernet encrypt/decrypt handle around one key.
type Box struct {
	key *fernet.Key
}

// LoadKey resolves the Fernet key the same way the Python store does:
// explicit key wins; otherwise .fernet_key in the data dir; otherwise a
// key is generated ONCE into the data dir (with a loud warning).
func LoadKey(explicit, dataDir string) (*Box, error) {
	if explicit != "" {
		k, err := fernet.DecodeKey(explicit)
		if err != nil {
			return nil, fmt.Errorf("GW_FERNET_KEY invalid: %w", err)
		}
		return &Box{key: k}, nil
	}
	keyPath := filepath.Join(dataDir, ".fernet_key")
	if raw, err := os.ReadFile(keyPath); err == nil {
		k, err := fernet.DecodeKey(string(bytes.TrimSpace(raw)))
		if err != nil {
			return nil, fmt.Errorf("%s invalid: %w", keyPath, err)
		}
		return &Box{key: k}, nil
	}
	k := new(fernet.Key)
	if err := k.Generate(); err != nil {
		return nil, err
	}
	if err := os.WriteFile(keyPath, []byte(k.Encode()), 0o600); err != nil {
		return nil, err
	}
	fmt.Fprintf(os.Stderr,
		"WARNING: GW_FERNET_KEY not set — generated a key at %s (back it up; losing it loses the stored tokens)\n",
		keyPath)
	return &Box{key: k}, nil
}

// Encrypt seals plaintext into a Fernet token (byte-compatible with
// Python's Fernet.encrypt).
func (b *Box) Encrypt(plain []byte) ([]byte, error) {
	return fernet.EncryptAndSign(plain, b.key)
}

// Decrypt opens a Fernet token; ok=false means the token is invalid for
// this key (logged as "wrong GW_FERNET_KEY?" by the caller).
func (b *Box) Decrypt(token []byte) (plain []byte, ok bool) {
	msg := fernet.VerifyAndDecrypt(token, 0, []*fernet.Key{b.key})
	return msg, msg != nil
}

// GenerateKey returns a fresh Fernet key in encoded form (the `genkey`
// subcommand).
func GenerateKey() (string, error) {
	k := new(fernet.Key)
	if err := k.Generate(); err != nil {
		return "", err
	}
	return k.Encode(), nil
}
