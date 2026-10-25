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

// blobEnvelopeV1 marks fernet-sealed at-rest blobs (session files and
// anything else that is NOT a python-compatible token). Tokens stay
// bare fernet tokens for byte compatibility with the Python store;
// every OTHER encrypted blob carries this prefix.
var blobEnvelopeV1 = []byte("tggw1:")

// SealBlob seals an at-rest blob with a versioned envelope. Unlike
// Encrypt (python-compatible bare tokens), the envelope makes the
// format explicit and lets reads distinguish "wrong key" from
// "legacy plaintext".
func (b *Box) SealBlob(plain []byte) ([]byte, error) {
	token, err := b.Encrypt(plain)
	if err != nil {
		return nil, err
	}
	out := make([]byte, 0, len(blobEnvelopeV1)+len(token))
	out = append(out, blobEnvelopeV1...)
	return append(out, token...), nil
}

// OpenBlob opens a SealBlob-sealed blob. Legacy PLAINTEXT blobs (the
// pre-0.1.0 session storage) are returned as-is so an existing volume
// migrates with zero downtime; the next write re-seals. A sealed blob
// that fails to open is a hard error: with the wrong GW_FERNET_KEY the
// data is unrecoverable and pretending otherwise corrupts sessions.
func (b *Box) OpenBlob(blob []byte) ([]byte, error) {
	if !bytes.HasPrefix(blob, blobEnvelopeV1) {
		return blob, nil // legacy plaintext — re-sealed on next write
	}
	plain, ok := b.Decrypt(blob[len(blobEnvelopeV1):])
	if !ok {
		return nil, fmt.Errorf("sealed blob failed to decrypt: wrong GW_FERNET_KEY for this volume?")
	}
	return plain, nil
}

// IsSealed reports whether a blob carries the at-rest envelope.
func IsSealed(blob []byte) bool { return bytes.HasPrefix(blob, blobEnvelopeV1) }
