package cryptoutil

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"
)

// Key + token generated with Python cryptography.fernet.Fernet — the
// stored gateway.db blobs must decrypt identically in Go.
const pyKey = "CfR-eL8QzhZJQb22PBP1zxH7CzJMFK4LwKvFACh1pfw="
const pyToken = "gAAAAABq2xl8n6wiAyuO3quwI9Hr7QWfPE4JWG4-3oTMYT-fn8EYxshpncVr_FOUXc7PHjYLruPhS0sM5YcBYlNTLpW6WewS26xJ71Donw_AuOLzEZX86Cc="

func TestDecryptPythonToken(t *testing.T) {
	box, err := LoadKey(pyKey, t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	plain, ok := box.Decrypt([]byte(pyToken))
	if !ok || string(plain) != "cross-language-test" {
		t.Fatalf("cross-language decrypt failed: ok=%v plain=%q", ok, plain)
	}
}

func TestRoundTrip(t *testing.T) {
	box, err := LoadKey(pyKey, t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	tok, err := box.Encrypt([]byte("710000:bot-token"))
	if err != nil {
		t.Fatal(err)
	}
	plain, ok := box.Decrypt(tok)
	if !ok || string(plain) != "710000:bot-token" {
		t.Fatalf("roundtrip failed: %q", plain)
	}
	// wrong key must fail loudly (ok=false), not panic
	otherKey := "ddddddddddddddddddddddddddddddddddddddddddd="
	other, err := LoadKey(otherKey, t.TempDir())
	if err != nil {
		t.Fatalf("second valid key failed to load: %v", err)
	}
	if _, ok := other.Decrypt(tok); ok {
		t.Fatal("wrong key must not decrypt")
	}
}

func TestLoadKeyGeneratedOnceIntoDataDir(t *testing.T) {
	dir := t.TempDir()
	if _, err := LoadKey("", dir); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(dir, ".fernet_key")); err != nil {
		t.Fatal("key file not created")
	}
	// second load must reuse the SAME key (else nothing decrypts)
	raw1, _ := os.ReadFile(filepath.Join(dir, ".fernet_key"))
	if _, err := LoadKey("", dir); err != nil {
		t.Fatal(err)
	}
	raw2, _ := os.ReadFile(filepath.Join(dir, ".fernet_key"))
	if !bytes.Equal(raw1, raw2) {
		t.Fatal("generated key must be stable across loads")
	}
	// and it must actually round-trip tokens it encrypted
	box, _ := LoadKey("", dir)
	tok, _ := box.Encrypt([]byte("x"))
	if p, ok := box.Decrypt(tok); !ok || string(p) != "x" {
		t.Fatal("generated key roundtrip failed")
	}
}

func TestGenerateKeyShape(t *testing.T) {
	k, err := GenerateKey()
	if err != nil {
		t.Fatal(err)
	}
	if len(k) != 44 || k[43] != '=' {
		t.Fatalf("fernet key shape wrong: %q", k)
	}
}

// otherKey is a second VALID fernet key — for wrong-key assertions the
// key must parse (a malformed key is a different failure).
const otherKey = "8uL5cJvWCElDsmuTGDh2Y8ycRYNa2AqX-1frOYlcX0c="

func TestSealBlobRoundTrip(t *testing.T) {
	b, err := LoadKey(pyKey, t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	plain := []byte("gotd session blob with auth_key material")
	sealed, err := b.SealBlob(plain)
	if err != nil {
		t.Fatal(err)
	}
	if !IsSealed(sealed) {
		t.Fatal("sealed blob carries no envelope prefix")
	}
	if bytes.Contains(sealed, plain) {
		t.Fatal("plaintext visible in sealed blob")
	}
	got, err := b.OpenBlob(sealed)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, plain) {
		t.Fatalf("round trip mismatch: %q", got)
	}
}

func TestOpenBlobLegacyPlaintextTolerated(t *testing.T) {
	b, err := LoadKey(pyKey, t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	legacy := []byte("pre-0.1.0 plaintext session blob")
	got, err := b.OpenBlob(legacy)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, legacy) {
		t.Fatalf("legacy blob altered: %q", got)
	}
}

func TestOpenBlobWrongKeyIsHardError(t *testing.T) {
	b, err := LoadKey(pyKey, t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	sealed, err := b.SealBlob([]byte("secret"))
	if err != nil {
		t.Fatal(err)
	}
	wrong, err := LoadKey(otherKey, t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := wrong.OpenBlob(sealed); err == nil {
		t.Fatal("wrong key must be a HARD error — silent misread corrupts sessions")
	}
}
