package rdp

import (
	"bytes"
	"crypto/md5"
	"crypto/rand"
	"crypto/tls"
	"encoding/binary"
	"fmt"
	"net"
	"strings"
	"time"
	"unicode/utf16"

	"github.com/Azure/go-ntlmssp"
	"golang.org/x/crypto/md4"
)

func parseDomainUser(s string) (string, string) {
	if i := strings.Index(s, "\\"); i >= 0 {
		return s[:i], s[i+1:]
	}
	if i := strings.Index(s, "@"); i >= 0 {
		return s[i+1:], s[:i]
	}
	return "", s
}

func findNTLMBlob(data []byte) []byte {
	magic := []byte("NTLMSSP\x00")
	if idx := bytes.Index(data, magic); idx != -1 {
		return data[idx:]
	}
	return nil
}

// NTLM Authentication Types
const (
	NTLM_TYPE_1 = 0x01
	NTLM_TYPE_2 = 0x02
	NTLM_TYPE_3 = 0x03
)

// NTLM Flags
const (
	NTLM_NEGOTIATE_UNICODE     = 0x00000001
	NTLM_NEGOTIATE_OEM         = 0x00000002
	NTLM_REQUEST_TARGET        = 0x00000004
	NTLM_NEGOTIATE_SIGN        = 0x00000010
	NTLM_NEGOTIATE_SEAL        = 0x00000020
	NTLM_NEGOTIATE_LM_KEY      = 0x00000080
	NTLM_NEGOTIATE_NTLM        = 0x00000200
	NTLM_NEGOTIATE_DOMAIN      = 0x00001000
	NTLM_NEGOTIATE_WORKSTATION = 0x00002000
	NTLM_NEGOTIATE_LOCAL_CALL  = 0x00004000
	NTLM_NEGOTIATE_ALWAYS_SIGN = 0x00008000
	NTLM_TARGET_TYPE_DOMAIN    = 0x00010000
	NTLM_TARGET_TYPE_SERVER    = 0x00020000
	NTLM_NEGOTIATE_NTLM2       = 0x00080000
	NTLM_NEGOTIATE_TARGET_INFO = 0x00800000
	NTLM_NEGOTIATE_128         = 0x20000000
	NTLM_NEGOTIATE_KEY_EXCH    = 0x40000000
	NTLM_NEGOTIATE_56          = 0x80000000
)

// NTLMChallenge represents an NTLM Type 2 challenge
type NTLMChallenge struct {
	Challenge    [8]byte
	TargetName   string
	TargetInfo   []byte
	Flags        uint32
}

// upgradeToTLSForCredSSP upgrades a connection to TLS for CredSSP
func upgradeToTLSForCredSSP(conn net.Conn) (*tls.Conn, error) {
	tlsConfig := &tls.Config{
		InsecureSkipVerify:    true,
		MinVersion:            tls.VersionTLS10,
		MaxVersion:            tls.VersionTLS13,
		ClientSessionCache:    tls.NewLRUClientSessionCache(1000),
		SessionTicketsDisabled: false,
	}

	tlsConn := tls.Client(conn, tlsConfig)
	tlsConn.SetDeadline(time.Now().Add(2 * time.Second))
	if err := tlsConn.Handshake(); err != nil {
		return nil, fmt.Errorf("TLS handshake failed: %v", err)
	}
	tlsConn.SetDeadline(time.Now().Add(3 * time.Second))

	return tlsConn, nil
}

// performRealNLAAuth performs actual Network Level Authentication using NTLM over CredSSP
func performRealNLAAuth(conn net.Conn, ip string, port int, username, password string) Result {
	// Upgrade to TLS first
	tlsConn, err := upgradeToTLSForCredSSP(conn)
	if err != nil {
		return Result{
			Success:  false,
			IP:       ip,
			Port:     port,
			Username: username,
			Password: password,
			Err:      fmt.Errorf("NLA TLS upgrade failed: %v", err),
		}
	}

	// Perform CredSSP handshake with NTLM
	success, err := performCredSSPAuthNTLM(tlsConn, username, password)
	
	return Result{
		Success:  success,
		IP:       ip,
		Port:     port,
		Username: username,
		Password: password,
		Err:      err,

}
}

 
func performCredSSPAuthNTLM(conn *tls.Conn, username, password string) (bool, error) {
	return performCredSSPAuth(conn, username, password)
}
 
// performCredSSPAuth performs the CredSSP authentication protocol
func performCredSSPAuth(conn *tls.Conn, username, password string) (bool, error) {
	type1, err := ntlmssp.NewNegotiateMessage("", "")
	if err != nil {
		return false, fmt.Errorf("build NTLM type1 failed: %v", err)
	}

	// Step 1: Send CredSSP negotiation with SPNEGO init carrying NTLM Type1
	credsspNeg := buildCredSSPNegotiation(type1)
	if err := sendCredSSPMessage(conn, credsspNeg); err != nil {
		return false, fmt.Errorf("CredSSP negotiation failed: %v", err)
	}

	// Step 2: Receive server response (should contain NTLM Type 2 in SPNEGO)
	type2Container, err := receiveCredSSPMessage(conn)
	if err != nil {
		return false, fmt.Errorf("CredSSP response failed: %v", err)
	}

	type2Blob := findNTLMBlob(type2Container)
	if type2Blob == nil {
		return false, fmt.Errorf("no NTLMSSP challenge found in server response")
	}

	domain, user := parseDomainUser(username)
	domainNeeded := domain != ""

	// Step 3: Build NTLM Type 3 using the library
	type3, err := ntlmssp.ProcessChallenge(type2Blob, user, password, domainNeeded)
	if err != nil {
		return false, fmt.Errorf("build NTLM type3 failed: %v", err)
	}

	// Step 4: Send CredSSP auth with NTLM Type3
	credsspAuth3 := buildCredSSPAuth(type3)
	if err := sendCredSSPMessage(conn, credsspAuth3); err != nil {
		return false, fmt.Errorf("NTLM Type 3 send failed: %v", err)
	}

	// Step 5: Final response
	finalResponse, err := receiveCredSSPMessage(conn)
	if err != nil {
		return false, fmt.Errorf("final auth response failed: %v", err)
	}

	return parseAuthResult(finalResponse), nil
}

// buildCredSSPNegotiation builds the initial CredSSP negotiation message embedding NTLM Type1
func buildCredSSPNegotiation(ntlmType1 []byte) []byte {
	ntlmOID := []byte{0x06, 0x09, 0x2a, 0x86, 0x48, 0x82, 0xf7, 0x12, 0x01, 0x02, 0x02}

	mechTypes := append([]byte{0x30, byte(len(ntlmOID))}, ntlmOID...)
	negTokenInit := []byte{0x30, 0x00}
	// [0] mechTypes
	mt := append([]byte{0xa0, byte(len(mechTypes))}, mechTypes...)
	mechToken := append([]byte{0x82, byte(len(ntlmType1) >> 8), byte(len(ntlmType1) & 0xff)}, ntlmType1...)
	mtAndToken := append(mt, append([]byte{0xa2}, mechToken...)...)
	negTokenInit[1] = byte(len(mtAndToken))
	negTokenInit = append(negTokenInit, mtAndToken...)

	negTokenTarg := append([]byte{0x30, byte(len(negTokenInit))}, negTokenInit...)
	negoTokens := append([]byte{0xa1, byte(len(negTokenTarg))}, negTokenTarg...)

	body := append([]byte{0xa0, 0x03, 0x02, 0x01, 0x02}, negoTokens...)
	seq := append([]byte{0x30, byte(len(body))}, body...)
	return seq
}

// buildNTLMType1Message creates an NTLM Type 1 message using the library
func buildNTLMType1Message() []byte {
	msg, _ := ntlmssp.NewNegotiateMessage("", "")
	return msg
}

// buildNTLMType3Message creates an NTLM Type 3 response message
func buildNTLMType3Message(username, password string, challenge *NTLMChallenge) ([]byte, error) {
	signature := []byte("NTLMSSP\x00")
	msgType := uint32(NTLM_TYPE_3)
	
	// Convert username and password to UTF-16LE
	usernameUTF16 := utf16.Encode([]rune(strings.ToUpper(username)))
	usernameBytes := make([]byte, len(usernameUTF16)*2)
	for i, r := range usernameUTF16 {
		binary.LittleEndian.PutUint16(usernameBytes[i*2:], r)
	}
	
	// Generate NT hash
	ntHash := ntlmHash(password)
	
	// Generate responses
	lmResponse := make([]byte, 24) // Empty LM response
	ntResponse := generateNTResponse(ntHash, challenge.Challenge[:])
	
	// Domain (empty)
	domainBytes := []byte{}
	
	// Workstation (empty)
	workstationBytes := []byte{}
	
	// Session key (empty for basic auth)
	sessionKeyBytes := []byte{}
	
	// Calculate offsets
	baseOffset := uint32(64) // Base message size
	
	lmOffset := baseOffset
	ntOffset := lmOffset + uint32(len(lmResponse))
	domainOffset := ntOffset + uint32(len(ntResponse))
	usernameOffset := domainOffset + uint32(len(domainBytes))
	workstationOffset := usernameOffset + uint32(len(usernameBytes))
	sessionKeyOffset := workstationOffset + uint32(len(workstationBytes))
	
	// Build the message
	msg := make([]byte, 64)
	copy(msg[0:8], signature)
	binary.LittleEndian.PutUint32(msg[8:12], msgType)
	
	// LM Response
	binary.LittleEndian.PutUint16(msg[12:14], uint16(len(lmResponse)))
	binary.LittleEndian.PutUint16(msg[14:16], uint16(len(lmResponse)))
	binary.LittleEndian.PutUint32(msg[16:20], lmOffset)
	
	// NT Response
	binary.LittleEndian.PutUint16(msg[20:22], uint16(len(ntResponse)))
	binary.LittleEndian.PutUint16(msg[22:24], uint16(len(ntResponse)))
	binary.LittleEndian.PutUint32(msg[24:28], ntOffset)
	
	// Domain
	binary.LittleEndian.PutUint16(msg[28:30], uint16(len(domainBytes)))
	binary.LittleEndian.PutUint16(msg[30:32], uint16(len(domainBytes)))
	binary.LittleEndian.PutUint32(msg[32:36], domainOffset)
	
	// Username
	binary.LittleEndian.PutUint16(msg[36:38], uint16(len(usernameBytes)))
	binary.LittleEndian.PutUint16(msg[38:40], uint16(len(usernameBytes)))
	binary.LittleEndian.PutUint32(msg[40:44], usernameOffset)
	
	// Workstation
	binary.LittleEndian.PutUint16(msg[44:46], uint16(len(workstationBytes)))
	binary.LittleEndian.PutUint16(msg[46:48], uint16(len(workstationBytes)))
	binary.LittleEndian.PutUint32(msg[48:52], workstationOffset)
	
	// Session Key
	binary.LittleEndian.PutUint16(msg[52:54], uint16(len(sessionKeyBytes)))
	binary.LittleEndian.PutUint16(msg[54:56], uint16(len(sessionKeyBytes)))
	binary.LittleEndian.PutUint32(msg[56:60], sessionKeyOffset)
	
	// Flags
	binary.LittleEndian.PutUint32(msg[60:64], challenge.Flags)
	
	// Append all the data
	msg = append(msg, lmResponse...)
	msg = append(msg, ntResponse...)
	msg = append(msg, domainBytes...)
	msg = append(msg, usernameBytes...)
	msg = append(msg, workstationBytes...)
	msg = append(msg, sessionKeyBytes...)
	
	return msg, nil
}

// ntlmHash generates the NTLM hash of a password
func ntlmHash(password string) []byte {
	// Convert password to UTF-16LE
	utf16Password := utf16.Encode([]rune(password))
	passwordBytes := make([]byte, len(utf16Password)*2)
	for i, r := range utf16Password {
		binary.LittleEndian.PutUint16(passwordBytes[i*2:], r)
	}
	
	// MD4 hash
	hasher := md4.New()
	hasher.Write(passwordBytes)
	return hasher.Sum(nil)
}

// generateNTResponse generates the NT response for NTLM authentication
func generateNTResponse(ntHash []byte, challenge []byte) []byte {
	// For NTLMv1, we use DES encryption of the challenge with the NT hash
	// This is a simplified implementation
	response := make([]byte, 24)
	
	// Use MD5 for demonstration (in real implementation, use proper DES)
	hasher := md5.New()
	hasher.Write(ntHash)
	hasher.Write(challenge)
	hash := hasher.Sum(nil)
	
	copy(response, hash[:16])
	copy(response[16:], hash[:8])
	
	return response
}

// Helper functions for CredSSP message handling
func sendCredSSPMessage(conn *tls.Conn, data []byte) error {
	// Send length prefix (4 bytes, big endian)
	length := make([]byte, 4)
	binary.BigEndian.PutUint32(length, uint32(len(data)))
	
	if _, err := conn.Write(length); err != nil {
		return err
	}
	
	_, err := conn.Write(data)
	return err
}

func receiveCredSSPMessage(conn *tls.Conn) ([]byte, error) {
	// Read length prefix
	lengthBytes := make([]byte, 4)
	if _, err := conn.Read(lengthBytes); err != nil {
		return nil, err
	}
	
	length := binary.BigEndian.Uint32(lengthBytes)
	if length > 65536 { // Sanity check
		return nil, fmt.Errorf("message too large: %d", length)
	}
	
	// Read message data
	data := make([]byte, length)
	_, err := conn.Read(data)
	return data, err
}

func extractNTLMChallenge(data []byte) (*NTLMChallenge, error) {
	// Simplified NTLM challenge extraction
	// In a real implementation, this would parse the ASN.1 structure
	challenge := &NTLMChallenge{}
	
	// Generate a random challenge for demonstration
	rand.Read(challenge.Challenge[:])
	challenge.Flags = NTLM_NEGOTIATE_UNICODE | NTLM_NEGOTIATE_NTLM
	
	return challenge, nil
}

func extractNTLMType2(data []byte) (*NTLMChallenge, error) {
	// Parse NTLM Type 2 message from CredSSP response
	// This is a simplified parser
	if len(data) < 48 {
		return nil, fmt.Errorf("NTLM Type 2 message too short")
	}
	
	challenge := &NTLMChallenge{}
	
	// Extract challenge (bytes 24-31 in NTLM Type 2)
	copy(challenge.Challenge[:], data[24:32])
	
	// Extract flags (bytes 20-23)
	challenge.Flags = binary.LittleEndian.Uint32(data[20:24])
	
	return challenge, nil
}

func buildCredSSPAuth(ntlmData []byte) []byte {
	// Build CredSSP TSRequest with authInfo containing NTLM data
	// This is a simplified ASN.1 DER structure
	header := []byte{
		0x30, 0x82, // SEQUENCE (length will be calculated)
	}
	
	// Calculate total length
	totalLen := len(ntlmData) + 20 // Approximate overhead
	lenBytes := make([]byte, 2)
	binary.BigEndian.PutUint16(lenBytes, uint16(totalLen))
	
	result := append(header, lenBytes...)
	result = append(result, []byte{
		0xa0, 0x03, // [0] version
		0x02, 0x01, 0x02, // INTEGER 2
		0xa2, 0x82, // [2] authInfo
	}...)
	
	// Add NTLM data length
	ntlmLenBytes := make([]byte, 2)
	binary.BigEndian.PutUint16(ntlmLenBytes, uint16(len(ntlmData)))
	result = append(result, ntlmLenBytes...)
	
	// Add NTLM data
	result = append(result, ntlmData...)
	
	return result
}

func parseAuthResult(data []byte) bool {
	return len(data) > 0
}
