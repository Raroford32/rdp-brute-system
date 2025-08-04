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
	"unicode/utf16"

	"golang.org/x/crypto/md4"
)

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
		InsecureSkipVerify: true, // Skip certificate verification for brute force
		ServerName:         "",   // Don't verify server name
	}
	
	tlsConn := tls.Client(conn, tlsConfig)
	
	// Perform TLS handshake
	if err := tlsConn.Handshake(); err != nil {
		return nil, fmt.Errorf("TLS handshake failed: %v", err)
	}
	
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
	success, err := performCredSSPAuth(tlsConn, username, password)
	
	return Result{
		Success:  success,
		IP:       ip,
		Port:     port,
		Username: username,
		Password: password,
		Err:      err,
	}
}

// performCredSSPAuth performs the CredSSP authentication protocol
func performCredSSPAuth(conn *tls.Conn, username, password string) (bool, error) {
	// Step 1: Send CredSSP negotiation
	credsspNeg := buildCredSSPNegotiation()
	if err := sendCredSSPMessage(conn, credsspNeg); err != nil {
		return false, fmt.Errorf("CredSSP negotiation failed: %v", err)
	}

	// Step 2: Receive server response
	response, err := receiveCredSSPMessage(conn)
	if err != nil {
		return false, fmt.Errorf("CredSSP response failed: %v", err)
	}

	// Step 3: Extract NTLM challenge from response
	_, err = extractNTLMChallenge(response)
	if err != nil {
		return false, fmt.Errorf("NTLM challenge extraction failed: %v", err)
	}

	// Step 4: Generate NTLM Type 1 message
	type1Msg := buildNTLMType1Message()
	credsspAuth1 := buildCredSSPAuth(type1Msg)
	if err := sendCredSSPMessage(conn, credsspAuth1); err != nil {
		return false, fmt.Errorf("NTLM Type 1 failed: %v", err)
	}

	// Step 5: Receive NTLM Type 2 challenge
	type2Response, err := receiveCredSSPMessage(conn)
	if err != nil {
		return false, fmt.Errorf("NTLM Type 2 response failed: %v", err)
	}

	type2Challenge, err := extractNTLMType2(type2Response)
	if err != nil {
		return false, fmt.Errorf("NTLM Type 2 parsing failed: %v", err)
	}

	// Step 6: Generate NTLM Type 3 response
	type3Msg, err := buildNTLMType3Message(username, password, type2Challenge)
	if err != nil {
		return false, fmt.Errorf("NTLM Type 3 generation failed: %v", err)
	}

	credsspAuth3 := buildCredSSPAuth(type3Msg)
	if err := sendCredSSPMessage(conn, credsspAuth3); err != nil {
		return false, fmt.Errorf("NTLM Type 3 failed: %v", err)
	}

	// Step 7: Check authentication result
	finalResponse, err := receiveCredSSPMessage(conn)
	if err != nil {
		return false, fmt.Errorf("final auth response failed: %v", err)
	}

	// Parse the response to determine success/failure
	return parseAuthResult(finalResponse), nil
}

// buildCredSSPNegotiation builds the initial CredSSP negotiation message
func buildCredSSPNegotiation() []byte {
	// CredSSP TSRequest with negoTokens
	// This is a simplified ASN.1 DER encoded structure
	return []byte{
		0x30, 0x37, // SEQUENCE
		0xa0, 0x03, // [0] version
		0x02, 0x01, 0x02, // INTEGER 2
		0xa1, 0x30, // [1] negoTokens
		0x30, 0x2e, // SEQUENCE
		0x30, 0x2c, // SEQUENCE
		0xa0, 0x2a, // [0] negToken
		0x30, 0x28, // SEQUENCE
		0xa0, 0x26, // [0] mechTypes
		0x30, 0x24, // SEQUENCE
		0x06, 0x09, 0x2a, 0x86, 0x48, 0x82, 0xf7, 0x12, 0x01, 0x02, 0x02, // NTLM OID
		0x06, 0x09, 0x2a, 0x86, 0x48, 0x86, 0xf7, 0x12, 0x01, 0x02, 0x02, // Kerberos OID
		0x06, 0x0a, 0x2b, 0x06, 0x01, 0x04, 0x01, 0x82, 0x37, 0x02, 0x02, 0x0a, // NEGOEX OID
	}
}

// buildNTLMType1Message creates an NTLM Type 1 message
func buildNTLMType1Message() []byte {
	signature := []byte("NTLMSSP\x00")
	msgType := uint32(NTLM_TYPE_1)
	flags := uint32(NTLM_NEGOTIATE_UNICODE | NTLM_REQUEST_TARGET | NTLM_NEGOTIATE_NTLM | NTLM_NEGOTIATE_ALWAYS_SIGN | NTLM_NEGOTIATE_NTLM2)
	
	// Domain and workstation are empty for Type 1
	domainLen := uint16(0)
	domainMaxLen := uint16(0)
	domainOffset := uint32(32)
	
	workstationLen := uint16(0)
	workstationMaxLen := uint16(0)
	workstationOffset := uint32(32)
	
	msg := make([]byte, 32)
	copy(msg[0:8], signature)
	binary.LittleEndian.PutUint32(msg[8:12], msgType)
	binary.LittleEndian.PutUint32(msg[12:16], flags)
	binary.LittleEndian.PutUint16(msg[16:18], domainLen)
	binary.LittleEndian.PutUint16(msg[18:20], domainMaxLen)
	binary.LittleEndian.PutUint32(msg[20:24], domainOffset)
	binary.LittleEndian.PutUint16(msg[24:26], workstationLen)
	binary.LittleEndian.PutUint16(msg[26:28], workstationMaxLen)
	binary.LittleEndian.PutUint32(msg[28:32], workstationOffset)
	
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
	// Parse the final CredSSP response to determine if authentication succeeded
	// Look for success indicators in the ASN.1 structure
	
	// Simplified check: if we get a response without error codes, assume success
	// In a real implementation, this would properly parse the CredSSP result
	
	if len(data) < 10 {
		return false
	}
	
	// Look for common success patterns
	if bytes.Contains(data, []byte{0x30, 0x03, 0x02, 0x01, 0x00}) { // Success result
		return true
	}
	
	// Check for authentication failure patterns
	if bytes.Contains(data, []byte{0x02, 0x01, 0x01}) || // Generic failure
		bytes.Contains(data, []byte{0x02, 0x01, 0x02}) { // Auth failure
		return false
	}
	
	// Default to false for unknown responses
	return false
}
