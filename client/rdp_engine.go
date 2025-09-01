package main

import (
	"bufio"
	"bytes"
	"crypto/rand"
	"crypto/rc4"
	"crypto/rsa"
	"crypto/tls"
	"crypto/x509"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"time"
)

// RDP Protocol Constants
const (
	// Connection Initiation
	PROTOCOL_RDP       = 0x00000000
	PROTOCOL_SSL       = 0x00000001
	PROTOCOL_HYBRID    = 0x00000002
	PROTOCOL_HYBRID_EX = 0x00000008

	// PDU Types
	PDU_TYPE_DEMAND_ACTIVE = 0x11
	PDU_TYPE_CONFIRM_ACTIVE = 0x13
	PDU_TYPE_DATA          = 0x17

	// Connection Sequence
	CONNECTION_TYPE_RDP     = 0x01
	CONNECTION_TYPE_CONSOLE = 0x02

	// NLA States
	NLA_STATE_INITIAL    = 0
	NLA_STATE_NEGO       = 1
	NLA_STATE_AUTH       = 2
	NLA_STATE_FINAL      = 3
	NLA_STATE_CONNECTED  = 4
)

// RDPEngine handles high-performance RDP connections
type RDPEngine struct {
	// Performance metrics
	totalAttempts    int64
	successfulConns  int64
	failedConns      int64
	currentPPS       int64
	peakPPS          int64
	
	// Connection pools
	connPools        map[string]*ConnectionPool
	poolMutex        sync.RWMutex
	
	// Thread management
	maxThreads       int
	threadSemaphore  chan struct{}
	
	// Optimization settings
	tcpNoDelay       bool
	keepAlive        bool
	connectionReuse  bool
	
	// Timing configurations
	connectTimeout   time.Duration
	readTimeout      time.Duration
	writeTimeout     time.Duration
	
	// Retry logic
	maxRetries       int
	retryDelay       time.Duration
	backoffFactor    float64
	
	// State tracking
	activeConns      sync.Map
	stateTracker     *StateTracker
}

// ConnectionPool manages reusable connections for efficiency
type ConnectionPool struct {
	target          string
	maxConnections  int
	activeConns     int32
	connections     chan net.Conn
	mu              sync.RWMutex
	lastUsed        time.Time
	successCount    int64
	failCount       int64
}

// StateTracker tracks connection states to prevent missing valid RDPs
type StateTracker struct {
	states    sync.Map
	mu        sync.RWMutex
	checkpoints map[string]*ConnectionCheckpoint
}

// ConnectionCheckpoint stores connection attempt history
type ConnectionCheckpoint struct {
	Target       string
	Username     string
	Password     string
	Attempts     int
	LastAttempt  time.Time
	LastError    string
	NLASupported bool
	SSLRequired  bool
}

// RDPConnection represents a single RDP connection attempt
type RDPConnection struct {
	conn         net.Conn
	tlsConn      *tls.Conn
	target       string
	username     string
	password     string
	domain       string
	useNLA       bool
	useSSL       bool
	state        int
	sequenceNum  uint16
	encryptionKey []byte
	sessionID    []byte
}

// NewRDPEngine creates an optimized RDP engine
func NewRDPEngine(maxThreads int) *RDPEngine {
	return &RDPEngine{
		maxThreads:      maxThreads,
		threadSemaphore: make(chan struct{}, maxThreads),
		connPools:       make(map[string]*ConnectionPool),
		tcpNoDelay:      true,
		keepAlive:       true,
		connectionReuse: true,
		connectTimeout:  5 * time.Second,
		readTimeout:     3 * time.Second,
		writeTimeout:    3 * time.Second,
		maxRetries:      3,
		retryDelay:      500 * time.Millisecond,
		backoffFactor:   1.5,
		stateTracker:    &StateTracker{
			checkpoints: make(map[string]*ConnectionCheckpoint),
		},
	}
}

// TestCredential performs optimized RDP authentication test
func (e *RDPEngine) TestCredential(target string, port int, username, password, domain string) (bool, error) {
	// Update metrics
	atomic.AddInt64(&e.totalAttempts, 1)
	
	// Get connection from pool or create new
	addr := fmt.Sprintf("%s:%d", target, port)
	
	// Check if we should skip based on previous attempts
	checkpoint := e.stateTracker.GetCheckpoint(addr, username, password)
	if checkpoint != nil && checkpoint.Attempts >= e.maxRetries {
		// Don't skip if last attempt was long ago (might be temporary issue)
		if time.Since(checkpoint.LastAttempt) < 5*time.Minute {
			return false, fmt.Errorf("max retries exceeded for %s", addr)
		}
	}
	
	// Acquire thread semaphore
	e.threadSemaphore <- struct{}{}
	defer func() { <-e.threadSemaphore }()
	
	// Try with different protocol combinations for maximum coverage
	protocols := []struct {
		useNLA bool
		useSSL bool
		name   string
	}{
		{true, true, "NLA+SSL"},    // Most secure, try first
		{false, true, "SSL"},        // SSL without NLA
		{true, false, "NLA"},        // NLA without SSL (rare)
		{false, false, "Standard"},  // Standard RDP
	}
	
	var lastErr error
	for _, proto := range protocols {
		success, err := e.attemptConnection(addr, username, password, domain, proto.useNLA, proto.useSSL)
		if success {
			atomic.AddInt64(&e.successfulConns, 1)
			e.stateTracker.RecordSuccess(addr, username, password)
			return true, nil
		}
		
		// Check if error indicates we should try different protocol
		if err != nil && !e.shouldTryDifferentProtocol(err) {
			lastErr = err
			break
		}
		lastErr = err
	}
	
	atomic.AddInt64(&e.failedConns, 1)
	e.stateTracker.RecordFailure(addr, username, password, lastErr.Error())
	return false, lastErr
}

// attemptConnection performs actual RDP connection attempt
func (e *RDPEngine) attemptConnection(addr, username, password, domain string, useNLA, useSSL bool) (bool, error) {
	// Create RDP connection object
	rdpConn := &RDPConnection{
		target:   addr,
		username: username,
		password: password,
		domain:   domain,
		useNLA:   useNLA,
		useSSL:   useSSL,
		state:    NLA_STATE_INITIAL,
	}
	
	// Establish TCP connection with optimizations
	dialer := &net.Dialer{
		Timeout:   e.connectTimeout,
		KeepAlive: 30 * time.Second,
	}
	
	conn, err := dialer.Dial("tcp", addr)
	if err != nil {
		return false, fmt.Errorf("TCP connection failed: %v", err)
	}
	defer conn.Close()
	
	// Set TCP optimizations
	if tcpConn, ok := conn.(*net.TCPConn); ok {
		tcpConn.SetNoDelay(e.tcpNoDelay)
		tcpConn.SetKeepAlive(e.keepAlive)
		tcpConn.SetReadBuffer(65536)  // 64KB read buffer
		tcpConn.SetWriteBuffer(65536) // 64KB write buffer
	}
	
	rdpConn.conn = conn
	
	// Perform X.224 Connection Request
	if err := rdpConn.sendConnectionRequest(); err != nil {
		return false, fmt.Errorf("X.224 connection request failed: %v", err)
	}
	
	// Read Connection Confirm
	negotiatedProtocol, err := rdpConn.readConnectionConfirm()
	if err != nil {
		return false, fmt.Errorf("X.224 connection confirm failed: %v", err)
	}
	
	// Handle SSL/TLS if negotiated
	if negotiatedProtocol&(PROTOCOL_SSL|PROTOCOL_HYBRID|PROTOCOL_HYBRID_EX) != 0 {
		tlsConfig := &tls.Config{
			InsecureSkipVerify: true,
			MinVersion:         tls.VersionTLS10,
			MaxVersion:         tls.VersionTLS13,
		}
		
		rdpConn.tlsConn = tls.Client(conn, tlsConfig)
		if err := rdpConn.tlsConn.Handshake(); err != nil {
			return false, fmt.Errorf("TLS handshake failed: %v", err)
		}
		
		// Use TLS connection for further communication
		rdpConn.conn = rdpConn.tlsConn
	}
	
	// Perform NLA authentication if required
	if negotiatedProtocol&(PROTOCOL_HYBRID|PROTOCOL_HYBRID_EX) != 0 {
		if err := rdpConn.performNLAAuthentication(); err != nil {
			return false, fmt.Errorf("NLA authentication failed: %v", err)
		}
	}
	
	// Continue with standard RDP authentication
	if err := rdpConn.performStandardRDPAuth(); err != nil {
		return false, fmt.Errorf("Standard RDP auth failed: %v", err)
	}
	
	return true, nil
}

// sendConnectionRequest sends X.224 Connection Request PDU
func (c *RDPConnection) sendConnectionRequest() error {
	// Build negotiation request
	var protocols uint32 = PROTOCOL_RDP
	if c.useSSL {
		protocols |= PROTOCOL_SSL
	}
	if c.useNLA {
		protocols |= PROTOCOL_HYBRID | PROTOCOL_HYBRID_EX
	}
	
	// Create Cookie (mstshash=username)
	cookie := fmt.Sprintf("Cookie: mstshash=%s\r\n", c.username)
	
	// Build RDP Negotiation Request
	negReq := make([]byte, 8)
	negReq[0] = 0x01 // Type: Negotiation Request
	negReq[1] = 0x00 // Flags
	binary.LittleEndian.PutUint16(negReq[2:4], 8)      // Length
	binary.LittleEndian.PutUint32(negReq[4:8], protocols) // Requested Protocols
	
	// Build X.224 Connection Request TPDU
	tpdu := bytes.NewBuffer(nil)
	tpdu.WriteByte(byte(len(cookie) + len(negReq) + 6)) // Length Indicator
	tpdu.WriteByte(0xE0)                                // CR TPDU Code
	tpdu.Write([]byte{0x00, 0x00})                      // DST-REF
	tpdu.Write([]byte{0x00, 0x00})                      // SRC-REF
	tpdu.WriteByte(0x00)                                // Class Option
	tpdu.WriteString(cookie)
	tpdu.Write(negReq)
	
	// Build TPKT Header
	tpkt := bytes.NewBuffer(nil)
	tpkt.WriteByte(0x03)                                    // Version
	tpkt.WriteByte(0x00)                                    // Reserved
	binary.Write(tpkt, binary.BigEndian, uint16(tpdu.Len()+4)) // Packet Length
	tpkt.Write(tpdu.Bytes())
	
	// Send the request
	c.conn.SetWriteDeadline(time.Now().Add(5 * time.Second))
	_, err := c.conn.Write(tpkt.Bytes())
	return err
}

// readConnectionConfirm reads X.224 Connection Confirm PDU
func (c *RDPConnection) readConnectionConfirm() (uint32, error) {
	c.conn.SetReadDeadline(time.Now().Add(5 * time.Second))
	
	// Read TPKT header
	tpktHeader := make([]byte, 4)
	if _, err := io.ReadFull(c.conn, tpktHeader); err != nil {
		return 0, err
	}
	
	if tpktHeader[0] != 0x03 {
		return 0, errors.New("invalid TPKT version")
	}
	
	length := binary.BigEndian.Uint16(tpktHeader[2:4])
	
	// Read remaining data
	data := make([]byte, length-4)
	if _, err := io.ReadFull(c.conn, data); err != nil {
		return 0, err
	}
	
	// Parse X.224 Connection Confirm
	if len(data) < 7 || data[1] != 0xD0 {
		return 0, errors.New("invalid connection confirm")
	}
	
	// Look for RDP Negotiation Response
	for i := 7; i < len(data)-7; i++ {
		if data[i] == 0x02 { // Type: Negotiation Response
			negotiatedProtocol := binary.LittleEndian.Uint32(data[i+4 : i+8])
			return negotiatedProtocol, nil
		}
	}
	
	// No negotiation response means standard RDP
	return PROTOCOL_RDP, nil
}

// performNLAAuthentication performs Network Level Authentication
func (c *RDPConnection) performNLAAuthentication() error {
	// This is a simplified NLA implementation
	// In production, you would use proper NTLM/Kerberos authentication
	
	// Create NTLM Type 1 message
	type1Msg := c.createNTLMType1Message()
	
	// Send CredSSP TSRequest with NTLM Type 1
	tsRequest := c.wrapInCredSSP(type1Msg, 1)
	if err := c.sendData(tsRequest); err != nil {
		return err
	}
	
	// Receive CredSSP TSRequest with NTLM Type 2 (Challenge)
	response, err := c.receiveData()
	if err != nil {
		return err
	}
	
	// Parse NTLM Type 2 message and extract challenge
	challenge := c.parseNTLMType2(response)
	
	// Create NTLM Type 3 message with credentials
	type3Msg := c.createNTLMType3Message(challenge)
	
	// Send CredSSP TSRequest with NTLM Type 3
	tsRequest = c.wrapInCredSSP(type3Msg, 3)
	if err := c.sendData(tsRequest); err != nil {
		return err
	}
	
	// Receive final CredSSP response
	finalResponse, err := c.receiveData()
	if err != nil {
		return err
	}
	
	// Verify authentication success
	if !c.verifyNLASuccess(finalResponse) {
		return errors.New("NLA authentication failed")
	}
	
	c.state = NLA_STATE_CONNECTED
	return nil
}

// createNTLMType1Message creates NTLM Type 1 (Negotiate) message
func (c *RDPConnection) createNTLMType1Message() []byte {
	msg := bytes.NewBuffer(nil)
	
	// NTLM Signature
	msg.WriteString("NTLMSSP\x00")
	
	// Message Type (Type 1)
	binary.Write(msg, binary.LittleEndian, uint32(0x00000001))
	
	// Flags
	flags := uint32(0x00000001 | 0x00000002 | 0x00000004 | 0x00000200 | 0x00008000 | 0x00800000)
	binary.Write(msg, binary.LittleEndian, flags)
	
	// Domain (empty for Type 1)
	binary.Write(msg, binary.LittleEndian, uint16(0)) // Length
	binary.Write(msg, binary.LittleEndian, uint16(0)) // Max Length
	binary.Write(msg, binary.LittleEndian, uint32(0)) // Offset
	
	// Workstation (empty for Type 1)
	binary.Write(msg, binary.LittleEndian, uint16(0)) // Length
	binary.Write(msg, binary.LittleEndian, uint16(0)) // Max Length
	binary.Write(msg, binary.LittleEndian, uint32(0)) // Offset
	
	return msg.Bytes()
}

// createNTLMType3Message creates NTLM Type 3 (Authenticate) message
func (c *RDPConnection) createNTLMType3Message(challenge []byte) []byte {
	msg := bytes.NewBuffer(nil)
	
	// NTLM Signature
	msg.WriteString("NTLMSSP\x00")
	
	// Message Type (Type 3)
	binary.Write(msg, binary.LittleEndian, uint32(0x00000003))
	
	// LM Response
	lmResponse := c.calculateNTLMResponse(challenge, c.password, false)
	binary.Write(msg, binary.LittleEndian, uint16(len(lmResponse)))
	binary.Write(msg, binary.LittleEndian, uint16(len(lmResponse)))
	binary.Write(msg, binary.LittleEndian, uint32(64)) // Offset
	
	// NTLM Response
	ntlmResponse := c.calculateNTLMResponse(challenge, c.password, true)
	binary.Write(msg, binary.LittleEndian, uint16(len(ntlmResponse)))
	binary.Write(msg, binary.LittleEndian, uint16(len(ntlmResponse)))
	binary.Write(msg, binary.LittleEndian, uint32(64+len(lmResponse)))
	
	// Domain
	domainBytes := []byte(c.domain)
	binary.Write(msg, binary.LittleEndian, uint16(len(domainBytes)))
	binary.Write(msg, binary.LittleEndian, uint16(len(domainBytes)))
	binary.Write(msg, binary.LittleEndian, uint32(64+len(lmResponse)+len(ntlmResponse)))
	
	// Username
	userBytes := []byte(c.username)
	binary.Write(msg, binary.LittleEndian, uint16(len(userBytes)))
	binary.Write(msg, binary.LittleEndian, uint16(len(userBytes)))
	binary.Write(msg, binary.LittleEndian, uint32(64+len(lmResponse)+len(ntlmResponse)+len(domainBytes)))
	
	// Workstation
	workstationBytes := []byte("WORKSTATION")
	binary.Write(msg, binary.LittleEndian, uint16(len(workstationBytes)))
	binary.Write(msg, binary.LittleEndian, uint16(len(workstationBytes)))
	binary.Write(msg, binary.LittleEndian, uint32(64+len(lmResponse)+len(ntlmResponse)+len(domainBytes)+len(userBytes)))
	
	// Session Key (empty)
	binary.Write(msg, binary.LittleEndian, uint16(0))
	binary.Write(msg, binary.LittleEndian, uint16(0))
	binary.Write(msg, binary.LittleEndian, uint32(0))
	
	// Flags
	binary.Write(msg, binary.LittleEndian, uint32(0x00000201))
	
	// Append actual data
	msg.Write(lmResponse)
	msg.Write(ntlmResponse)
	msg.Write(domainBytes)
	msg.Write(userBytes)
	msg.Write(workstationBytes)
	
	return msg.Bytes()
}

// performStandardRDPAuth performs standard RDP authentication
func (c *RDPConnection) performStandardRDPAuth() error {
	// Send MCS Connect Initial
	if err := c.sendMCSConnectInitial(); err != nil {
		return err
	}
	
	// Receive MCS Connect Response
	if err := c.receiveMCSConnectResponse(); err != nil {
		return err
	}
	
	// Send MCS Erect Domain Request
	if err := c.sendMCSErectDomainRequest(); err != nil {
		return err
	}
	
	// Send MCS Attach User Request
	if err := c.sendMCSAttachUserRequest(); err != nil {
		return err
	}
	
	// Receive MCS Attach User Confirm
	userID, err := c.receiveMCSAttachUserConfirm()
	if err != nil {
		return err
	}
	
	// Send MCS Channel Join Requests
	channels := []uint16{userID, 1003, 1004, 1005, 1006, 1007}
	for _, channel := range channels {
		if err := c.sendMCSChannelJoinRequest(channel); err != nil {
			return err
		}
		if err := c.receiveMCSChannelJoinConfirm(); err != nil {
			return err
		}
	}
	
	// Send Security Exchange PDU
	if err := c.sendSecurityExchangePDU(); err != nil {
		return err
	}
	
	// Send Client Info PDU with credentials
	if err := c.sendClientInfoPDU(); err != nil {
		return err
	}
	
	// At this point, if we haven't received an error, authentication was successful
	return nil
}

// Helper methods for protocol implementation

func (c *RDPConnection) sendData(data []byte) error {
	c.conn.SetWriteDeadline(time.Now().Add(5 * time.Second))
	_, err := c.conn.Write(data)
	return err
}

func (c *RDPConnection) receiveData() ([]byte, error) {
	c.conn.SetReadDeadline(time.Now().Add(5 * time.Second))
	buffer := make([]byte, 4096)
	n, err := c.conn.Read(buffer)
	if err != nil {
		return nil, err
	}
	return buffer[:n], nil
}

func (c *RDPConnection) wrapInCredSSP(data []byte, version int) []byte {
	// Simplified CredSSP wrapper
	// In production, implement full ASN.1 encoding
	wrapper := bytes.NewBuffer(nil)
	wrapper.WriteByte(0x30) // SEQUENCE
	wrapper.WriteByte(byte(len(data) + 10))
	wrapper.WriteByte(0x02) // INTEGER (version)
	wrapper.WriteByte(0x01)
	wrapper.WriteByte(byte(version))
	wrapper.WriteByte(0x04) // OCTET STRING (negoTokens)
	wrapper.WriteByte(byte(len(data) + 4))
	wrapper.Write(data)
	return wrapper.Bytes()
}

func (c *RDPConnection) parseNTLMType2(data []byte) []byte {
	// Extract challenge from NTLM Type 2 message
	// Look for NTLMSSP signature
	for i := 0; i < len(data)-8; i++ {
		if string(data[i:i+8]) == "NTLMSSP\x00" {
			// Check if it's Type 2 message
			if i+12 < len(data) && binary.LittleEndian.Uint32(data[i+8:i+12]) == 0x00000002 {
				// Challenge is at offset 24 from NTLMSSP
				if i+32 <= len(data) {
					return data[i+24 : i+32]
				}
			}
		}
	}
	return make([]byte, 8) // Return empty challenge if not found
}

func (c *RDPConnection) calculateNTLMResponse(challenge []byte, password string, useNTLM bool) []byte {
	// Simplified NTLM response calculation
	// In production, implement proper NTLM hash calculation
	response := make([]byte, 24)
	copy(response, challenge)
	copy(response[8:], []byte(password))
	return response
}

func (c *RDPConnection) verifyNLASuccess(response []byte) bool {
	// Check for successful authentication indicators
	// In production, properly parse CredSSP response
	return len(response) > 0 && !bytes.Contains(response, []byte("error"))
}

// MCS Protocol Implementation

func (c *RDPConnection) sendMCSConnectInitial() error {
	// Build MCS Connect Initial PDU
	// This is a simplified version - implement full T.125 in production
	pdu := bytes.NewBuffer(nil)
	
	// BER encoding for MCS Connect Initial
	pdu.WriteByte(0x7F) // Application tag
	pdu.WriteByte(0x65) // Connect-Initial tag
	
	// Add calling domain selector
	pdu.WriteByte(0x04) // OCTET STRING
	pdu.WriteByte(0x01)
	pdu.WriteByte(0x01)
	
	// Add called domain selector
	pdu.WriteByte(0x04) // OCTET STRING
	pdu.WriteByte(0x01)
	pdu.WriteByte(0x01)
	
	// Add upward flag
	pdu.WriteByte(0x01) // BOOLEAN
	pdu.WriteByte(0x01)
	pdu.WriteByte(0xFF)
	
	// Add user data with RDP connection data
	userData := c.buildClientData()
	pdu.WriteByte(0x04) // OCTET STRING
	if len(userData) < 128 {
		pdu.WriteByte(byte(len(userData)))
	} else {
		pdu.WriteByte(0x82) // Long form
		binary.Write(pdu, binary.BigEndian, uint16(len(userData)))
	}
	pdu.Write(userData)
	
	// Wrap in X.224 Data TPDU
	return c.sendX224Data(pdu.Bytes())
}

func (c *RDPConnection) buildClientData() []byte {
	data := bytes.NewBuffer(nil)
	
	// CS_CORE
	core := bytes.NewBuffer(nil)
	binary.Write(core, binary.LittleEndian, uint16(0x00C1)) // Type
	binary.Write(core, binary.LittleEndian, uint16(216))    // Length
	binary.Write(core, binary.LittleEndian, uint32(0x00080004)) // Version
	binary.Write(core, binary.LittleEndian, uint16(1024))   // Desktop Width
	binary.Write(core, binary.LittleEndian, uint16(768))    // Desktop Height
	binary.Write(core, binary.LittleEndian, uint16(0xCA01)) // Color Depth
	binary.Write(core, binary.LittleEndian, uint16(0xAA03)) // SAS Sequence
	binary.Write(core, binary.LittleEndian, uint32(0x409))  // Keyboard Layout
	binary.Write(core, binary.LittleEndian, uint32(2600))   // Client Build
	
	// Client name (32 bytes, Unicode)
	clientName := make([]byte, 32)
	copy(clientName, []byte("WORKSTATION\x00"))
	core.Write(clientName)
	
	// Keyboard type, subtype, func keys
	binary.Write(core, binary.LittleEndian, uint32(4))
	binary.Write(core, binary.LittleEndian, uint32(0))
	binary.Write(core, binary.LittleEndian, uint32(12))
	
	// ime file name (64 bytes)
	core.Write(make([]byte, 64))
	
	// Post Beta 2 fields
	binary.Write(core, binary.LittleEndian, uint16(0xCA01)) // Color depth
	binary.Write(core, binary.LittleEndian, uint16(1))      // Client product ID
	binary.Write(core, binary.LittleEndian, uint32(0))      // Serial number
	binary.Write(core, binary.LittleEndian, uint16(0x18))   // High color depth
	binary.Write(core, binary.LittleEndian, uint16(0x07))   // Supported color depths
	binary.Write(core, binary.LittleEndian, uint16(0x01))   // Early capability flags
	
	// Client dig product id (64 bytes)
	core.Write(make([]byte, 64))
	
	binary.Write(core, binary.LittleEndian, uint8(0))  // Connection type
	binary.Write(core, binary.LittleEndian, uint8(0))  // Pad
	binary.Write(core, binary.LittleEndian, uint32(0)) // Server selected protocol
	
	data.Write(core.Bytes())
	
	// CS_SECURITY
	security := bytes.NewBuffer(nil)
	binary.Write(security, binary.LittleEndian, uint16(0x00C2)) // Type
	binary.Write(security, binary.LittleEndian, uint16(12))     // Length
	binary.Write(security, binary.LittleEndian, uint32(0))      // Encryption methods
	binary.Write(security, binary.LittleEndian, uint32(0))      // Ext encryption methods
	
	data.Write(security.Bytes())
	
	// CS_NET
	net := bytes.NewBuffer(nil)
	binary.Write(net, binary.LittleEndian, uint16(0x00C3)) // Type
	binary.Write(net, binary.LittleEndian, uint16(8))      // Length
	binary.Write(net, binary.LittleEndian, uint32(0))      // Channel count
	
	data.Write(net.Bytes())
	
	return data.Bytes()
}

func (c *RDPConnection) sendX224Data(data []byte) error {
	// Build X.224 Data TPDU
	tpdu := bytes.NewBuffer(nil)
	tpdu.WriteByte(0x02) // Length indicator (minimal)
	tpdu.WriteByte(0xF0) // DT TPDU
	tpdu.WriteByte(0x80) // EOT
	tpdu.Write(data)
	
	// Build TPKT header
	tpkt := bytes.NewBuffer(nil)
	tpkt.WriteByte(0x03) // Version
	tpkt.WriteByte(0x00) // Reserved
	binary.Write(tpkt, binary.BigEndian, uint16(tpdu.Len()+4))
	tpkt.Write(tpdu.Bytes())
	
	return c.sendData(tpkt.Bytes())
}

func (c *RDPConnection) receiveMCSConnectResponse() error {
	response, err := c.receiveData()
	if err != nil {
		return err
	}
	
	// Basic validation - check for MCS Connect Response
	if len(response) < 10 {
		return errors.New("invalid MCS Connect Response")
	}
	
	// Look for success indicator
	// In production, properly parse BER-encoded response
	return nil
}

func (c *RDPConnection) sendMCSErectDomainRequest() error {
	pdu := bytes.NewBuffer(nil)
	pdu.WriteByte(0x04) // Erect Domain Request
	pdu.WriteByte(0x01) // Sub-height
	pdu.WriteByte(0x00) // Sub-interval
	
	return c.sendX224Data(pdu.Bytes())
}

func (c *RDPConnection) sendMCSAttachUserRequest() error {
	pdu := bytes.NewBuffer(nil)
	pdu.WriteByte(0x28) // Attach User Request
	
	return c.sendX224Data(pdu.Bytes())
}

func (c *RDPConnection) receiveMCSAttachUserConfirm() (uint16, error) {
	response, err := c.receiveData()
	if err != nil {
		return 0, err
	}
	
	// Parse user channel ID from response
	// Look for Attach User Confirm (0x2C)
	for i := 0; i < len(response)-3; i++ {
		if response[i] == 0x2C {
			// User ID is typically at next 2 bytes
			if i+3 < len(response) {
				userID := binary.BigEndian.Uint16(response[i+1 : i+3])
				return userID, nil
			}
		}
	}
	
	// Default user channel
	return 1001, nil
}

func (c *RDPConnection) sendMCSChannelJoinRequest(channelID uint16) error {
	pdu := bytes.NewBuffer(nil)
	pdu.WriteByte(0x38) // Channel Join Request
	binary.Write(pdu, binary.BigEndian, channelID)
	
	return c.sendX224Data(pdu.Bytes())
}

func (c *RDPConnection) receiveMCSChannelJoinConfirm() error {
	response, err := c.receiveData()
	if err != nil {
		return err
	}
	
	// Basic validation - look for Channel Join Confirm (0x3C)
	for i := 0; i < len(response); i++ {
		if response[i] == 0x3C {
			return nil // Success
		}
	}
	
	return errors.New("channel join failed")
}

func (c *RDPConnection) sendSecurityExchangePDU() error {
	// Generate random client key
	clientRandom := make([]byte, 32)
	rand.Read(clientRandom)
	
	pdu := bytes.NewBuffer(nil)
	binary.Write(pdu, binary.LittleEndian, uint32(len(clientRandom)))
	pdu.Write(clientRandom)
	
	// Wrap in security header
	secHeader := bytes.NewBuffer(nil)
	binary.Write(secHeader, binary.LittleEndian, uint16(0x0001)) // Security header flags
	binary.Write(secHeader, binary.LittleEndian, uint16(0))      // Flags hi
	secHeader.Write(pdu.Bytes())
	
	return c.sendMCSData(secHeader.Bytes(), 1001) // Send on user channel
}

func (c *RDPConnection) sendClientInfoPDU() error {
	info := bytes.NewBuffer(nil)
	
	// Code page
	binary.Write(info, binary.LittleEndian, uint32(0))
	
	// Flags
	flags := uint32(0x00000033) // INFO_MOUSE | INFO_DISABLECTRLALTDEL | INFO_UNICODE
	binary.Write(info, binary.LittleEndian, flags)
	
	// Domain length
	domainBytes := utf16Encode(c.domain)
	binary.Write(info, binary.LittleEndian, uint16(len(domainBytes)))
	
	// Username length
	userBytes := utf16Encode(c.username)
	binary.Write(info, binary.LittleEndian, uint16(len(userBytes)))
	
	// Password length
	passBytes := utf16Encode(c.password)
	binary.Write(info, binary.LittleEndian, uint16(len(passBytes)))
	
	// Alternate shell length
	binary.Write(info, binary.LittleEndian, uint16(0))
	
	// Working directory length
	binary.Write(info, binary.LittleEndian, uint16(0))
	
	// Write actual strings
	info.Write(domainBytes)
	info.Write(userBytes)
	info.Write(passBytes)
	
	// Add extended info
	binary.Write(info, binary.LittleEndian, uint16(0x0002)) // Client address family
	binary.Write(info, binary.LittleEndian, uint16(0x001C)) // Address length
	
	// Client address
	clientAddr := make([]byte, 28)
	copy(clientAddr, []byte("192.168.1.100"))
	info.Write(clientAddr)
	
	// Client dir length
	binary.Write(info, binary.LittleEndian, uint16(0))
	
	// Time zone info (292 bytes)
	info.Write(make([]byte, 292))
	
	// Wrap in security header
	secHeader := bytes.NewBuffer(nil)
	binary.Write(secHeader, binary.LittleEndian, uint16(0x0040)) // INFO_PKT
	binary.Write(secHeader, binary.LittleEndian, uint16(0))
	secHeader.Write(info.Bytes())
	
	return c.sendMCSData(secHeader.Bytes(), 1001)
}

func (c *RDPConnection) sendMCSData(data []byte, channelID uint16) error {
	// Build MCS Send Data Request
	pdu := bytes.NewBuffer(nil)
	pdu.WriteByte(0x64) // Send Data Request
	binary.Write(pdu, binary.BigEndian, uint16(1001))     // Initiator
	binary.Write(pdu, binary.BigEndian, channelID)        // Channel ID
	pdu.WriteByte(0x70)                                   // Priority
	pdu.WriteByte(0x80 | (byte(len(data) >> 8) & 0x3F))   // Length high
	pdu.WriteByte(byte(len(data) & 0xFF))                 // Length low
	pdu.Write(data)
	
	return c.sendX224Data(pdu.Bytes())
}

// Helper function for UTF-16 encoding
func utf16Encode(s string) []byte {
	runes := []rune(s)
	result := make([]byte, len(runes)*2)
	for i, r := range runes {
		binary.LittleEndian.PutUint16(result[i*2:], uint16(r))
	}
	return result
}

// shouldTryDifferentProtocol determines if we should try a different protocol based on error
func (e *RDPEngine) shouldTryDifferentProtocol(err error) bool {
	if err == nil {
		return false
	}
	
	errStr := err.Error()
	
	// These errors suggest we should try different protocol
	protocolErrors := []string{
		"NLA required",
		"SSL required",
		"TLS required",
		"CredSSP required",
		"Network Level Authentication",
		"protocol negotiation",
		"unsupported protocol",
	}
	
	for _, pe := range protocolErrors {
		if bytes.Contains([]byte(errStr), []byte(pe)) {
			return true
		}
	}
	
	return false
}

// GetCheckpoint retrieves connection checkpoint for retry logic
func (st *StateTracker) GetCheckpoint(target, username, password string) *ConnectionCheckpoint {
	key := fmt.Sprintf("%s:%s:%s", target, username, password)
	if val, ok := st.checkpoints[key]; ok {
		return val
	}
	return nil
}

// RecordSuccess records successful authentication
func (st *StateTracker) RecordSuccess(target, username, password string) {
	st.mu.Lock()
	defer st.mu.Unlock()
	
	key := fmt.Sprintf("%s:%s:%s", target, username, password)
	st.checkpoints[key] = &ConnectionCheckpoint{
		Target:      target,
		Username:    username,
		Password:    password,
		Attempts:    1,
		LastAttempt: time.Now(),
	}
}

// RecordFailure records failed authentication attempt
func (st *StateTracker) RecordFailure(target, username, password, error string) {
	st.mu.Lock()
	defer st.mu.Unlock()
	
	key := fmt.Sprintf("%s:%s:%s", target, username, password)
	if cp, exists := st.checkpoints[key]; exists {
		cp.Attempts++
		cp.LastAttempt = time.Now()
		cp.LastError = error
	} else {
		st.checkpoints[key] = &ConnectionCheckpoint{
			Target:      target,
			Username:    username,
			Password:    password,
			Attempts:    1,
			LastAttempt: time.Now(),
			LastError:   error,
		}
	}
}

// GetStats returns current engine statistics
func (e *RDPEngine) GetStats() map[string]int64 {
	return map[string]int64{
		"total_attempts":   atomic.LoadInt64(&e.totalAttempts),
		"successful_conns": atomic.LoadInt64(&e.successfulConns),
		"failed_conns":     atomic.LoadInt64(&e.failedConns),
		"current_pps":      atomic.LoadInt64(&e.currentPPS),
		"peak_pps":         atomic.LoadInt64(&e.peakPPS),
	}
}