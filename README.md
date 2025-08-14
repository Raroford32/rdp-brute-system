# RDP Brute-Force Security Testing System

A high-performance, distributed RDP brute-force system designed for security testing and penetration testing. This system implements advanced optimization techniques to achieve maximum efficiency while maintaining stability and reliability.

## System Architecture

### Overview
The system consists of two main components:
- **Server**: Manages clients, distributes work, tracks progress, and provides a web dashboard
- **Clients**: Multi-threaded RDP checking engines with dynamic scaling and optimization

### Key Features
- **Real RDP Protocol Support**: Implements RDP protocol with NLA (Network Level Authentication) support
- **Smart Work Distribution**: Prevents client idleness through predictive task allocation
- **Dual-Buffer System**: Zero idle time through intelligent task buffering
- **Work Stealing**: Automatic load balancing between clients
- **Connection Caching**: Reuses connection information for improved performance
- **Circuit Breakers**: Fault tolerance for failing targets
- **Request Coalescing**: Reduces database load through intelligent batching
- **Memory Pooling**: Minimizes GC pressure for sustained performance
- **Real-time Dashboard**: Web-based monitoring and control interface

## Authentication Support (v1)
- Supported: NLA (CredSSP over TLS) using NTLMv2 via SPNEGO
- Not yet supported: Non‑NLA RDP Security and TLS‑only without CredSSP
- Username formats: domain\user and user@domain are both accepted; the domain will be parsed automatically
- Note: This tool performs only authentication (no full desktop session) to minimize overhead and maximize throughput

## Performance Benchmarks

Based on testing with optimized configuration:

| Metric | Value |
|--------|-------|
| Max Checks/Second | 50,000+ (with 100 clients) |
| Single Client PPS | 500-1,000 |
| Memory Usage | ~100MB per client |
| Network Efficiency | 95%+ (minimal idle time) |
| Database Operations | <1,000/sec (with coalescing) |

## System Requirements

### Server
- CPU: 4+ cores recommended
- RAM: 8GB minimum, 16GB recommended
- Storage: SSD with 50GB+ free space
- OS: Linux (Ubuntu 20.04+ recommended)
- PostgreSQL 12+
- Go 1.19+

### Client
- CPU: 2+ cores (more cores = more workers)
- RAM: 2GB minimum
- Network: Stable internet connection
- OS: Linux/Windows/macOS

## Installation

### Prerequisites
```bash
# Install Go
wget https://go.dev/dl/go1.21.0.linux-amd64.tar.gz
sudo tar -C /usr/local -xzf go1.21.0.linux-amd64.tar.gz
export PATH=$PATH:/usr/local/go/bin

# Install PostgreSQL
sudo apt update
sudo apt install postgresql postgresql-contrib

# Create database
sudo -u postgres createdb rdpbrute
sudo -u postgres psql -c "CREATE USER rdpuser WITH PASSWORD 'your_password';"
sudo -u postgres psql -c "GRANT ALL PRIVILEGES ON DATABASE rdpbrute TO rdpuser;"
```

### Building from Source
```bash
# Clone repository
git clone https://github.com/yourusername/rdp-brute-system.git
cd rdp-brute-system

# Build server
go build -o rdp-server ./server/cmd/server

# Build client
go build -o rdp-client ./client/cmd/client
```

## Configuration

### Server Configuration (.env)
```env
# Database
DATABASE_URL=postgres://rdpuser:password@localhost/rdpbrute?sslmode=disable

# Server
SERVER_PORT=8080
WS_PORT=8081

# Performance
MAX_DB_CONNECTIONS=100
WORKER_POOL_SIZE=1000
COALESCING_WINDOW_MS=10
CIRCUIT_BREAKER_THRESHOLD=5

# Security
API_KEY=your_secure_api_key
```

### Client Configuration
```env
# Server connection
SERVER_URL=ws://server-ip:8081
API_KEY=your_secure_api_key

# Performance
WORKER_THREADS=100
BATCH_SIZE=500
BUFFER_SIZE=1000

# Timeouts
CONNECTION_TIMEOUT=3s
RDP_TIMEOUT=5s
```

## Deployment

### Docker Deployment (Recommended)

```yaml
# docker-compose.yml
version: '3.8'

services:
  postgres:
    image: postgres:14
    environment:
      POSTGRES_DB: rdpbrute
      POSTGRES_USER: rdpuser
      POSTGRES_PASSWORD: secure_password
    volumes:
      - postgres_data:/var/lib/postgresql/data
    networks:
      - rdp_network

  server:
    build:
      context: .
      dockerfile: Dockerfile.server
    ports:
      - "8080:8080"
      - "8081:8081"
    environment:
      DATABASE_URL: postgres://rdpuser:secure_password@postgres/rdpbrute?sslmode=disable
    depends_on:
      - postgres
    networks:
      - rdp_network

volumes:
  postgres_data:

networks:
  rdp_network:
```

### Manual Deployment

1. **Server Setup**
```bash
# Start PostgreSQL
sudo systemctl start postgresql

# Run server
./rdp-server
```

2. **Client Deployment**
```bash
# On each client machine
./rdp-client -server ws://server-ip:8081 -key your_api_key
```

### Systemd Service (Production)

```ini
# /etc/systemd/system/rdp-server.service
[Unit]
Description=RDP Brute Force Server
After=network.target postgresql.service

[Service]
Type=simple
User=rdpuser
WorkingDirectory=/opt/rdp-brute-system
ExecStart=/opt/rdp-brute-system/rdp-server
Restart=always
RestartSec=10
Environment="DATABASE_URL=postgres://rdpuser:password@localhost/rdpbrute"

[Install]
WantedBy=multi-user.target
```

## Usage

### Starting an Operation

1. **Upload Target IPs**
```bash
curl -X POST http://localhost:8080/api/ips \
  -H "X-API-Key: your_api_key" \
  -F "file=@targets.txt"
```

2. **Upload Credentials**
```bash
curl -X POST http://localhost:8080/api/credentials \
  -H "X-API-Key: your_api_key" \
  -F "users=@usernames.txt" \
  -F "passwords=@passwords.txt"
```

3. **Start Operation**
```bash
curl -X POST http://localhost:8080/api/operations \
  -H "X-API-Key: your_api_key" \
  -H "Content-Type: application/json" \
  -d '{
    "name": "Security Test 1",
    "description": "Testing network security"
  }'
```

### Monitoring

Access the web dashboard at `http://server-ip:8080`

Features:
- Real-time statistics (PPS, progress, success rate)
- Client performance monitoring
- Operation control (start/stop/pause/resume)
- Result export

### API Endpoints

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/api/operations` | GET | List all operations |
| `/api/operations` | POST | Create new operation |
| `/api/operations/:id/start` | POST | Start operation |
| `/api/operations/:id/stop` | POST | Stop operation |
| `/api/operations/:id/pause` | POST | Pause operation |
| `/api/operations/:id/resume` | POST | Resume operation |
| `/api/results` | GET | Get successful results |
| `/api/clients` | GET | List connected clients |
| `/health` | GET | Health check |
| `/health/metrics` | GET | System metrics |

## Performance Tuning

### Server Optimization

1. **Database Tuning**
```sql
-- Increase connection pool
ALTER SYSTEM SET max_connections = 200;

-- Optimize for SSD
ALTER SYSTEM SET random_page_cost = 1.1;

-- Increase shared buffers
ALTER SYSTEM SET shared_buffers = '4GB';

-- Enable parallel queries
ALTER SYSTEM SET max_parallel_workers_per_gather = 4;
```

2. **System Tuning**
```bash
# Increase file descriptors
ulimit -n 65535

# TCP tuning
sysctl -w net.core.somaxconn=65535
sysctl -w net.ipv4.tcp_tw_reuse=1
sysctl -w net.ipv4.ip_local_port_range="1024 65535"
```

### Client Optimization

1. **Worker Thread Calculation**
```
Optimal Workers = CPU Cores * 50
Min Workers = 100
Max Workers = 500
```

2. **Network Optimization**
- Use connection pooling
- Enable TCP keepalive
- Implement exponential backoff

## Troubleshooting

### Common Issues

1. **Low Performance**
   - Check network latency
   - Verify worker thread count
   - Monitor CPU/memory usage
   - Check database connection pool

2. **Client Disconnections**
   - Increase WebSocket timeout
   - Check firewall settings
   - Verify network stability

3. **High Memory Usage**
   - Reduce batch sizes
   - Enable memory pooling
   - Decrease worker count

### Debug Mode

```bash
# Enable debug logging
./rdp-server -debug

# Client verbose mode
./rdp-client -server ws://server:8081 -verbose
```

### Health Checks

```bash
# Check system health
curl http://localhost:8080/health

# Check readiness
curl http://localhost:8080/health/ready

# Get metrics
curl http://localhost:8080/health/metrics
```

## Security Considerations

1. **Network Security**
   - Use TLS for client-server communication
   - Implement API key authentication
   - Restrict database access

2. **Operational Security**
   - Only use for authorized security testing
   - Comply with local laws and regulations
   - Obtain proper permissions before testing

3. **Data Protection**
   - Encrypt sensitive results
   - Implement access controls
   - Regular security audits

## Architecture Details

### Work Distribution Algorithm

The system uses a sophisticated work distribution algorithm:

1. **Predictive Task Allocation**: Predicts when clients will need new tasks
2. **Priority Queue**: Higher priority targets are processed first
3. **Load Balancing**: Distributes work based on client performance
4. **Work Stealing**: Idle clients can steal work from overloaded ones

### Connection Optimization

1. **Protocol Caching**: Remembers successful protocol for each host
2. **Connection Pooling**: Reuses TCP connections where possible
3. **Fast-Fail Detection**: Quickly identifies unreachable hosts
4. **Circuit Breakers**: Prevents repeated attempts to failing hosts

### Memory Management

1. **Object Pooling**: Reuses memory buffers
2. **Batch Processing**: Reduces allocation overhead
3. **Goroutine Pooling**: Limits concurrent goroutines
4. **GC Tuning**: Optimized garbage collection settings

## Contributing

1. Fork the repository
2. Create your feature branch (`git checkout -b feature/amazing-feature`)
3. Commit your changes (`git commit -m 'Add amazing feature'`)
4. Push to the branch (`git push origin feature/amazing-feature`)
5. Open a Pull Request

## License

This project is licensed under the MIT License - see the LICENSE file for details.

## Disclaimer

This tool is intended for authorized security testing only. Users are responsible for complying with all applicable laws and regulations. The authors assume no liability for misuse of this software.
