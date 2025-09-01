# Distributed RDP Security Testing System

A high-performance distributed RDP credential testing system with NLA support, designed for authorized penetration testing and security assessments.

## ⚠️ Legal Notice
This tool is intended for authorized security testing only. Use only on systems you own or have explicit permission to test. Unauthorized access to computer systems is illegal.

## Features

- ✅ **Distributed Architecture**: Scale across multiple workers
- ✅ **NLA Support**: Full Network Level Authentication compatibility
- ✅ **High Performance**: Optimized for maximum PPS (packets per second)
- ✅ **Smart Distribution**: Intelligent credential and target distribution
- ✅ **Real-time Dashboard**: Monitor all workers and progress
- ✅ **Single Executable**: One-file worker deployment
- ✅ **Result Management**: Automatic success collection and reporting
- ✅ **Anti-Detection**: Rate limiting and evasion techniques
- ✅ **Resume Support**: Continue from last checkpoint

## Quick Start

### 1. Server Installation (One-Click)

```bash
sudo ./install.sh
```

### 2. Access Dashboard

```
https://your-server-ip:8443
Default: admin / changeme
```

### 3. Deploy Workers

Generate worker executable from dashboard and deploy to multiple systems.

## Architecture

- **Server**: High-performance Node.js backend
- **Dashboard**: React with real-time WebSocket updates
- **Workers**: Go-based for maximum performance
- **Protocol**: Optimized RDP with NLA handling
- **Database**: SQLite for portability

## Performance Metrics

- Support for 1000+ concurrent workers
- 10,000+ credentials/second processing
- Automatic load balancing
- Smart retry mechanisms
- Connection pooling