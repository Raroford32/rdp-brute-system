# Remote Management System

A high-performance remote management system with one-click installation, advanced dashboard, and optimized client/worker management.

## Features

- ✅ One-click server installation
- ✅ Advanced web dashboard with real-time monitoring
- ✅ Single executable payload generator
- ✅ Batch processing for IPs, users, and passwords
- ✅ High-performance PPS optimization
- ✅ Secure authentication and encryption
- ✅ Real-time worker status monitoring
- ✅ Task distribution and management

## Quick Start

### Server Installation (One-Click)

```bash
sudo ./install.sh
```

### Access Dashboard

After installation, access the dashboard at:
```
https://your-server-ip:8443
Default credentials: admin / changeme
```

## Architecture

- **Server**: Node.js + Express + Socket.io
- **Dashboard**: React + Material-UI
- **Database**: SQLite (portable)
- **Client**: Go (compiled to single executable)
- **Communication**: WebSocket + REST API

## Performance

- Optimized for high PPS (packets per second)
- Concurrent worker management
- Efficient task distribution
- Real-time monitoring with minimal overhead