class WorkerManager {
    constructor(database) {
        this.db = database;
        this.workers = new Map();
        this.workerSockets = new Map();
    }

    async registerWorker(workerId, socket) {
        const workerInfo = {
            id: workerId,
            socket: socket,
            status: 'online',
            connectedAt: Date.now(),
            lastSeen: Date.now(),
            currentPPS: 0,
            tasksCompleted: 0,
            totalAttempts: 0,
            successfulAttempts: 0,
            hostname: socket.handshake.query.hostname || 'unknown',
            ip: socket.handshake.address
        };

        this.workers.set(workerId, workerInfo);
        this.workerSockets.set(workerId, socket);

        await this.db.registerWorker(workerId, {
            hostname: workerInfo.hostname,
            ip: workerInfo.ip
        });

        // Set up heartbeat
        this.setupHeartbeat(workerId);

        return workerInfo;
    }

    unregisterWorker(workerId) {
        const worker = this.workers.get(workerId);
        if (worker) {
            worker.status = 'offline';
            this.db.updateWorkerStatus(workerId, { status: 'offline' });
        }
        
        this.workers.delete(workerId);
        this.workerSockets.delete(workerId);
        
        if (worker && worker.heartbeatInterval) {
            clearInterval(worker.heartbeatInterval);
        }
    }

    setupHeartbeat(workerId) {
        const worker = this.workers.get(workerId);
        if (!worker) return;

        worker.heartbeatInterval = setInterval(() => {
            const socket = this.workerSockets.get(workerId);
            if (socket && socket.connected) {
                socket.emit('ping');
                
                // Check if worker is responsive
                const timeout = setTimeout(() => {
                    console.log(`Worker ${workerId} not responding, marking as offline`);
                    this.unregisterWorker(workerId);
                }, 10000);

                socket.once('pong', () => {
                    clearTimeout(timeout);
                    worker.lastSeen = Date.now();
                });
            } else {
                this.unregisterWorker(workerId);
            }
        }, 30000); // Heartbeat every 30 seconds
    }

    async updateWorkerStatus(workerId, status) {
        const worker = this.workers.get(workerId);
        if (worker) {
            Object.assign(worker, status);
            worker.lastSeen = Date.now();
            
            if (status.pps) {
                worker.currentPPS = status.pps;
            }
            if (status.tasksCompleted) {
                worker.tasksCompleted += status.tasksCompleted;
            }
            if (status.attempts) {
                worker.totalAttempts += status.attempts;
            }
            if (status.successes) {
                worker.successfulAttempts += status.successes;
            }

            await this.db.updateWorkerStatus(workerId, status);
        }
    }

    async getAllWorkers() {
        const workers = await this.db.getWorkers();
        
        // Merge with in-memory data for real-time status
        return workers.map(dbWorker => {
            const memWorker = this.workers.get(dbWorker.id);
            if (memWorker) {
                return {
                    ...dbWorker,
                    status: memWorker.status,
                    currentPPS: memWorker.currentPPS,
                    isOnline: true
                };
            }
            return {
                ...dbWorker,
                isOnline: false
            };
        });
    }

    async getActiveWorkerCount() {
        return this.workers.size;
    }

    async getAveragePPS() {
        if (this.workers.size === 0) return 0;
        
        let totalPPS = 0;
        for (const worker of this.workers.values()) {
            totalPPS += worker.currentPPS || 0;
        }
        
        return Math.round(totalPPS / this.workers.size);
    }

    getWorkerSocket(workerId) {
        return this.workerSockets.get(workerId);
    }

    broadcastToWorkers(event, data) {
        for (const socket of this.workerSockets.values()) {
            if (socket && socket.connected) {
                socket.emit(event, data);
            }
        }
    }

    // Send command to specific worker
    sendToWorker(workerId, event, data) {
        const socket = this.workerSockets.get(workerId);
        if (socket && socket.connected) {
            socket.emit(event, data);
            return true;
        }
        return false;
    }

    // Load balancing - get best worker for task
    getBestWorker() {
        let bestWorker = null;
        let lowestLoad = Infinity;

        for (const worker of this.workers.values()) {
            if (worker.status === 'online') {
                // Calculate load based on current tasks and PPS
                const load = worker.tasksInProgress / (worker.currentPPS || 1);
                if (load < lowestLoad) {
                    lowestLoad = load;
                    bestWorker = worker;
                }
            }
        }

        return bestWorker;
    }

    // Get worker statistics
    getStatistics() {
        const stats = {
            totalWorkers: this.workers.size,
            onlineWorkers: 0,
            totalPPS: 0,
            totalAttempts: 0,
            totalSuccesses: 0,
            avgTasksPerWorker: 0
        };

        for (const worker of this.workers.values()) {
            if (worker.status === 'online') {
                stats.onlineWorkers++;
                stats.totalPPS += worker.currentPPS || 0;
            }
            stats.totalAttempts += worker.totalAttempts || 0;
            stats.totalSuccesses += worker.successfulAttempts || 0;
        }

        if (stats.onlineWorkers > 0) {
            stats.avgTasksPerWorker = Math.round(
                Array.from(this.workers.values())
                    .reduce((sum, w) => sum + (w.tasksCompleted || 0), 0) / stats.onlineWorkers
            );
        }

        return stats;
    }
}

module.exports = WorkerManager;