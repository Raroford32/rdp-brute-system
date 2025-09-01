const sqlite3 = require('sqlite3').verbose();
const bcrypt = require('bcryptjs');
const path = require('path');

class Database {
    constructor() {
        this.db = new sqlite3.Database(path.join(__dirname, 'data.db'));
        this.initialize();
    }

    initialize() {
        this.db.serialize(() => {
            // Users table
            this.db.run(`
                CREATE TABLE IF NOT EXISTS users (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    username TEXT UNIQUE NOT NULL,
                    password TEXT NOT NULL,
                    role TEXT DEFAULT 'user',
                    created_at DATETIME DEFAULT CURRENT_TIMESTAMP
                )
            `);

            // Targets table
            this.db.run(`
                CREATE TABLE IF NOT EXISTS targets (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    ip TEXT NOT NULL,
                    port INTEGER DEFAULT 3389,
                    status TEXT DEFAULT 'pending',
                    last_attempt DATETIME,
                    attempts INTEGER DEFAULT 0,
                    UNIQUE(ip, port)
                )
            `);

            // Credentials table
            this.db.run(`
                CREATE TABLE IF NOT EXISTS credentials (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    username TEXT NOT NULL,
                    password TEXT NOT NULL,
                    UNIQUE(username, password)
                )
            `);

            // Results table
            this.db.run(`
                CREATE TABLE IF NOT EXISTS results (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    target_ip TEXT NOT NULL,
                    target_port INTEGER,
                    username TEXT NOT NULL,
                    password TEXT NOT NULL,
                    worker_id TEXT,
                    success BOOLEAN,
                    error_message TEXT,
                    discovered_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE(target_ip, target_port, username, password)
                )
            `);

            // Workers table
            this.db.run(`
                CREATE TABLE IF NOT EXISTS workers (
                    id TEXT PRIMARY KEY,
                    hostname TEXT,
                    ip TEXT,
                    status TEXT DEFAULT 'offline',
                    last_seen DATETIME,
                    tasks_completed INTEGER DEFAULT 0,
                    current_pps REAL DEFAULT 0,
                    total_attempts INTEGER DEFAULT 0,
                    successful_attempts INTEGER DEFAULT 0,
                    connected_at DATETIME DEFAULT CURRENT_TIMESTAMP
                )
            `);

            // Campaigns table
            this.db.run(`
                CREATE TABLE IF NOT EXISTS campaigns (
                    id TEXT PRIMARY KEY,
                    name TEXT NOT NULL,
                    status TEXT DEFAULT 'pending',
                    threads_per_worker INTEGER DEFAULT 10,
                    timeout INTEGER DEFAULT 5000,
                    retry_attempts INTEGER DEFAULT 2,
                    delay_between_attempts INTEGER DEFAULT 1000,
                    total_targets INTEGER DEFAULT 0,
                    total_credentials INTEGER DEFAULT 0,
                    completed_attempts INTEGER DEFAULT 0,
                    successful_attempts INTEGER DEFAULT 0,
                    started_at DATETIME,
                    completed_at DATETIME,
                    created_at DATETIME DEFAULT CURRENT_TIMESTAMP
                )
            `);

            // Tasks table
            this.db.run(`
                CREATE TABLE IF NOT EXISTS tasks (
                    id TEXT PRIMARY KEY,
                    campaign_id TEXT,
                    worker_id TEXT,
                    target_ip TEXT,
                    target_port INTEGER,
                    credentials TEXT,
                    status TEXT DEFAULT 'pending',
                    attempts INTEGER DEFAULT 0,
                    assigned_at DATETIME,
                    completed_at DATETIME,
                    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (campaign_id) REFERENCES campaigns(id)
                )
            `);

            // Create indexes for performance
            this.db.run('CREATE INDEX IF NOT EXISTS idx_targets_status ON targets(status)');
            this.db.run('CREATE INDEX IF NOT EXISTS idx_results_success ON results(success)');
            this.db.run('CREATE INDEX IF NOT EXISTS idx_tasks_status ON tasks(status)');
            this.db.run('CREATE INDEX IF NOT EXISTS idx_tasks_worker ON tasks(worker_id)');

            // Insert default admin user if not exists
            this.createDefaultAdmin();
        });
    }

    async createDefaultAdmin() {
        const hashedPassword = await bcrypt.hash('changeme', 10);
        this.db.run(
            'INSERT OR IGNORE INTO users (username, password, role) VALUES (?, ?, ?)',
            ['admin', hashedPassword, 'admin']
        );
    }

    // User methods
    getUser(username) {
        return new Promise((resolve, reject) => {
            this.db.get(
                'SELECT * FROM users WHERE username = ?',
                [username],
                (err, row) => {
                    if (err) reject(err);
                    else resolve(row);
                }
            );
        });
    }

    // Target methods
    addTargets(targets) {
        return new Promise((resolve, reject) => {
            const stmt = this.db.prepare('INSERT OR IGNORE INTO targets (ip, port) VALUES (?, ?)');
            targets.forEach(target => {
                stmt.run(target.ip, target.port);
            });
            stmt.finalize(err => {
                if (err) reject(err);
                else resolve();
            });
        });
    }

    getTargets(limit = 100, status = 'pending') {
        return new Promise((resolve, reject) => {
            this.db.all(
                'SELECT * FROM targets WHERE status = ? LIMIT ?',
                [status, limit],
                (err, rows) => {
                    if (err) reject(err);
                    else resolve(rows);
                }
            );
        });
    }

    updateTargetStatus(id, status, attempts) {
        return new Promise((resolve, reject) => {
            this.db.run(
                'UPDATE targets SET status = ?, attempts = ?, last_attempt = CURRENT_TIMESTAMP WHERE id = ?',
                [status, attempts, id],
                err => {
                    if (err) reject(err);
                    else resolve();
                }
            );
        });
    }

    // Credential methods
    addCredentials(users, passwords) {
        return new Promise((resolve, reject) => {
            const stmt = this.db.prepare('INSERT OR IGNORE INTO credentials (username, password) VALUES (?, ?)');
            users.forEach(user => {
                passwords.forEach(password => {
                    stmt.run(user, password);
                });
            });
            stmt.finalize(err => {
                if (err) reject(err);
                else resolve();
            });
        });
    }

    getCredentials(limit = 1000) {
        return new Promise((resolve, reject) => {
            this.db.all(
                'SELECT * FROM credentials LIMIT ?',
                [limit],
                (err, rows) => {
                    if (err) reject(err);
                    else resolve(rows);
                }
            );
        });
    }

    // Result methods
    saveResult(result) {
        return new Promise((resolve, reject) => {
            this.db.run(
                `INSERT OR REPLACE INTO results 
                (target_ip, target_port, username, password, worker_id, success, error_message) 
                VALUES (?, ?, ?, ?, ?, ?, ?)`,
                [result.ip, result.port, result.username, result.password, 
                 result.workerId, result.success, result.error],
                err => {
                    if (err) reject(err);
                    else resolve();
                }
            );
        });
    }

    getSuccessfulResults() {
        return new Promise((resolve, reject) => {
            this.db.all(
                'SELECT * FROM results WHERE success = 1 ORDER BY discovered_at DESC',
                (err, rows) => {
                    if (err) reject(err);
                    else resolve(rows);
                }
            );
        });
    }

    getSuccessfulAttempts() {
        return new Promise((resolve, reject) => {
            this.db.get(
                'SELECT COUNT(*) as count FROM results WHERE success = 1',
                (err, row) => {
                    if (err) reject(err);
                    else resolve(row.count);
                }
            );
        });
    }

    getFailedAttempts() {
        return new Promise((resolve, reject) => {
            this.db.get(
                'SELECT COUNT(*) as count FROM results WHERE success = 0',
                (err, row) => {
                    if (err) reject(err);
                    else resolve(row.count);
                }
            );
        });
    }

    // Worker methods
    registerWorker(workerId, info = {}) {
        return new Promise((resolve, reject) => {
            this.db.run(
                `INSERT OR REPLACE INTO workers 
                (id, hostname, ip, status, last_seen) 
                VALUES (?, ?, ?, 'online', CURRENT_TIMESTAMP)`,
                [workerId, info.hostname, info.ip],
                err => {
                    if (err) reject(err);
                    else resolve();
                }
            );
        });
    }

    updateWorkerStatus(workerId, status) {
        return new Promise((resolve, reject) => {
            this.db.run(
                `UPDATE workers SET 
                status = ?, 
                last_seen = CURRENT_TIMESTAMP,
                current_pps = ?,
                tasks_completed = tasks_completed + ?,
                total_attempts = total_attempts + ?,
                successful_attempts = successful_attempts + ?
                WHERE id = ?`,
                [status.status || 'online', status.pps || 0, 
                 status.tasksCompleted || 0, status.attempts || 0, 
                 status.successes || 0, workerId],
                err => {
                    if (err) reject(err);
                    else resolve();
                }
            );
        });
    }

    getWorkers() {
        return new Promise((resolve, reject) => {
            this.db.all(
                'SELECT * FROM workers ORDER BY last_seen DESC',
                (err, rows) => {
                    if (err) reject(err);
                    else resolve(rows);
                }
            );
        });
    }

    // Campaign methods
    createCampaign(campaign) {
        return new Promise((resolve, reject) => {
            const id = require('uuid').v4();
            this.db.run(
                `INSERT INTO campaigns 
                (id, name, threads_per_worker, timeout, retry_attempts, delay_between_attempts) 
                VALUES (?, ?, ?, ?, ?, ?)`,
                [id, campaign.name, campaign.threadsPerWorker, campaign.timeout, 
                 campaign.retryAttempts, campaign.delayBetweenAttempts],
                err => {
                    if (err) reject(err);
                    else resolve(id);
                }
            );
        });
    }

    updateCampaignStatus(id, status) {
        return new Promise((resolve, reject) => {
            const query = status === 'completed' 
                ? 'UPDATE campaigns SET status = ?, completed_at = CURRENT_TIMESTAMP WHERE id = ?'
                : 'UPDATE campaigns SET status = ?, started_at = CURRENT_TIMESTAMP WHERE id = ?';
            
            this.db.run(query, [status, id], err => {
                if (err) reject(err);
                else resolve();
            });
        });
    }

    // Task methods
    createTask(task) {
        return new Promise((resolve, reject) => {
            const id = require('uuid').v4();
            this.db.run(
                `INSERT INTO tasks 
                (id, campaign_id, target_ip, target_port, credentials, status) 
                VALUES (?, ?, ?, ?, ?, 'pending')`,
                [id, task.campaignId, task.targetIp, task.targetPort, 
                 JSON.stringify(task.credentials)],
                err => {
                    if (err) reject(err);
                    else resolve(id);
                }
            );
        });
    }

    assignTask(taskId, workerId) {
        return new Promise((resolve, reject) => {
            this.db.run(
                'UPDATE tasks SET worker_id = ?, status = "assigned", assigned_at = CURRENT_TIMESTAMP WHERE id = ?',
                [workerId, taskId],
                err => {
                    if (err) reject(err);
                    else resolve();
                }
            );
        });
    }

    completeTask(taskId, result) {
        return new Promise((resolve, reject) => {
            this.db.run(
                'UPDATE tasks SET status = "completed", completed_at = CURRENT_TIMESTAMP WHERE id = ?',
                [taskId],
                err => {
                    if (err) reject(err);
                    else resolve();
                }
            );
        });
    }

    getNextTask(workerId) {
        return new Promise((resolve, reject) => {
            this.db.get(
                'SELECT * FROM tasks WHERE status = "pending" LIMIT 1',
                async (err, row) => {
                    if (err) {
                        reject(err);
                    } else if (row) {
                        await this.assignTask(row.id, workerId);
                        resolve(row);
                    } else {
                        resolve(null);
                    }
                }
            );
        });
    }

    close() {
        this.db.close();
    }
}

module.exports = Database;