const { v4: uuidv4 } = require('uuid');

class TaskManager {
    constructor(database) {
        this.db = database;
        this.activeCampaigns = new Map();
        this.taskQueue = [];
        this.distributionStrategy = 'round-robin';
    }

    async addTargets(targets) {
        await this.db.addTargets(targets);
    }

    async addCredentials(users, passwords) {
        await this.db.addCredentials(users, passwords);
    }

    async getTotalTargets() {
        return new Promise((resolve, reject) => {
            this.db.db.get(
                'SELECT COUNT(*) as count FROM targets',
                (err, row) => {
                    if (err) reject(err);
                    else resolve(row.count);
                }
            );
        });
    }

    async getTotalCredentials() {
        return new Promise((resolve, reject) => {
            this.db.db.get(
                'SELECT COUNT(*) as count FROM credentials',
                (err, row) => {
                    if (err) reject(err);
                    else resolve(row.count);
                }
            );
        });
    }

    async createCampaign(config) {
        const campaignId = await this.db.createCampaign(config);
        this.activeCampaigns.set(campaignId, {
            id: campaignId,
            config,
            status: 'pending',
            startTime: null,
            tasksGenerated: false
        });
        return campaignId;
    }

    async startCampaign(campaignId) {
        const campaign = this.activeCampaigns.get(campaignId);
        if (!campaign) {
            throw new Error('Campaign not found');
        }

        campaign.status = 'running';
        campaign.startTime = Date.now();
        
        await this.db.updateCampaignStatus(campaignId, 'running');
        
        // Generate tasks for the campaign
        if (!campaign.tasksGenerated) {
            await this.generateTasks(campaignId);
            campaign.tasksGenerated = true;
        }

        return true;
    }

    async stopCampaign(campaignId) {
        const campaign = this.activeCampaigns.get(campaignId);
        if (campaign) {
            campaign.status = 'stopped';
            await this.db.updateCampaignStatus(campaignId, 'stopped');
        }
        this.activeCampaigns.delete(campaignId);
    }

    async generateTasks(campaignId) {
        // Get all targets and credentials
        const targets = await this.db.getTargets(10000);
        const credentials = await this.db.getCredentials(10000);

        // Create task batches for efficient distribution
        const batchSize = 50; // Credentials per task
        const tasks = [];

        for (const target of targets) {
            for (let i = 0; i < credentials.length; i += batchSize) {
                const credBatch = credentials.slice(i, i + batchSize);
                const task = {
                    campaignId,
                    targetIp: target.ip,
                    targetPort: target.port,
                    credentials: credBatch
                };
                
                const taskId = await this.db.createTask(task);
                tasks.push(taskId);
            }
        }

        console.log(`Generated ${tasks.length} tasks for campaign ${campaignId}`);
        return tasks;
    }

    async getNextTask(workerId) {
        // Get next pending task from database
        const task = await this.db.getNextTask(workerId);
        
        if (task) {
            // Parse credentials if stored as JSON
            if (typeof task.credentials === 'string') {
                task.credentials = JSON.parse(task.credentials);
            }

            // Add campaign config to task
            const campaign = Array.from(this.activeCampaigns.values())
                .find(c => c.id === task.campaign_id);
            
            if (campaign) {
                task.config = campaign.config;
            }
        }

        return task;
    }

    async completeTask(taskId, result) {
        await this.db.completeTask(taskId, result);
        
        // Save individual results if provided
        if (result && result.results) {
            for (const res of result.results) {
                await this.db.saveResult(res);
            }
        }
    }

    async getCompletedTaskCount() {
        return new Promise((resolve, reject) => {
            this.db.db.get(
                'SELECT COUNT(*) as count FROM tasks WHERE status = "completed"',
                (err, row) => {
                    if (err) reject(err);
                    else resolve(row.count);
                }
            );
        });
    }

    async getActiveTaskCount() {
        return new Promise((resolve, reject) => {
            this.db.db.get(
                'SELECT COUNT(*) as count FROM tasks WHERE status IN ("pending", "assigned")',
                (err, row) => {
                    if (err) reject(err);
                    else resolve(row.count);
                }
            );
        });
    }

    // Optimize task distribution for maximum PPS
    optimizeDistribution(workers) {
        // Sort workers by performance (PPS)
        const sortedWorkers = workers.sort((a, b) => b.currentPPS - a.currentPPS);
        
        // Assign more tasks to faster workers
        const distribution = [];
        const totalPPS = sortedWorkers.reduce((sum, w) => sum + w.currentPPS, 0);
        
        for (const worker of sortedWorkers) {
            const weight = worker.currentPPS / totalPPS;
            const taskCount = Math.ceil(this.taskQueue.length * weight);
            distribution.push({
                workerId: worker.id,
                taskCount
            });
        }
        
        return distribution;
    }
}

module.exports = TaskManager;