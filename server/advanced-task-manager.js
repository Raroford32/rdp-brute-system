const { v4: uuidv4 } = require('uuid');
const crypto = require('crypto');

class AdvancedTaskManager {
    constructor(database) {
        this.db = database;
        this.activeCampaigns = new Map();
        this.taskQueue = [];
        this.workerLoad = new Map();
        this.credentialCache = new Map();
        this.targetCache = new Map();
        
        // Performance optimization settings
        this.chunkSize = 100; // Credentials per chunk
        this.maxChunksPerWorker = 10;
        this.rebalanceInterval = 30000; // 30 seconds
        this.adaptiveChunking = true;
        
        // Distribution strategies
        this.strategies = {
            'performance-weighted': this.performanceWeightedDistribution.bind(this),
            'round-robin': this.roundRobinDistribution.bind(this),
            'least-loaded': this.leastLoadedDistribution.bind(this),
            'geographic': this.geographicDistribution.bind(this),
            'adaptive': this.adaptiveDistribution.bind(this)
        };
        
        this.currentStrategy = 'adaptive';
        
        // Statistics tracking
        this.stats = {
            totalTasksCreated: 0,
            totalTasksDistributed: 0,
            totalTasksCompleted: 0,
            averageCompletionTime: 0,
            workerEfficiency: new Map(),
            targetDifficulty: new Map()
        };
        
        // Start rebalancing timer
        this.startRebalancing();
    }

    // Advanced task generation with intelligent chunking
    async generateOptimizedTasks(campaignId, targets, credentials) {
        const campaign = this.activeCampaigns.get(campaignId);
        if (!campaign) throw new Error('Campaign not found');
        
        console.log(`Generating optimized tasks for ${targets.length} targets and ${credentials.length} credentials`);
        
        const tasks = [];
        const taskBatches = new Map();
        
        // Analyze target difficulty based on historical data
        const targetDifficulty = await this.analyzeTargetDifficulty(targets);
        
        // Sort targets by difficulty (easier first for quick wins)
        targets.sort((a, b) => {
            const diffA = targetDifficulty.get(`${a.ip}:${a.port}`) || 0.5;
            const diffB = targetDifficulty.get(`${b.ip}:${b.port}`) || 0.5;
            return diffA - diffB;
        });
        
        // Group credentials by likelihood of success
        const credentialGroups = this.groupCredentialsByLikelihood(credentials);
        
        // Create task chunks with intelligent pairing
        for (const target of targets) {
            const targetKey = `${target.ip}:${target.port}`;
            const difficulty = targetDifficulty.get(targetKey) || 0.5;
            
            // Determine optimal chunk size based on target difficulty
            const optimalChunkSize = this.calculateOptimalChunkSize(difficulty);
            
            // Create chunks for this target
            for (const credGroup of credentialGroups) {
                for (let i = 0; i < credGroup.length; i += optimalChunkSize) {
                    const chunk = credGroup.slice(i, i + optimalChunkSize);
                    
                    const task = {
                        id: uuidv4(),
                        campaignId,
                        targetIp: target.ip,
                        targetPort: target.port,
                        credentials: chunk,
                        priority: this.calculateTaskPriority(difficulty, credGroup.priority),
                        estimatedTime: this.estimateTaskTime(chunk.length, difficulty),
                        retryCount: 0,
                        maxRetries: 3,
                        createdAt: Date.now()
                    };
                    
                    tasks.push(task);
                    this.stats.totalTasksCreated++;
                    
                    // Group tasks by estimated completion time for better distribution
                    const timeGroup = Math.floor(task.estimatedTime / 1000) * 1000;
                    if (!taskBatches.has(timeGroup)) {
                        taskBatches.set(timeGroup, []);
                    }
                    taskBatches.get(timeGroup).push(task);
                }
            }
        }
        
        // Store tasks in database with batch optimization
        await this.batchInsertTasks(tasks);
        
        // Prepare initial distribution plan
        campaign.taskBatches = taskBatches;
        campaign.totalTasks = tasks.length;
        campaign.taskQueue = tasks.sort((a, b) => b.priority - a.priority);
        
        console.log(`Generated ${tasks.length} optimized tasks with intelligent chunking`);
        return tasks;
    }

    // Analyze target difficulty based on historical success rates
    async analyzeTargetDifficulty(targets) {
        const difficulty = new Map();
        
        for (const target of targets) {
            const key = `${target.ip}:${target.port}`;
            
            // Check historical success rate for this target
            const history = await this.db.getTargetHistory(target.ip, target.port);
            
            if (history) {
                const successRate = history.successes / (history.attempts || 1);
                const avgTime = history.avgResponseTime || 5000;
                
                // Calculate difficulty score (0-1, higher is more difficult)
                const difficultyScore = (1 - successRate) * (avgTime / 10000);
                difficulty.set(key, Math.min(Math.max(difficultyScore, 0), 1));
            } else {
                // Default difficulty for new targets
                difficulty.set(key, 0.5);
            }
        }
        
        return difficulty;
    }

    // Group credentials by likelihood of success
    groupCredentialsByLikelihood(credentials) {
        const groups = [
            { priority: 1, credentials: [] }, // High likelihood
            { priority: 2, credentials: [] }, // Medium likelihood
            { priority: 3, credentials: [] }  // Low likelihood
        ];
        
        // Common/weak passwords that are more likely to work
        const commonPasswords = new Set([
            'password', '123456', 'admin', 'administrator', 'Password1',
            'Welcome1', 'Password123', 'Admin123', 'admin123', 'root',
            'toor', 'pass', 'qwerty', '12345678', 'password123'
        ]);
        
        // Common usernames
        const commonUsernames = new Set([
            'administrator', 'admin', 'user', 'test', 'guest',
            'demo', 'oracle', 'postgres', 'web', 'root'
        ]);
        
        for (const cred of credentials) {
            const isCommonUsername = commonUsernames.has(cred.username.toLowerCase());
            const isCommonPassword = commonPasswords.has(cred.password.toLowerCase());
            
            if (isCommonUsername && isCommonPassword) {
                groups[0].credentials.push(cred);
            } else if (isCommonUsername || isCommonPassword) {
                groups[1].credentials.push(cred);
            } else {
                groups[2].credentials.push(cred);
            }
        }
        
        return groups.filter(g => g.credentials.length > 0);
    }

    // Calculate optimal chunk size based on target difficulty
    calculateOptimalChunkSize(difficulty) {
        // Base chunk size
        let chunkSize = this.chunkSize;
        
        if (this.adaptiveChunking) {
            // Adjust chunk size based on difficulty
            // Harder targets get smaller chunks for better distribution
            if (difficulty > 0.7) {
                chunkSize = Math.max(25, Math.floor(chunkSize * 0.5));
            } else if (difficulty > 0.4) {
                chunkSize = Math.max(50, Math.floor(chunkSize * 0.75));
            }
            
            // Consider current worker count
            const activeWorkers = this.workerLoad.size;
            if (activeWorkers > 10) {
                // More workers = smaller chunks for better parallelization
                chunkSize = Math.max(20, Math.floor(chunkSize / Math.sqrt(activeWorkers)));
            }
        }
        
        return chunkSize;
    }

    // Calculate task priority
    calculateTaskPriority(difficulty, credentialPriority) {
        // Higher priority for easier targets with likely credentials
        const basePriority = 100;
        const difficultyFactor = (1 - difficulty) * 50;
        const credentialFactor = (4 - credentialPriority) * 20;
        
        return basePriority + difficultyFactor + credentialFactor;
    }

    // Estimate task completion time
    estimateTaskTime(credentialCount, difficulty) {
        const baseTimePerCred = 500; // Base time in ms
        const difficultyMultiplier = 1 + (difficulty * 2);
        const parallelFactor = 0.7; // Accounting for parallel processing
        
        return Math.floor(credentialCount * baseTimePerCred * difficultyMultiplier * parallelFactor);
    }

    // Adaptive distribution strategy
    async adaptiveDistribution(workers, tasks) {
        const distribution = new Map();
        
        // Analyze current worker performance
        const workerMetrics = await this.analyzeWorkerPerformance(workers);
        
        // Sort workers by efficiency score
        const sortedWorkers = Array.from(workerMetrics.entries())
            .sort((a, b) => b[1].score - a[1].score);
        
        // Calculate total capacity
        const totalCapacity = sortedWorkers.reduce((sum, [_, metrics]) => 
            sum + metrics.capacity, 0);
        
        // Distribute tasks proportionally to worker capacity
        let taskIndex = 0;
        for (const [workerId, metrics] of sortedWorkers) {
            const workerShare = Math.ceil((metrics.capacity / totalCapacity) * tasks.length);
            const workerTasks = [];
            
            for (let i = 0; i < workerShare && taskIndex < tasks.length; i++) {
                // Assign tasks that match worker's strengths
                const task = this.selectBestTaskForWorker(tasks.slice(taskIndex), metrics);
                if (task) {
                    workerTasks.push(task);
                    taskIndex++;
                }
            }
            
            distribution.set(workerId, workerTasks);
        }
        
        return distribution;
    }

    // Analyze worker performance metrics
    async analyzeWorkerPerformance(workers) {
        const metrics = new Map();
        
        for (const worker of workers) {
            const history = await this.db.getWorkerHistory(worker.id);
            
            const workerMetrics = {
                id: worker.id,
                currentPPS: worker.currentPPS || 0,
                avgPPS: history.avgPPS || 0,
                successRate: history.successRate || 0,
                avgCompletionTime: history.avgCompletionTime || 5000,
                reliability: this.calculateReliability(history),
                capacity: this.calculateWorkerCapacity(worker, history),
                score: 0
            };
            
            // Calculate overall efficiency score
            workerMetrics.score = 
                (workerMetrics.currentPPS * 0.3) +
                (workerMetrics.avgPPS * 0.2) +
                (workerMetrics.successRate * 100 * 0.2) +
                (workerMetrics.reliability * 100 * 0.2) +
                ((1 / workerMetrics.avgCompletionTime) * 10000 * 0.1);
            
            metrics.set(worker.id, workerMetrics);
        }
        
        return metrics;
    }

    // Calculate worker reliability score
    calculateReliability(history) {
        if (!history || history.totalTasks === 0) return 0.5;
        
        const completionRate = history.completedTasks / history.totalTasks;
        const uptimeRate = history.uptime / (history.totalTime || 1);
        const errorRate = 1 - (history.errors / (history.totalTasks || 1));
        
        return (completionRate * 0.5) + (uptimeRate * 0.3) + (errorRate * 0.2);
    }

    // Calculate worker capacity
    calculateWorkerCapacity(worker, history) {
        const baseCapa = worker.threadsPerWorker || 10;
        const ppsFactor = (worker.currentPPS || 100) / 100;
        const reliabilityFactor = this.calculateReliability(history);
        
        return Math.floor(baseCapacity * ppsFactor * reliabilityFactor);
    }

    // Select best task for specific worker
    selectBestTaskForWorker(availableTasks, workerMetrics) {
        if (availableTasks.length === 0) return null;
        
        // Find task that best matches worker capabilities
        let bestTask = availableTasks[0];
        let bestScore = 0;
        
        for (const task of availableTasks) {
            const score = this.calculateTaskWorkerMatch(task, workerMetrics);
            if (score > bestScore) {
                bestScore = score;
                bestTask = task;
            }
        }
        
        return bestTask;
    }

    // Calculate how well a task matches a worker
    calculateTaskWorkerMatch(task, workerMetrics) {
        let score = 0;
        
        // Worker can handle estimated time
        if (task.estimatedTime <= workerMetrics.avgCompletionTime * 1.5) {
            score += 30;
        }
        
        // Worker has good success rate
        if (workerMetrics.successRate > 0.7) {
            score += 20;
        }
        
        // Task priority alignment
        score += task.priority * 0.1;
        
        // Worker current load (prefer less loaded workers)
        const currentLoad = this.workerLoad.get(workerMetrics.id) || 0;
        score += (100 - currentLoad) * 0.3;
        
        return score;
    }

    // Rebalance tasks periodically
    startRebalancing() {
        setInterval(async () => {
            await this.rebalanceTasks();
        }, this.rebalanceInterval);
    }

    // Rebalance tasks among workers
    async rebalanceTasks() {
        const activeCampaigns = Array.from(this.activeCampaigns.values())
            .filter(c => c.status === 'running');
        
        for (const campaign of activeCampaigns) {
            const workers = await this.db.getActiveWorkers();
            if (workers.length === 0) continue;
            
            // Get incomplete tasks
            const incompleteTasks = await this.db.getIncompleteTasks(campaign.id);
            
            // Check for stalled tasks
            const stalledTasks = incompleteTasks.filter(task => {
                const timeSinceAssigned = Date.now() - task.assignedAt;
                return task.status === 'assigned' && timeSinceAssigned > 60000; // 1 minute
            });
            
            // Reassign stalled tasks
            for (const task of stalledTasks) {
                await this.reassignTask(task, workers);
            }
            
            // Check worker load balance
            const loadVariance = this.calculateLoadVariance(workers);
            if (loadVariance > 0.3) {
                // Significant imbalance detected, redistribute
                await this.redistributeTasks(campaign.id, workers);
            }
        }
    }

    // Calculate load variance among workers
    calculateLoadVariance(workers) {
        if (workers.length === 0) return 0;
        
        const loads = workers.map(w => this.workerLoad.get(w.id) || 0);
        const avgLoad = loads.reduce((a, b) => a + b, 0) / loads.length;
        
        const variance = loads.reduce((sum, load) => {
            return sum + Math.pow(load - avgLoad, 2);
        }, 0) / loads.length;
        
        return Math.sqrt(variance) / (avgLoad || 1);
    }

    // Reassign a stalled task
    async reassignTask(task, workers) {
        // Find best available worker
        const metrics = await this.analyzeWorkerPerformance(workers);
        const sortedWorkers = Array.from(metrics.entries())
            .sort((a, b) => b[1].score - a[1].score);
        
        for (const [workerId, _] of sortedWorkers) {
            const currentLoad = this.workerLoad.get(workerId) || 0;
            if (currentLoad < this.maxChunksPerWorker) {
                await this.db.reassignTask(task.id, workerId);
                this.workerLoad.set(workerId, currentLoad + 1);
                
                console.log(`Reassigned stalled task ${task.id} to worker ${workerId}`);
                break;
            }
        }
    }

    // Redistribute tasks for better balance
    async redistributeTasks(campaignId, workers) {
        console.log(`Redistributing tasks for campaign ${campaignId}`);
        
        const pendingTasks = await this.db.getPendingTasks(campaignId);
        if (pendingTasks.length === 0) return;
        
        // Use adaptive distribution strategy
        const distribution = await this.adaptiveDistribution(workers, pendingTasks);
        
        // Apply new distribution
        for (const [workerId, tasks] of distribution.entries()) {
            for (const task of tasks) {
                await this.db.assignTask(task.id, workerId);
            }
            this.workerLoad.set(workerId, tasks.length);
        }
        
        console.log(`Redistributed ${pendingTasks.length} tasks among ${workers.length} workers`);
    }

    // Get next optimized task for worker
    async getNextOptimizedTask(workerId) {
        // Get worker metrics
        const worker = await this.db.getWorker(workerId);
        const history = await this.db.getWorkerHistory(workerId);
        const metrics = {
            currentPPS: worker.currentPPS || 0,
            avgCompletionTime: history.avgCompletionTime || 5000,
            successRate: history.successRate || 0
        };
        
        // Find best matching task from all campaigns
        let bestTask = null;
        let bestScore = 0;
        
        for (const campaign of this.activeCampaigns.values()) {
            if (campaign.status !== 'running') continue;
            
            const availableTasks = campaign.taskQueue.filter(t => !t.assigned);
            const task = this.selectBestTaskForWorker(availableTasks, metrics);
            
            if (task) {
                const score = this.calculateTaskWorkerMatch(task, metrics);
                if (score > bestScore) {
                    bestScore = score;
                    bestTask = task;
                }
            }
        }
        
        if (bestTask) {
            bestTask.assigned = true;
            bestTask.assignedTo = workerId;
            bestTask.assignedAt = Date.now();
            
            // Update worker load
            const currentLoad = this.workerLoad.get(workerId) || 0;
            this.workerLoad.set(workerId, currentLoad + 1);
            
            // Save to database
            await this.db.assignTask(bestTask.id, workerId);
            
            return bestTask;
        }
        
        return null;
    }

    // Handle task completion
    async handleTaskCompletion(taskId, workerId, results) {
        const task = await this.db.getTask(taskId);
        if (!task) return;
        
        // Update worker load
        const currentLoad = this.workerLoad.get(workerId) || 1;
        this.workerLoad.set(workerId, Math.max(0, currentLoad - 1));
        
        // Update statistics
        this.stats.totalTasksCompleted++;
        const completionTime = Date.now() - task.createdAt;
        this.stats.averageCompletionTime = 
            (this.stats.averageCompletionTime * (this.stats.totalTasksCompleted - 1) + completionTime) / 
            this.stats.totalTasksCompleted;
        
        // Update worker efficiency
        const efficiency = this.stats.workerEfficiency.get(workerId) || { completed: 0, totalTime: 0 };
        efficiency.completed++;
        efficiency.totalTime += completionTime;
        this.stats.workerEfficiency.set(workerId, efficiency);
        
        // Update target difficulty based on results
        if (results && results.attempts) {
            const targetKey = `${task.targetIp}:${task.targetPort}`;
            const difficulty = this.stats.targetDifficulty.get(targetKey) || { attempts: 0, successes: 0 };
            difficulty.attempts += results.attempts;
            difficulty.successes += results.successes || 0;
            this.stats.targetDifficulty.set(targetKey, difficulty);
        }
        
        // Mark task as completed
        await this.db.completeTask(taskId, results);
    }

    // Batch insert tasks for performance
    async batchInsertTasks(tasks) {
        const batchSize = 1000;
        for (let i = 0; i < tasks.length; i += batchSize) {
            const batch = tasks.slice(i, i + batchSize);
            await this.db.insertTaskBatch(batch);
        }
    }

    // Get distribution statistics
    getDistributionStats() {
        return {
            totalTasks: this.stats.totalTasksCreated,
            distributed: this.stats.totalTasksDistributed,
            completed: this.stats.totalTasksCompleted,
            averageCompletionTime: Math.round(this.stats.averageCompletionTime),
            workerEfficiency: Array.from(this.stats.workerEfficiency.entries()).map(([id, eff]) => ({
                workerId: id,
                completed: eff.completed,
                avgTime: Math.round(eff.totalTime / eff.completed)
            })),
            targetDifficulty: Array.from(this.stats.targetDifficulty.entries()).map(([target, diff]) => ({
                target,
                attempts: diff.attempts,
                successes: diff.successes,
                successRate: (diff.successes / diff.attempts * 100).toFixed(2)
            }))
        };
    }
}

module.exports = AdvancedTaskManager;