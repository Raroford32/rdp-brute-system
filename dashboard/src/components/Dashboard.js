import React, { useState, useEffect } from 'react';
import {
  Box,
  Grid,
  Card,
  CardContent,
  Typography,
  LinearProgress,
  IconButton,
  Button,
  Chip,
  Paper,
  Tooltip,
} from '@mui/material';
import {
  Speed as SpeedIcon,
  Computer as ComputerIcon,
  VpnKey as VpnKeyIcon,
  CheckCircle as CheckCircleIcon,
  Error as ErrorIcon,
  PlayArrow as PlayIcon,
  Stop as StopIcon,
  Refresh as RefreshIcon,
  TrendingUp as TrendingUpIcon,
} from '@mui/icons-material';
import {
  LineChart,
  Line,
  AreaChart,
  Area,
  BarChart,
  Bar,
  XAxis,
  YAxis,
  CartesianGrid,
  Tooltip as RechartsTooltip,
  ResponsiveContainer,
  PieChart,
  Pie,
  Cell,
} from 'recharts';
import { useSocket } from '../contexts/SocketContext';
import api from '../services/api';

const COLORS = ['#00C49F', '#FFBB28', '#FF8042', '#8884D8'];

function Dashboard() {
  const { socket } = useSocket();
  const [stats, setStats] = useState({
    totalWorkers: 0,
    totalTargets: 0,
    totalCredentials: 0,
    successfulAttempts: 0,
    failedAttempts: 0,
    averagePPS: 0,
    uptime: 0,
    tasksCompleted: 0,
    tasksInProgress: 0,
  });

  const [performanceData, setPerformanceData] = useState([]);
  const [recentResults, setRecentResults] = useState([]);
  const [campaignActive, setCampaignActive] = useState(false);

  useEffect(() => {
    fetchStats();
    const interval = setInterval(fetchStats, 5000);

    if (socket) {
      socket.on('worker-update', handleWorkerUpdate);
      socket.on('new-result', handleNewResult);
    }

    return () => {
      clearInterval(interval);
      if (socket) {
        socket.off('worker-update');
        socket.off('new-result');
      }
    };
  }, [socket]);

  const fetchStats = async () => {
    try {
      const response = await api.get('/stats');
      setStats(response.data);
      
      // Update performance data for charts
      setPerformanceData(prev => {
        const newData = [...prev, {
          time: new Date().toLocaleTimeString(),
          pps: response.data.averagePPS,
          workers: response.data.totalWorkers,
        }].slice(-20); // Keep last 20 data points
        return newData;
      });
    } catch (error) {
      console.error('Failed to fetch stats:', error);
    }
  };

  const handleWorkerUpdate = (data) => {
    // Update worker-specific stats
    fetchStats();
  };

  const handleNewResult = (result) => {
    setRecentResults(prev => [result, ...prev].slice(0, 10));
    if (result.success) {
      setStats(prev => ({
        ...prev,
        successfulAttempts: prev.successfulAttempts + 1,
      }));
    }
  };

  const handleStartCampaign = async () => {
    try {
      await api.post('/campaign/start', {
        name: `Campaign ${Date.now()}`,
        threadsPerWorker: 10,
        timeout: 5000,
        retryAttempts: 2,
        delayBetweenAttempts: 1000,
      });
      setCampaignActive(true);
    } catch (error) {
      console.error('Failed to start campaign:', error);
    }
  };

  const handleStopCampaign = async () => {
    try {
      // Stop active campaign logic here
      setCampaignActive(false);
    } catch (error) {
      console.error('Failed to stop campaign:', error);
    }
  };

  const successRate = stats.successfulAttempts + stats.failedAttempts > 0
    ? ((stats.successfulAttempts / (stats.successfulAttempts + stats.failedAttempts)) * 100).toFixed(2)
    : 0;

  const pieData = [
    { name: 'Successful', value: stats.successfulAttempts },
    { name: 'Failed', value: stats.failedAttempts },
    { name: 'In Progress', value: stats.tasksInProgress },
  ];

  return (
    <Box className="fade-in">
      <Box sx={{ mb: 3, display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
        <Typography variant="h4" component="h1">
          Dashboard
        </Typography>
        <Box>
          <Button
            variant="contained"
            color={campaignActive ? "error" : "primary"}
            startIcon={campaignActive ? <StopIcon /> : <PlayIcon />}
            onClick={campaignActive ? handleStopCampaign : handleStartCampaign}
            sx={{ mr: 2 }}
          >
            {campaignActive ? 'Stop Campaign' : 'Start Campaign'}
          </Button>
          <IconButton onClick={fetchStats} color="primary">
            <RefreshIcon />
          </IconButton>
        </Box>
      </Box>

      <Grid container spacing={3}>
        {/* Stats Cards */}
        <Grid item xs={12} sm={6} md={3}>
          <Card>
            <CardContent>
              <Box sx={{ display: 'flex', alignItems: 'center', mb: 2 }}>
                <ComputerIcon sx={{ mr: 2, color: 'primary.main' }} />
                <Typography color="textSecondary" gutterBottom>
                  Active Workers
                </Typography>
              </Box>
              <Typography variant="h3">
                {stats.totalWorkers}
              </Typography>
              <Chip
                label="Online"
                color="success"
                size="small"
                sx={{ mt: 1 }}
              />
            </CardContent>
          </Card>
        </Grid>

        <Grid item xs={12} sm={6} md={3}>
          <Card>
            <CardContent>
              <Box sx={{ display: 'flex', alignItems: 'center', mb: 2 }}>
                <SpeedIcon sx={{ mr: 2, color: 'warning.main' }} />
                <Typography color="textSecondary" gutterBottom>
                  Average PPS
                </Typography>
              </Box>
              <Typography variant="h3">
                {stats.averagePPS.toFixed(0)}
              </Typography>
              <LinearProgress
                variant="determinate"
                value={Math.min((stats.averagePPS / 1000) * 100, 100)}
                sx={{ mt: 1 }}
              />
            </CardContent>
          </Card>
        </Grid>

        <Grid item xs={12} sm={6} md={3}>
          <Card>
            <CardContent>
              <Box sx={{ display: 'flex', alignItems: 'center', mb: 2 }}>
                <CheckCircleIcon sx={{ mr: 2, color: 'success.main' }} />
                <Typography color="textSecondary" gutterBottom>
                  Success Rate
                </Typography>
              </Box>
              <Typography variant="h3">
                {successRate}%
              </Typography>
              <Typography variant="body2" color="textSecondary" sx={{ mt: 1 }}>
                {stats.successfulAttempts} successful
              </Typography>
            </CardContent>
          </Card>
        </Grid>

        <Grid item xs={12} sm={6} md={3}>
          <Card>
            <CardContent>
              <Box sx={{ display: 'flex', alignItems: 'center', mb: 2 }}>
                <VpnKeyIcon sx={{ mr: 2, color: 'info.main' }} />
                <Typography color="textSecondary" gutterBottom>
                  Credentials
                </Typography>
              </Box>
              <Typography variant="h3">
                {stats.totalCredentials}
              </Typography>
              <Typography variant="body2" color="textSecondary" sx={{ mt: 1 }}>
                {stats.totalTargets} targets
              </Typography>
            </CardContent>
          </Card>
        </Grid>

        {/* Performance Chart */}
        <Grid item xs={12} md={8}>
          <Paper sx={{ p: 3 }}>
            <Typography variant="h6" gutterBottom>
              Performance Metrics
            </Typography>
            <ResponsiveContainer width="100%" height={300}>
              <AreaChart data={performanceData}>
                <CartesianGrid strokeDasharray="3 3" stroke="rgba(255,255,255,0.1)" />
                <XAxis dataKey="time" stroke="rgba(255,255,255,0.5)" />
                <YAxis stroke="rgba(255,255,255,0.5)" />
                <RechartsTooltip
                  contentStyle={{ backgroundColor: '#1e1e2e', border: 'none' }}
                />
                <Area
                  type="monotone"
                  dataKey="pps"
                  stroke="#90caf9"
                  fill="rgba(144, 202, 249, 0.3)"
                  strokeWidth={2}
                />
                <Area
                  type="monotone"
                  dataKey="workers"
                  stroke="#f48fb1"
                  fill="rgba(244, 143, 177, 0.3)"
                  strokeWidth={2}
                />
              </AreaChart>
            </ResponsiveContainer>
          </Paper>
        </Grid>

        {/* Success Distribution */}
        <Grid item xs={12} md={4}>
          <Paper sx={{ p: 3 }}>
            <Typography variant="h6" gutterBottom>
              Task Distribution
            </Typography>
            <ResponsiveContainer width="100%" height={300}>
              <PieChart>
                <Pie
                  data={pieData}
                  cx="50%"
                  cy="50%"
                  labelLine={false}
                  label={(entry) => `${entry.name}: ${entry.value}`}
                  outerRadius={80}
                  fill="#8884d8"
                  dataKey="value"
                >
                  {pieData.map((entry, index) => (
                    <Cell key={`cell-${index}`} fill={COLORS[index % COLORS.length]} />
                  ))}
                </Pie>
                <RechartsTooltip
                  contentStyle={{ backgroundColor: '#1e1e2e', border: 'none' }}
                />
              </PieChart>
            </ResponsiveContainer>
          </Paper>
        </Grid>

        {/* Recent Results */}
        <Grid item xs={12}>
          <Paper sx={{ p: 3 }}>
            <Typography variant="h6" gutterBottom>
              Recent Successful Attempts
            </Typography>
            <Box sx={{ mt: 2 }}>
              {recentResults.filter(r => r.success).slice(0, 5).map((result, index) => (
                <Box
                  key={index}
                  sx={{
                    display: 'flex',
                    alignItems: 'center',
                    justifyContent: 'space-between',
                    p: 2,
                    mb: 1,
                    bgcolor: 'rgba(0, 196, 159, 0.1)',
                    borderRadius: 1,
                    border: '1px solid rgba(0, 196, 159, 0.3)',
                  }}
                >
                  <Box sx={{ display: 'flex', alignItems: 'center' }}>
                    <CheckCircleIcon sx={{ color: 'success.main', mr: 2 }} />
                    <Box>
                      <Typography variant="body1">
                        {result.ip}:{result.port}
                      </Typography>
                      <Typography variant="body2" color="textSecondary">
                        {result.username} / {result.password}
                      </Typography>
                    </Box>
                  </Box>
                  <Chip
                    label={`Worker ${result.workerId?.slice(0, 8)}`}
                    size="small"
                    variant="outlined"
                  />
                </Box>
              ))}
              {recentResults.filter(r => r.success).length === 0 && (
                <Typography variant="body2" color="textSecondary" sx={{ textAlign: 'center', py: 3 }}>
                  No successful attempts yet
                </Typography>
              )}
            </Box>
          </Paper>
        </Grid>
      </Grid>
    </Box>
  );
}

export default Dashboard;