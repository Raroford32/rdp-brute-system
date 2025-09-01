import React, { useState } from 'react';
import {
  Box,
  Paper,
  Typography,
  Button,
  TextField,
  Grid,
  Card,
  CardContent,
  CardActions,
  Dialog,
  DialogTitle,
  DialogContent,
  DialogActions,
  Stepper,
  Step,
  StepLabel,
  FormControl,
  InputLabel,
  Select,
  MenuItem,
  Slider,
  Chip,
  Alert,
  LinearProgress,
} from '@mui/material';
import {
  CloudUpload as UploadIcon,
  PlayArrow as PlayIcon,
  Stop as StopIcon,
  Settings as SettingsIcon,
  Group as GroupIcon,
  VpnKey as KeyIcon,
  Computer as ComputerIcon,
} from '@mui/icons-material';
import { useDropzone } from 'react-dropzone';
import { useSnackbar } from 'notistack';
import api from '../services/api';

function Campaigns() {
  const { enqueueSnackbar } = useSnackbar();
  const [activeStep, setActiveStep] = useState(0);
  const [openDialog, setOpenDialog] = useState(false);
  const [campaigns, setCampaigns] = useState([]);
  const [newCampaign, setNewCampaign] = useState({
    name: '',
    targets: [],
    users: [],
    passwords: [],
    threadsPerWorker: 10,
    timeout: 5000,
    retryAttempts: 2,
    delayBetweenAttempts: 1000,
  });

  const steps = ['Upload Targets', 'Upload Credentials', 'Configure Settings', 'Review & Launch'];

  const handleNext = () => {
    setActiveStep((prevStep) => prevStep + 1);
  };

  const handleBack = () => {
    setActiveStep((prevStep) => prevStep - 1);
  };

  const handleReset = () => {
    setActiveStep(0);
    setNewCampaign({
      name: '',
      targets: [],
      users: [],
      passwords: [],
      threadsPerWorker: 10,
      timeout: 5000,
      retryAttempts: 2,
      delayBetweenAttempts: 1000,
    });
  };

  const onDropTargets = (acceptedFiles) => {
    const file = acceptedFiles[0];
    if (file) {
      const reader = new FileReader();
      reader.onload = (e) => {
        const content = e.target.result;
        const targets = content.split('\n')
          .filter(line => line.trim())
          .map(line => {
            const parts = line.trim().split(':');
            return { ip: parts[0], port: parts[1] || '3389' };
          });
        setNewCampaign(prev => ({ ...prev, targets }));
        enqueueSnackbar(`Loaded ${targets.length} targets`, { variant: 'success' });
      };
      reader.readAsText(file);
    }
  };

  const onDropUsers = (acceptedFiles) => {
    const file = acceptedFiles[0];
    if (file) {
      const reader = new FileReader();
      reader.onload = (e) => {
        const content = e.target.result;
        const users = content.split('\n').filter(line => line.trim());
        setNewCampaign(prev => ({ ...prev, users }));
        enqueueSnackbar(`Loaded ${users.length} usernames`, { variant: 'success' });
      };
      reader.readAsText(file);
    }
  };

  const onDropPasswords = (acceptedFiles) => {
    const file = acceptedFiles[0];
    if (file) {
      const reader = new FileReader();
      reader.onload = (e) => {
        const content = e.target.result;
        const passwords = content.split('\n').filter(line => line.trim());
        setNewCampaign(prev => ({ ...prev, passwords }));
        enqueueSnackbar(`Loaded ${passwords.length} passwords`, { variant: 'success' });
      };
      reader.readAsText(file);
    }
  };

  const targetsDropzone = useDropzone({ onDrop: onDropTargets, accept: { 'text/plain': ['.txt'] } });
  const usersDropzone = useDropzone({ onDrop: onDropUsers, accept: { 'text/plain': ['.txt'] } });
  const passwordsDropzone = useDropzone({ onDrop: onDropPasswords, accept: { 'text/plain': ['.txt'] } });

  const handleLaunchCampaign = async () => {
    try {
      // Upload targets
      const targetsFormData = new FormData();
      const targetsBlob = new Blob([newCampaign.targets.map(t => `${t.ip}:${t.port}`).join('\n')], { type: 'text/plain' });
      targetsFormData.append('file', targetsBlob, 'targets.txt');
      await api.post('/upload/targets', targetsFormData);

      // Upload credentials
      const credentialsFormData = new FormData();
      const usersBlob = new Blob([newCampaign.users.join('\n')], { type: 'text/plain' });
      const passwordsBlob = new Blob([newCampaign.passwords.join('\n')], { type: 'text/plain' });
      credentialsFormData.append('users', usersBlob, 'users.txt');
      credentialsFormData.append('passwords', passwordsBlob, 'passwords.txt');
      await api.post('/upload/credentials', credentialsFormData);

      // Start campaign
      const response = await api.post('/campaign/start', {
        name: newCampaign.name,
        threadsPerWorker: newCampaign.threadsPerWorker,
        timeout: newCampaign.timeout,
        retryAttempts: newCampaign.retryAttempts,
        delayBetweenAttempts: newCampaign.delayBetweenAttempts,
      });

      enqueueSnackbar('Campaign launched successfully!', { variant: 'success' });
      setOpenDialog(false);
      handleReset();
    } catch (error) {
      enqueueSnackbar('Failed to launch campaign', { variant: 'error' });
    }
  };

  const getStepContent = (step) => {
    switch (step) {
      case 0:
        return (
          <Box>
            <Typography variant="h6" gutterBottom>
              Upload Target IPs
            </Typography>
            <Typography variant="body2" color="textSecondary" gutterBottom>
              Upload a text file with target IPs (one per line, format: IP:PORT or just IP)
            </Typography>
            <Box
              {...targetsDropzone.getRootProps()}
              sx={{
                border: '2px dashed',
                borderColor: 'primary.main',
                borderRadius: 2,
                p: 4,
                mt: 2,
                textAlign: 'center',
                cursor: 'pointer',
                bgcolor: 'rgba(144, 202, 249, 0.05)',
                '&:hover': {
                  bgcolor: 'rgba(144, 202, 249, 0.1)',
                },
              }}
            >
              <input {...targetsDropzone.getInputProps()} />
              <UploadIcon sx={{ fontSize: 48, color: 'primary.main', mb: 2 }} />
              <Typography>
                Drag & drop targets.txt here, or click to select
              </Typography>
              {newCampaign.targets.length > 0 && (
                <Chip
                  label={`${newCampaign.targets.length} targets loaded`}
                  color="success"
                  sx={{ mt: 2 }}
                />
              )}
            </Box>
          </Box>
        );

      case 1:
        return (
          <Box>
            <Typography variant="h6" gutterBottom>
              Upload Credentials
            </Typography>
            <Grid container spacing={3}>
              <Grid item xs={12} md={6}>
                <Typography variant="body2" color="textSecondary" gutterBottom>
                  Upload usernames (one per line)
                </Typography>
                <Box
                  {...usersDropzone.getRootProps()}
                  sx={{
                    border: '2px dashed',
                    borderColor: 'secondary.main',
                    borderRadius: 2,
                    p: 3,
                    textAlign: 'center',
                    cursor: 'pointer',
                    bgcolor: 'rgba(244, 143, 177, 0.05)',
                    '&:hover': {
                      bgcolor: 'rgba(244, 143, 177, 0.1)',
                    },
                  }}
                >
                  <input {...usersDropzone.getInputProps()} />
                  <GroupIcon sx={{ fontSize: 36, color: 'secondary.main', mb: 1 }} />
                  <Typography variant="body2">
                    Drop users.txt here
                  </Typography>
                  {newCampaign.users.length > 0 && (
                    <Chip
                      label={`${newCampaign.users.length} users`}
                      size="small"
                      color="success"
                      sx={{ mt: 1 }}
                    />
                  )}
                </Box>
              </Grid>
              <Grid item xs={12} md={6}>
                <Typography variant="body2" color="textSecondary" gutterBottom>
                  Upload passwords (one per line)
                </Typography>
                <Box
                  {...passwordsDropzone.getRootProps()}
                  sx={{
                    border: '2px dashed',
                    borderColor: 'warning.main',
                    borderRadius: 2,
                    p: 3,
                    textAlign: 'center',
                    cursor: 'pointer',
                    bgcolor: 'rgba(255, 167, 38, 0.05)',
                    '&:hover': {
                      bgcolor: 'rgba(255, 167, 38, 0.1)',
                    },
                  }}
                >
                  <input {...passwordsDropzone.getInputProps()} />
                  <KeyIcon sx={{ fontSize: 36, color: 'warning.main', mb: 1 }} />
                  <Typography variant="body2">
                    Drop passwords.txt here
                  </Typography>
                  {newCampaign.passwords.length > 0 && (
                    <Chip
                      label={`${newCampaign.passwords.length} passwords`}
                      size="small"
                      color="success"
                      sx={{ mt: 1 }}
                    />
                  )}
                </Box>
              </Grid>
            </Grid>
            {newCampaign.users.length > 0 && newCampaign.passwords.length > 0 && (
              <Alert severity="info" sx={{ mt: 2 }}>
                Total combinations: {newCampaign.users.length * newCampaign.passwords.length}
              </Alert>
            )}
          </Box>
        );

      case 2:
        return (
          <Box>
            <Typography variant="h6" gutterBottom>
              Configure Campaign Settings
            </Typography>
            <Grid container spacing={3}>
              <Grid item xs={12}>
                <TextField
                  fullWidth
                  label="Campaign Name"
                  value={newCampaign.name}
                  onChange={(e) => setNewCampaign(prev => ({ ...prev, name: e.target.value }))}
                  variant="outlined"
                />
              </Grid>
              <Grid item xs={12} md={6}>
                <Typography gutterBottom>
                  Threads per Worker: {newCampaign.threadsPerWorker}
                </Typography>
                <Slider
                  value={newCampaign.threadsPerWorker}
                  onChange={(e, value) => setNewCampaign(prev => ({ ...prev, threadsPerWorker: value }))}
                  min={1}
                  max={50}
                  marks
                  step={1}
                />
              </Grid>
              <Grid item xs={12} md={6}>
                <Typography gutterBottom>
                  Connection Timeout: {newCampaign.timeout}ms
                </Typography>
                <Slider
                  value={newCampaign.timeout}
                  onChange={(e, value) => setNewCampaign(prev => ({ ...prev, timeout: value }))}
                  min={1000}
                  max={30000}
                  step={1000}
                />
              </Grid>
              <Grid item xs={12} md={6}>
                <Typography gutterBottom>
                  Retry Attempts: {newCampaign.retryAttempts}
                </Typography>
                <Slider
                  value={newCampaign.retryAttempts}
                  onChange={(e, value) => setNewCampaign(prev => ({ ...prev, retryAttempts: value }))}
                  min={0}
                  max={5}
                  marks
                  step={1}
                />
              </Grid>
              <Grid item xs={12} md={6}>
                <Typography gutterBottom>
                  Delay Between Attempts: {newCampaign.delayBetweenAttempts}ms
                </Typography>
                <Slider
                  value={newCampaign.delayBetweenAttempts}
                  onChange={(e, value) => setNewCampaign(prev => ({ ...prev, delayBetweenAttempts: value }))}
                  min={0}
                  max={10000}
                  step={500}
                />
              </Grid>
            </Grid>
          </Box>
        );

      case 3:
        return (
          <Box>
            <Typography variant="h6" gutterBottom>
              Review Campaign Configuration
            </Typography>
            <Grid container spacing={2}>
              <Grid item xs={12}>
                <Alert severity="warning" sx={{ mb: 2 }}>
                  Please ensure you have authorization to test these targets before proceeding.
                </Alert>
              </Grid>
              <Grid item xs={12} md={6}>
                <Card>
                  <CardContent>
                    <Typography variant="subtitle1" color="primary" gutterBottom>
                      Campaign Details
                    </Typography>
                    <Typography variant="body2">Name: {newCampaign.name || 'Unnamed Campaign'}</Typography>
                    <Typography variant="body2">Targets: {newCampaign.targets.length}</Typography>
                    <Typography variant="body2">Usernames: {newCampaign.users.length}</Typography>
                    <Typography variant="body2">Passwords: {newCampaign.passwords.length}</Typography>
                    <Typography variant="body2" color="secondary">
                      Total Attempts: {newCampaign.targets.length * newCampaign.users.length * newCampaign.passwords.length}
                    </Typography>
                  </CardContent>
                </Card>
              </Grid>
              <Grid item xs={12} md={6}>
                <Card>
                  <CardContent>
                    <Typography variant="subtitle1" color="primary" gutterBottom>
                      Performance Settings
                    </Typography>
                    <Typography variant="body2">Threads/Worker: {newCampaign.threadsPerWorker}</Typography>
                    <Typography variant="body2">Timeout: {newCampaign.timeout}ms</Typography>
                    <Typography variant="body2">Retry Attempts: {newCampaign.retryAttempts}</Typography>
                    <Typography variant="body2">Retry Delay: {newCampaign.delayBetweenAttempts}ms</Typography>
                  </CardContent>
                </Card>
              </Grid>
            </Grid>
          </Box>
        );

      default:
        return 'Unknown step';
    }
  };

  return (
    <Box>
      <Box sx={{ mb: 3, display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
        <Typography variant="h4" component="h1">
          Campaigns
        </Typography>
        <Button
          variant="contained"
          startIcon={<PlayIcon />}
          onClick={() => setOpenDialog(true)}
        >
          New Campaign
        </Button>
      </Box>

      <Grid container spacing={3}>
        {campaigns.length === 0 ? (
          <Grid item xs={12}>
            <Paper sx={{ p: 4, textAlign: 'center' }}>
              <ComputerIcon sx={{ fontSize: 64, color: 'text.secondary', mb: 2 }} />
              <Typography variant="h6" color="textSecondary">
                No campaigns yet
              </Typography>
              <Typography variant="body2" color="textSecondary" sx={{ mb: 3 }}>
                Create your first campaign to start testing
              </Typography>
              <Button
                variant="outlined"
                startIcon={<PlayIcon />}
                onClick={() => setOpenDialog(true)}
              >
                Create Campaign
              </Button>
            </Paper>
          </Grid>
        ) : (
          campaigns.map((campaign) => (
            <Grid item xs={12} md={6} lg={4} key={campaign.id}>
              <Card>
                <CardContent>
                  <Typography variant="h6">{campaign.name}</Typography>
                  <Typography variant="body2" color="textSecondary">
                    Status: {campaign.status}
                  </Typography>
                  <LinearProgress
                    variant="determinate"
                    value={campaign.progress || 0}
                    sx={{ mt: 2, mb: 1 }}
                  />
                  <Typography variant="body2">
                    {campaign.completedTasks || 0} / {campaign.totalTasks || 0} tasks
                  </Typography>
                </CardContent>
                <CardActions>
                  <Button size="small" startIcon={<StopIcon />}>
                    Stop
                  </Button>
                  <Button size="small" startIcon={<SettingsIcon />}>
                    Configure
                  </Button>
                </CardActions>
              </Card>
            </Grid>
          ))
        )}
      </Grid>

      <Dialog
        open={openDialog}
        onClose={() => setOpenDialog(false)}
        maxWidth="md"
        fullWidth
      >
        <DialogTitle>Create New Campaign</DialogTitle>
        <DialogContent>
          <Box sx={{ mt: 2 }}>
            <Stepper activeStep={activeStep}>
              {steps.map((label) => (
                <Step key={label}>
                  <StepLabel>{label}</StepLabel>
                </Step>
              ))}
            </Stepper>
            <Box sx={{ mt: 3 }}>
              {getStepContent(activeStep)}
            </Box>
          </Box>
        </DialogContent>
        <DialogActions>
          <Button onClick={() => setOpenDialog(false)}>Cancel</Button>
          <Button
            disabled={activeStep === 0}
            onClick={handleBack}
          >
            Back
          </Button>
          {activeStep === steps.length - 1 ? (
            <Button
              variant="contained"
              onClick={handleLaunchCampaign}
              startIcon={<PlayIcon />}
            >
              Launch Campaign
            </Button>
          ) : (
            <Button
              variant="contained"
              onClick={handleNext}
            >
              Next
            </Button>
          )}
        </DialogActions>
      </Dialog>
    </Box>
  );
}

export default Campaigns;