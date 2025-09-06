import React, { useState, useEffect, useRef } from 'react';
import './App.css';
import AceEditor from "react-ace";
import "ace-builds/src-noconflict/mode-python";
import "ace-builds/src-noconflict/theme-dracula"; 

function App() {
  const [fileName, setFileName] = useState('');
  const [file, setFile] = useState(null);
  const [showSuccess, setShowSuccess] = useState(false);
  const [uploadActive, setUploadActive] = useState(false);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState(null);
  const [codeId, setCodeId] = useState(null);
  const [validation, setValidation] = useState(null);
  const [templateDownloading, setTemplateDownloading] = useState(false);


  // Code preview states
  const [generatedCode, setGeneratedCode] = useState('');
  const [displayedCode, setDisplayedCode] = useState('');
  const [isEditing, setIsEditing] = useState(false);
  const [isPaused, setIsPaused] = useState(false);
  const [isStopped, setIsStopped] = useState(false);
  const [showDownloadOptions, setShowDownloadOptions] = useState(false);
  const [copySuccess, setCopySuccess] = useState(false);
  const [isCodeEdited, setIsCodeEdited] = useState(false);
  const [typewriterEnabled, setTypewriterEnabled] = useState(true);

  // SSE Connection states
  const [connectionStatus, setConnectionStatus] = useState('disconnected');
  const [reconnectAttempts, setReconnectAttempts] = useState(0);
  const maxReconnectAttempts = 3;

  // Refs
  const logsEndRef = useRef(null);
  const typewriterRef = useRef(null);
  const agentSectionRef = useRef(null);
  const codePreviewSectionRef = useRef(null);
  const eventSourceRef = useRef(null);
  const reconnectTimeoutRef = useRef(null);

  const [agents, setAgents] = useState([
    { name: 'Config Parser Agent', description: 'Analyzing mapping document structure and extracting configuration', icon: '📄', status: 'pending' },
    { name: 'Code Generation Agent', description: 'Generating code based on parsed configuration and mappings', icon: '⚙️', status: 'pending' },
    { name: 'Validation Agent', description: 'Validating generated code and ensuring quality standards', icon: '✓', status: 'pending' },
    { name: 'Testing Agent', description: 'Running unit tests and verifying code functionality', icon: '🧪', status: 'pending' }
  ]);

  const [agentLogs, setAgentLogs] = useState([]);

  // Smooth scroll function with offset for better visibility
  const smoothScrollToRef = (ref, offset = 100) => {
    if (ref && ref.current) {
      const element = ref.current;
      const elementPosition = element.getBoundingClientRect().top + window.pageYOffset;
      const offsetPosition = elementPosition - offset;

      window.scrollTo({
        top: offsetPosition,
        behavior: 'smooth'
      });
    }
  };

  // Enhanced typewriter effect that actually works
  useEffect(() => {
    if (generatedCode && !isEditing && !isPaused && !isStopped && typewriterEnabled) {
      let currentIndex = displayedCode.length;

      if (currentIndex < generatedCode.length) {
        typewriterRef.current = setTimeout(() => {
          const charsToAdd = Math.min(10, generatedCode.length - currentIndex); // Add more chars at once
          setDisplayedCode(generatedCode.substring(0, currentIndex + charsToAdd));
        }, 20); // Faster typing
      }
    }

    return () => {
      if (typewriterRef.current) {
        clearTimeout(typewriterRef.current);
      }
    };
  }, [generatedCode, displayedCode, isEditing, isPaused, isStopped, typewriterEnabled]);

  // Auto-scroll logs to bottom when new entries are added
  useEffect(() => {
    if (logsEndRef.current) {
      const logsContainer = logsEndRef.current.parentElement;
      logsContainer.scrollTop = logsContainer.scrollHeight;
    }
  }, [agentLogs]);

  // Reset displayed code when new generation starts
  useEffect(() => {
    if (loading) {
      setDisplayedCode('');
      setGeneratedCode('');
      setIsPaused(false);
      setIsStopped(false);
      setIsCodeEdited(false);
      setTypewriterEnabled(true);
    }
  }, [loading]);

  // Cleanup on unmount
  useEffect(() => {
    return () => {
      if (eventSourceRef.current) {
        eventSourceRef.current.close();
      }
      if (reconnectTimeoutRef.current) {
        clearTimeout(reconnectTimeoutRef.current);
      }
      if (typewriterRef.current) {
        clearTimeout(typewriterRef.current);
      }
    };
  }, []);

  // Function to download sample configuration template from backend
  const handleDownloadSampleConfig = async () => {
    if (templateDownloading) return;
    
    try {
      console.log('Downloading template from backend...');
      setTemplateDownloading(true);
      setError(null);
      
      const response = await fetch('http://localhost:5000/api/download-template', {
        method: 'GET',
      });

      if (!response.ok) {
        // Try to get error message from response
        const contentType = response.headers.get("content-type");
        if (contentType && contentType.indexOf("application/json") !== -1) {
          const errorData = await response.json();
          throw new Error(errorData.error || 'Failed to download template');
        } else {
          throw new Error(`Server error: ${response.status}`);
        }
      }

      // Get the blob from response
      const blob = await response.blob();
      
      // Validate blob size
      if (blob.size === 0) {
        throw new Error('Downloaded file is empty');
      }
      
      // Create download link
      const url = window.URL.createObjectURL(blob);
      const a = document.createElement('a');
      a.href = url;
      a.download = 'Config_Template.xlsx';
      document.body.appendChild(a);
      a.click();
      
      // Cleanup
      setTimeout(() => {
        window.URL.revokeObjectURL(url);
        document.body.removeChild(a);
      }, 100);
      
      console.log('Template downloaded successfully');
      
      // Show success feedback
      const originalText = 'Download Template';
      const successButton = document.querySelector('.template-download-btn');
      if (successButton) {
        successButton.textContent = 'Downloaded!';
        successButton.style.backgroundColor = '#059669';
        setTimeout(() => {
          successButton.textContent = originalText;
          successButton.style.backgroundColor = '#10B981';
        }, 2000);
      }
      
    } catch (error) {
      console.error('Error downloading template:', error);
      setError(`Failed to download template: ${error.message}`);
      
      // Provide fallback message
      if (error.message.includes('Template file not found')) {
        setError('Template file not found on server. Please contact support.');
      }
    } finally {
      setTemplateDownloading(false);
    }
  };

  const handleFileSelect = (file) => {
    if (file) {
      // Only allow Excel files
      const validExtensions = ['.xlsx', '.xls'];
      const fileExtension = file.name.substring(file.name.lastIndexOf('.')).toLowerCase();

      if (!validExtensions.includes(fileExtension)) {
        setError(`Invalid file type. Please upload Excel files only (.xlsx, .xls)`);
        return;
      }

      setFileName(file.name);
      setFile(file);
      setUploadActive(true);
      setError(null);
    }
  };

  const handleFileInputChange = (e) => {
    const file = e.target.files[0];
    handleFileSelect(file);
  };

  const handleDragOver = (e) => {
    e.preventDefault();
    setUploadActive(true);
  };

  const handleDragLeave = () => {
    setUploadActive(false);
  };

  const handleDrop = (e) => {
    e.preventDefault();
    const file = e.dataTransfer.files[0];
    handleFileSelect(file);
  };

  const handleUploadClick = () => {
    const input = document.createElement('input');
    input.type = 'file';
    // Only accept Excel files
    input.accept = '.xlsx,.xls';
    input.onchange = handleFileInputChange;
    input.click();
  };

  // Enhanced fetchGeneratedCode with better error handling and immediate display
  const fetchGeneratedCode = async (codeId) => {
    console.log('🔍 Fetching full generated code for:', codeId);
    
    try {
      const response = await fetch('http://localhost:5000/api/code-preview', {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
        },
        body: JSON.stringify({ code_id: codeId }),
      });

      console.log('📡 Code preview response status:', response.status);

      if (!response.ok) {
        throw new Error(`HTTP ${response.status}`);
      }

      const data = await response.json();
      console.log('📊 Code preview data success:', data.success);

      if (data.success && data.code) {
        console.log('✅ Full code received, length:', data.code.length);
        console.log('📝 First 200 characters:', data.code.substring(0, 200));
        
        // Set the complete code immediately
        setGeneratedCode(data.code);
        
        // Decide whether to use typewriter effect or show immediately
        if (data.code.length > 1000) {
          console.log('🚀 Large code detected, showing immediately');
          setDisplayedCode(data.code);
          setTypewriterEnabled(false);
          setIsStopped(true);
        } else {
          console.log('⏳ Starting typewriter effect');
          setDisplayedCode('');
          setTypewriterEnabled(true);
        }
        
      } else {
        console.error('❌ Code preview failed:', data.error || 'No code returned');
        setError(data.error || 'Failed to fetch generated code');
      }
    } catch (err) {
      console.error('💥 Error fetching code preview:', err);
      setError('Network error while fetching code: ' + err.message);
    }
  };

  // Enhanced SSE connection setup with better code handling
  const setupSSEConnection = (processId, isReconnect = false) => {
    if (eventSourceRef.current) {
      eventSourceRef.current.close();
    }

    if (!isReconnect) {
      setConnectionStatus('connecting');
    }

    console.log(`${isReconnect ? '🔄 Reconnecting' : '🔌 Connecting'} to SSE for process:`, processId);

    const eventSource = new EventSource(`http://localhost:5000/api/process/${processId}/events`);
    eventSourceRef.current = eventSource;

    // Connection opened
    eventSource.onopen = (event) => {
      console.log('✅ SSE connection opened');
      setConnectionStatus('connected');
      setReconnectAttempts(0);
      if (reconnectTimeoutRef.current) {
        clearTimeout(reconnectTimeoutRef.current);
        reconnectTimeoutRef.current = null;
      }
    };

    // Handle messages
    eventSource.onmessage = (event) => {
      try {
        const update = JSON.parse(event.data);
        console.log('📨 Received SSE update:', update.stage, update.status);

        // Handle different event types
        if (update.type === 'connected') {
          setConnectionStatus('connected');
          return;
        }

        if (update.type === 'heartbeat') {
          console.log('💗 Heartbeat received');
          return;
        }

        if (update.type === 'error') {
          console.error('🚨 Server error:', update.message);
          setError(`Server error: ${update.message}`);
          return;
        }

        // Handle normal progress updates
        const currentTime = new Date().toLocaleTimeString('en-US', { hour12: false });

        // Update agents based on stage
        setAgents(prevAgents => {
          const newAgents = [...prevAgents];

          switch (update.stage) {
            case 'parsing':
              newAgents[0].status = update.status;
              break;
            case 'generation':
              if (update.status === 'running') {
                newAgents[0].status = 'completed';
              }
              newAgents[1].status = update.status;
              
              // Handle code preview from generation stage
              if (update.code_preview) {
                console.log('🎁 Received code preview in SSE:', update.code_preview.length, 'chars');
                setGeneratedCode(update.code_preview);
                // Start typewriter for preview
                setDisplayedCode('');
                setTypewriterEnabled(true);
              }
              break;
            case 'validation':
              if (update.status === 'running') {
                newAgents[1].status = 'completed';
              }
              newAgents[2].status = update.status;
              break;
            case 'testing':
              if (update.status === 'running') {
                newAgents[2].status = update.status === 'failed' ? 'failed' : 'completed';
              }
              newAgents[3].status = update.status;
              break;
            case 'complete':
              newAgents[3].status = 'completed';
              break;
            default:
              break;
          }

          return newAgents;
        });

        // Add log entry (skip heartbeat and duplicate messages)
        if (update.message && !update.message.includes('Still processing')) {
          setAgentLogs(prevLogs => [...prevLogs, {
            time: currentTime,
            agent: getAgentNameFromStage(update.stage),
            message: update.message,
            type: update.status === 'failed' ? 'error' : update.status === 'completed' ? 'success' : 'info'
          }]);
        }

        // Handle completion - CRITICAL: Always fetch full code here
        if (update.stage === 'complete') {
          console.log('🎉 Process completed! Fetching full code...');
          setCodeId(update.code_id);
          setValidation(update.validation);
          setShowSuccess(true);
          setLoading(false);
          setConnectionStatus('completed');

          // CRITICAL: Always fetch the complete generated code
          setTimeout(() => {
            fetchGeneratedCode(update.code_id);
          }, 100);

          // Auto-scroll to Code Preview section after successful completion
          setTimeout(() => {
            smoothScrollToRef(codePreviewSectionRef, 80);
          }, 1000);

          eventSource.close();

          setTimeout(() => {
            setShowSuccess(false);
          }, 5000);
        }

        // Handle errors
        if (update.status === 'failed' && update.stage !== 'validation') {
          setError(`Failed at ${update.stage}: ${update.message}`);
          setLoading(false);
          setConnectionStatus('failed');
          eventSource.close();
        }

      } catch (parseError) {
        console.error('💥 Error parsing SSE data:', parseError, event.data);
      }
    };

    // Handle connection errors with retry logic
    eventSource.onerror = (error) => {
      console.error('🚨 SSE connection error:', error);
      setConnectionStatus('error');
      
      eventSource.close();

      // Only retry if we haven't exceeded max attempts and still loading
      if (reconnectAttempts < maxReconnectAttempts && loading) {
        const nextAttempt = reconnectAttempts + 1;
        setReconnectAttempts(nextAttempt);
        
        const retryDelay = Math.min(1000 * Math.pow(2, nextAttempt - 1), 10000);
        console.log(`🔄 Attempting to reconnect in ${retryDelay}ms (attempt ${nextAttempt}/${maxReconnectAttempts})`);
        
        reconnectTimeoutRef.current = setTimeout(() => {
          setupSSEConnection(processId, true);
        }, retryDelay);
      } else {
        // Max retries reached or not loading anymore
        if (loading) {
          setError('Connection lost after multiple retry attempts. Please refresh the page and try again.');
          setLoading(false);
        }
        setConnectionStatus('failed');
      }
    };
  };

  const uploadAndGenerate = async () => {
    if (!file) {
      setError('Please select a file first');
      return;
    }

    console.log('🚀 Starting upload and generation process');

    setLoading(true);
    setError(null);
    setShowSuccess(false);
    setGeneratedCode('');
    setDisplayedCode('');
    setIsCodeEdited(false);
    setConnectionStatus('connecting');
    setReconnectAttempts(0);
    setTypewriterEnabled(true);

    // Reset agents to pending state
    setAgents(prevAgents => prevAgents.map(agent => ({ ...agent, status: 'pending' })));
    setAgentLogs([]);

    // Auto-scroll to Agent Pipeline section when generation starts
    setTimeout(() => {
      smoothScrollToRef(agentSectionRef, 80);
    }, 100);

    const formData = new FormData();
    formData.append('file', file);

    try {
      const response = await fetch('http://localhost:5000/api/upload-config', {
        method: 'POST',
        body: formData,
      });

      const data = await response.json();
      console.log('📤 Upload response:', data);

      if (data.success && data.process_id) {
        console.log('✅ Upload successful, setting up SSE connection');
        setupSSEConnection(data.process_id);
      } else {
        setError(data.error || 'Failed to start processing');
        setLoading(false);
        setConnectionStatus('disconnected');
      }
    } catch (err) {
      console.error('💥 Network error:', err);
      setError('Network error: ' + err.message);
      setLoading(false);
      setConnectionStatus('failed');
    }
  };

  // Helper function to map stage to agent name
  const getAgentNameFromStage = (stage) => {
    switch (stage) {
      case 'parsing': return 'Config Parser';
      case 'generation': return 'Code Generator';
      case 'validation': return 'Validation';
      case 'testing': return 'Testing';
      case 'complete': return 'Pipeline';
      default: return 'System';
    }
  };

  // Handle copy to clipboard
  const handleCopyCode = () => {
    const codeToCopy = displayedCode || generatedCode;
    if (codeToCopy) {
      navigator.clipboard.writeText(codeToCopy).then(() => {
        setCopySuccess(true);
        setTimeout(() => setCopySuccess(false), 2000);
      });
    }
  };

  // Handle edit toggle
  const handleEditToggle = () => {
    if (!isEditing) {
      setDisplayedCode(generatedCode);
      setIsEditing(true);
      setTypewriterEnabled(false);
    } else {
      setIsEditing(false);
    }
  };

  // Handle pause/resume
  const handlePauseResume = () => {
    setIsPaused(!isPaused);
  };

  // Handle stop
  const handleStop = () => {
    console.log('⏹️ Stopping typewriter effect, showing full code');
    setIsStopped(true);
    setDisplayedCode(generatedCode);
    setTypewriterEnabled(false);
    if (typewriterRef.current) {
      clearTimeout(typewriterRef.current);
    }
  };

  // Handle code changes in edit mode
  const handleCodeChange = (newValue) => {
    setDisplayedCode(newValue);
    setIsCodeEdited(true);
  };

  // Download functions
  const handleDownloadIPYNB = async () => {
    if (!codeId) {
      setError('Please generate code first');
      return;
    }

    if (isCodeEdited) {
      const choice = window.confirm(
        'You have edited the code. Click OK to download the edited version, or Cancel to download the original version.'
      );
      
      if (choice) {
        const blob = new Blob([displayedCode], { type: 'text/plain' });
        const url = window.URL.createObjectURL(blob);
        const a = document.createElement('a');
        a.href = url;
        a.download = 'etl_pipeline_edited.ipynb';
        document.body.appendChild(a);
        a.click();
        window.URL.revokeObjectURL(url);
        document.body.removeChild(a);
        return;
      }
    }

    try {
      const response = await fetch('http://localhost:5000/api/download/ipynb', {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
        },
        body: JSON.stringify({ code_id: codeId }),
      });

      if (response.ok) {
        const blob = await response.blob();
        const url = window.URL.createObjectURL(blob);
        const a = document.createElement('a');
        a.href = url;
        a.download = 'etl_pipeline.ipynb';
        document.body.appendChild(a);
        a.click();
        window.URL.revokeObjectURL(url);
        document.body.removeChild(a);
      } else {
        setError('Failed to download notebook');
      }
    } catch (err) {
      setError('Download error: ' + err.message);
    }
  };

  const handleDownloadPY = async () => {
    if (!codeId) {
      setError('Please generate code first');
      return;
    }

    if (isCodeEdited) {
      const choice = window.confirm(
        'You have edited the code. Click OK to download the edited version, or Cancel to download the original version.'
      );
      
      if (choice) {
        const blob = new Blob([displayedCode], { type: 'text/x-python' });
        const url = window.URL.createObjectURL(blob);
        const a = document.createElement('a');
        a.href = url;
        a.download = 'etl_pipeline_edited.py';
        document.body.appendChild(a);
        a.click();
        window.URL.revokeObjectURL(url);
        document.body.removeChild(a);
        return;
      }
    }

    try {
      const response = await fetch('http://localhost:5000/api/download/py', {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
        },
        body: JSON.stringify({ code_id: codeId }),
      });

      if (response.ok) {
        const blob = await response.blob();
        const url = window.URL.createObjectURL(blob);
        const a = document.createElement('a');
        a.href = url;
        a.download = 'etl_pipeline.py';
        document.body.appendChild(a);
        a.click();
        window.URL.revokeObjectURL(url);
        document.body.removeChild(a);
      } else {
        setError('Failed to download Python file');
      }
    } catch (err) {
      setError('Download error: ' + err.message);
    }
  };

  // Calculate pipeline status
  const getPipelineStatus = () => {
    const completed = agents.filter(a => a.status === 'completed').length;
    const running = agents.filter(a => a.status === 'running').length;
    const pending = agents.filter(a => a.status === 'pending').length;
    const failed = agents.filter(a => a.status === 'failed').length;

    return { completed, running, pending, failed, total: agents.length };
  };

  const pipelineStatus = getPipelineStatus();

  // Connection status indicator component
  const ConnectionStatus = () => {
    const getStatusColor = () => {
      switch (connectionStatus) {
        case 'connected': return '#10B981';
        case 'connecting': return '#F59E0B';
        case 'error': return '#EF4444';
        case 'completed': return '#10B981';
        case 'failed': return '#EF4444';
        default: return '#6B7280';
      }
    };

    const getStatusText = () => {
      switch (connectionStatus) {
        case 'connected': return 'Connected';
        case 'connecting': return 'Connecting...';
        case 'error': return `Connection Error (Retry ${reconnectAttempts}/${maxReconnectAttempts})`;
        case 'completed': return 'Completed';
        case 'failed': return 'Connection Failed';
        default: return 'Disconnected';
      }
    };

    if (connectionStatus === 'disconnected') return null;

    return (
      <div className="connection-status" style={{
        position: 'fixed',
        top: '20px',
        right: '20px',
        backgroundColor: 'rgba(0, 0, 0, 0.8)',
        color: 'white',
        padding: '8px 12px',
        borderRadius: '4px',
        fontSize: '12px',
        zIndex: 1000,
        display: 'flex',
        alignItems: 'center',
        gap: '8px'
      }}>
        <div style={{
          width: '8px',
          height: '8px',
          borderRadius: '50%',
          backgroundColor: getStatusColor()
        }}></div>
        {getStatusText()}
      </div>
    );
  };

  // Enhanced error display with retry option
  const ErrorDisplay = () => {
    if (!error) return null;

    const canRetry = connectionStatus === 'failed' && file;

    return (
      <div className="error-message">
        <span>✕</span> 
        {error}
        {canRetry && (
          <button 
            onClick={() => uploadAndGenerate()} 
            style={{
              marginLeft: '10px',
              padding: '4px 8px',
              fontSize: '12px',
              backgroundColor: '#EF4444',
              color: 'white',
              border: 'none',
              borderRadius: '3px',
              cursor: 'pointer'
            }}
          >
            Retry
          </button>
        )}
      </div>
    );
  };

  // Force full code display button
  const handleShowFullCode = () => {
    console.log('🎯 Force showing full code');
    if (generatedCode) {
      setDisplayedCode(generatedCode);
      setTypewriterEnabled(false);
      setIsStopped(true);
      setIsPaused(false);
    } else if (codeId) {
      fetchGeneratedCode(codeId);
    }
  };

  return (
    <div className="App">
      {/* Connection Status Indicator */}
      <ConnectionStatus />

      {/* Navigation */}
      <nav>
        <div className="nav-container">
          <img 
            src="https://reknew.ai/_next/static/media/reknew-logo-white.faee4a98.png" 
            alt="ReKnew" 
            className="logo-image"
          />
        </div>
      </nav>

      {/* Main Container */}
      <div className="container">
        {/* Header */}
        <div className="header">
          <h1>Code Generator</h1>
          <p>Powered by ReKnew - Oracle to Databricks ETL Pipeline Generator</p>
        </div>

        {/* Upload Section */}
        <div className="upload-section">
          <div className="upload-container">
            <div
              className={`upload-area ${uploadActive ? 'active' : ''}`}
              onClick={handleUploadClick}
              onDragOver={handleDragOver}
              onDragLeave={handleDragLeave}
              onDrop={handleDrop}
            >
              <div className="upload-content">
                {/* Help section on the left side */}
                <div style={{
                  backgroundColor: 'rgba(255, 255, 255, 0.03)',
                  border: '1px solid rgba(255, 255, 255, 0.1)',
                  padding: '12px',
                  borderRadius: '6px',
                  width: '160px',
                  marginRight: '1.5rem',
                  flexShrink: 0
                }}>
                  <div style={{ 
                    fontSize: '12px', 
                    color: 'rgba(255, 255, 255, 0.5)', 
                    marginBottom: '8px',
                    lineHeight: '1.3'
                  }}>
                    Need help with configuration file?
                  </div>
                  <button
                    onClick={(e) => {
                      e.stopPropagation();
                      handleDownloadSampleConfig();
                    }}
                    style={{
                      backgroundColor: '#10B981',
                      color: 'white',
                      border: 'none',
                      padding: '5px 10px',
                      borderRadius: '4px',
                      fontSize: '11px',
                      cursor: 'pointer',
                      width: '100%',
                      fontWeight: '500',
                      letterSpacing: '0.3px',
                      textTransform: 'uppercase',
                      transition: 'all 0.3s ease'
                    }}
                    onMouseEnter={(e) => {
                      e.target.style.backgroundColor = '#059669';
                      e.target.style.transform = 'translateY(-1px)';
                    }}
                    onMouseLeave={(e) => {
                      e.target.style.backgroundColor = '#10B981';
                      e.target.style.transform = 'translateY(0)';
                    }}
                  >
                    Download Template
                  </button>
                </div>

                {/* Main upload content */}
                <div style={{ display: 'flex', alignItems: 'center', gap: '2rem', flex: 1 }}>
                  <div className="upload-icon-section">
                    <svg width="40" height="40" viewBox="0 0 24 24" fill="none" stroke="#FF6B4A" strokeWidth="2">
                      <path d="M21 15v4a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2v-4"/>
                      <polyline points="17 8 12 3 7 8"/>
                      <line x1="12" y1="3" x2="12" y2="15"/>
                    </svg>
                  </div>
                  <div className="upload-text-section">
                    <h3>Drop your Excel configuration file here or click to browse</h3>
                    <p>Upload your ETL configuration Excel file to generate optimized PySpark code</p>
                    
                    <div className="file-formats">
                      <span className="format-badge">EXCEL (.xlsx)</span>
                      <span className="format-badge">EXCEL (.xls)</span>
                    </div>
                    {fileName && (
                      <div className="file-name">
                        <span>📁</span> {fileName}
                      </div>
                    )}
                  </div>
                </div>
              </div>
            </div>
            <div className="action-section">
              <button
                className="btn-generate"
                onClick={uploadAndGenerate}
                disabled={loading || !file}
              >
                {loading ? 'Generating...' : 'Generate Code'}
              </button>
              {showSuccess && (
                <div className="success-message">
                  <span>✓</span> Generated successfully
                </div>
              )}
              <ErrorDisplay />
            </div>
          </div>
        </div>

        {/* Combined Agent Section with Pipeline Status */}
        <div className="combined-agent-section" ref={agentSectionRef}>
          <div className="sticky-container">
            {/* Agent Pipeline & Status Combined */}
            <div className="agent-status-card">
              <h3 className="card-title">Data Analyst Agent Pipeline</h3>
              
              {/* Pipeline Status Grid */}
              <div className="mini-status-grid">
                <div className="mini-status-item completed">
                  <span className="mini-status-number">{pipelineStatus.completed}</span>
                  <span className="mini-status-label">Completed</span>
                </div>
                <div className="mini-status-item running">
                  <span className="mini-status-number">{pipelineStatus.running}</span>
                  <span className="mini-status-label">Running</span>
                </div>
                <div className="mini-status-item pending">
                  <span className="mini-status-number">{pipelineStatus.pending}</span>
                  <span className="mini-status-label">Pending</span>
                </div>
                <div className="mini-status-item failed">
                  <span className="mini-status-number">{pipelineStatus.failed}</span>
                  <span className="mini-status-label">Failed</span>
                </div>
              </div>

              {/* Agent List */}
              <div className="compact-agent-list">
                {agents.map((agent, index) => (
                  <div key={index} className={`compact-agent-item status-${agent.status}`}>
                    <div className="compact-agent-icon">{agent.icon}</div>
                    <div className="compact-agent-info">
                      <div className="compact-agent-name">{agent.name}</div>
                      <div className="compact-agent-desc">{agent.description}</div>
                    </div>
                    <span className={`compact-agent-status status-${agent.status}`}>
                      {agent.status === 'running' ? '⚡' : agent.status === 'completed' ? '✓' : agent.status === 'failed' ? '✕' : '⏳'}
                    </span>
                  </div>
                ))}
              </div>
            </div>

            {/* Agent Outputs */}
            <div className="agent-outputs-card">
              <h3 className="card-title">Agent Outputs</h3>
              <div className="logs-container">
                {agentLogs.length > 0 ? (
                  <>
                    {agentLogs.map((log, index) => (
                      <div key={index} className={`log-entry log-${log.type}`} style={{'--index': index}}>
                        <span className="log-time">{log.time}</span>
                        <span className="log-agent">[{log.agent}]</span>
                        <span className="log-message">{log.message}</span>
                      </div>
                    ))}
                    <div ref={logsEndRef} />
                  </>
                ) : (
                  <div className="log-entry">
                    <span className="log-message" style={{color: '#666'}}>
                      Waiting for Excel configuration file upload...
                    </span>
                  </div>
                )}
              </div>
            </div>
          </div>
        </div>

        {/* Code Preview Section */}
        <div className="code-preview-section" ref={codePreviewSectionRef}>
          <div className="code-header">
            <h3>Generated ETL Pipeline Code</h3>
            <div className="code-actions">
              {/* Show Full Code Button */}
              <button
                className="code-action-btn"
                onClick={handleShowFullCode}
                disabled={!generatedCode && !codeId}
                title="Show Full Code"
                style={{backgroundColor: '#10B981'}}
              >
                <svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2">
                  <polyline points="6 9 12 15 18 9"/>
                </svg>
              </button>

              {/* Edit / Save */}
              <button
                className={`code-action-btn ${isEditing ? 'active' : ''}`}
                onClick={handleEditToggle}
                disabled={!generatedCode}
                title={isEditing ? "Save Changes" : "Edit Code"}
              >
                {isEditing ? (
                  <svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2">
                    <path d="M19 21H5a2 2 0 0 1-2 2V5a2 2 0 0 1 2-2h11l5 5v11a2 2 0 0 1-2 2z"/>
                    <polyline points="17 21 17 13 7 13 7 21"/>
                    <polyline points="7 3 7 8 15 8"/>
                  </svg>
                ) : (
                  <svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2">
                    <path d="M11 4H4a2 2 0 0 0-2 2v14a2 2 0 0 0 2 2h14a2 2 0 0 0 2-2v-7"/>
                    <path d="M18.5 2.5a2.121 2.121 0 0 1 3 3L12 15l-4 1 1-4 9.5-9.5z"/>
                  </svg>
                )}
              </button>

              {/* Pause / Resume */}
              <button
                className={`code-action-btn ${isPaused ? 'active' : ''}`}
                onClick={handlePauseResume}
                disabled={!typewriterEnabled || isEditing}
                title={isPaused ? "Resume" : "Pause"}
              >
                {isPaused ? (
                  <svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2">
                    <polygon points="5 3 19 12 5 21 5 3"/>
                  </svg>
                ) : (
                  <svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2">
                    <rect x="6" y="4" width="4" height="16"/>
                    <rect x="14" y="4" width="4" height="16"/>
                  </svg>
                )}
              </button>

              {/* Stop */}
              <button
                className="code-action-btn"
                onClick={handleStop}
                disabled={!typewriterEnabled || isEditing}
                title="Stop & Show Full Code"
              >
                <svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2">
                  <rect x="6" y="6" width="12" height="12"/>
                </svg>
              </button>

              {/* Copy */}
              <button
                className={`code-action-btn ${copySuccess ? 'success' : ''}`}
                onClick={handleCopyCode}
                disabled={!generatedCode && !displayedCode}
                title={copySuccess ? "Copied!" : "Copy Code"}
              >
                {copySuccess ? (
                  <svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="#10B981" strokeWidth="2">
                    <polyline points="20 6 9 17 4 12"/>
                  </svg>
                ) : (
                  <svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2">
                    <rect x="9" y="9" width="13" height="13" rx="2" ry="2"/>
                    <path d="M5 15H4a2 2 0 0 1-2 2V4a2 2 0 0 1 2-2h9a2 2 0 0 1 2 2v1"/>
                  </svg>
                )}
              </button>

              {/* Download Dropdown */}
              <div className="download-dropdown">
                <button
                  className="code-action-btn"
                  onClick={() => setShowDownloadOptions(!showDownloadOptions)}
                  disabled={!codeId}
                  title="Download Code"
                >
                  <svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2">
                    <path d="M21 15v4a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2v-4"/>
                    <polyline points="7 10 12 15 17 10"/>
                    <line x1="12" y1="15" x2="12" y2="3"/>
                  </svg>
                </button>
                {showDownloadOptions && (
                  <div className="download-options">
                    <button onClick={handleDownloadPY} className="download-option">
                      Download as .py
                    </button>
                    <button onClick={handleDownloadIPYNB} className="download-option">
                      Download as .ipynb
                    </button>
                  </div>
                )}
              </div>
            </div>
          </div>

          {/* Code Container */}
          <div className="code-container">
            {(displayedCode || loading) ? (
              <>
                <AceEditor
                  mode="python"
                  theme="dracula" 
                  name="etl-code-editor"
                  value={displayedCode}
                  onChange={handleCodeChange}
                  readOnly={!isEditing}
                  fontSize={14}
                  width="100%"
                  height="400px"
                  setOptions={{
                    showLineNumbers: true,
                    tabSize: 2,
                    useWorker: false
                  }}
                />
                {/* Code Status Indicator */}
                <div style={{
                  position: 'absolute',
                  bottom: '10px',
                  right: '10px',
                  background: 'rgba(0,0,0,0.7)',
                  color: 'white',
                  padding: '4px 8px',
                  borderRadius: '4px',
                  fontSize: '12px'
                }}>
                  {displayedCode.length} / {generatedCode.length} chars
                  {typewriterEnabled && !isStopped && ' (typing...)'}
                </div>
              </>
            ) : (
              <div className="code-placeholder">
                Upload an Excel configuration file to generate ETL pipeline code
              </div>
            )}
          </div>
        </div>

        {/* Validation Details */}
        {validation && (
          <div className="validation-section">
            <h2 className="section-title">Validation Details</h2>
            <div className="validation-grid">
              {Object.entries(validation).map(([check, result], index) => (
                <div key={index} className="validation-item">
                  <span className="validation-check">{check}</span>
                  <span className={`validation-status ${result.status.includes('PASS') ? 'pass' : result.status.includes('WARNING') ? 'warning' : 'fail'}`}>
                    {result.status}
                  </span>
                  <span className="validation-details">{result.details}</span>
                </div>
              ))}
            </div>
          </div>
        )}
      </div>
    </div>
  );
}

export default App;