#!/usr/bin/env python3
"""
Production Flask API Server for ETL Pipeline Generator
Fixed version with SSE stability and template download support
"""

import os
import json
import uuid
import threading
import time
from datetime import datetime
from pathlib import Path
from queue import Queue, Empty
from typing import Dict, Any, Optional
from flask import Flask, request, jsonify, send_file, Response
from flask_cors import CORS
from werkzeug.utils import secure_filename
import tempfile

# Import your ETL pipeline components
from lang import run_etl_pipeline

# Flask app setup
app = Flask(__name__)
CORS(app, origins=["http://localhost:3000"])

# Configuration
UPLOAD_FOLDER = Path("uploads")
GENERATED_FOLDER = Path("generated_etl")
TEMPLATE_FOLDER = Path(__file__).parent / "template"

# Create necessary directories
UPLOAD_FOLDER.mkdir(exist_ok=True)
GENERATED_FOLDER.mkdir(exist_ok=True)
TEMPLATE_FOLDER.mkdir(exist_ok=True)

app.config['UPLOAD_FOLDER'] = str(UPLOAD_FOLDER)
app.config['MAX_CONTENT_LENGTH'] = 16 * 1024 * 1024  # 16MB max file size

# Allowed file extensions
ALLOWED_EXTENSIONS = {'.yaml', '.yml', '.json', '.xml', '.xaml', '.xlsx', '.xls'}

def allowed_file(filename):
    """Check if file extension is allowed"""
    return Path(filename).suffix.lower() in ALLOWED_EXTENSIONS

class ProcessManager:
    """ProcessManager with enhanced SSE handling"""
    
    def __init__(self):
        self.processes = {}
        self.event_queues = {}
        self.active_connections = {}
        print("ProcessManager initialized")
    
    def create_process(self, process_id: str, config_file: str, schema_file: Optional[str] = None):
        """Create a new ETL generation process"""
        print(f"Creating process: {process_id}")
        self.processes[process_id] = {
            'id': process_id,
            'status': 'created',
            'config_file': config_file,
            'schema_file': schema_file,
            'start_time': datetime.now(),
            'current_stage': 'initialized',
            'generated_code': '',
            'validation_report': {},
            'test_report': [],
            'export_files': {},
            'errors': [],
            'last_heartbeat': datetime.now()
        }
        self.event_queues[process_id] = Queue()
        print(f"Process {process_id} created successfully")
        return self.processes[process_id]
    
    def update_process(self, process_id: str, stage: str, status: str, message: str, **kwargs):
        """Update process status and emit event"""
        print(f"Update: {process_id[:8]}... -> {stage} ({status}): {message}")
        
        if process_id in self.processes:
            self.processes[process_id].update({
                'current_stage': stage,
                'status': status,
                'last_update': datetime.now(),
                'last_heartbeat': datetime.now(),
                **kwargs
            })
            
            # Create event data
            event_data = {
                'process_id': process_id,
                'stage': stage,
                'status': status,
                'message': message,
                'timestamp': datetime.now().isoformat(),
                **kwargs
            }
            
            # Add to event queue
            if process_id in self.event_queues:
                try:
                    self.event_queues[process_id].put(event_data, timeout=1)
                    print(f"Event queued: {stage}")
                except:
                    print(f"Failed to queue event for {stage}")
        else:
            print(f"Process {process_id[:8]}... not found!")
    
    def get_events(self, process_id: str):
        """Generator for server-sent events"""
        print(f"Starting event stream for: {process_id[:8]}...")
        
        if process_id not in self.event_queues:
            print(f"No event queue found")
            return
            
        queue = self.event_queues[process_id]
        last_heartbeat = time.time()
        connection_active = True
        
        # Mark connection as active
        self.active_connections[process_id] = True
        print(f"Connection marked as active")
        
        try:
            while connection_active:
                try:
                    # Try to get event with timeout
                    event = queue.get(timeout=5)  # 5 second timeout
                    print(f"Sending: {event.get('stage', 'unknown')} - {event.get('status', 'unknown')}")
                    yield f"data: {json.dumps(event)}\n\n"
                    
                    # Stop if process is complete or failed
                    if event.get('stage') == 'complete' or event.get('status') == 'failed':
                        print(f"Process completed/failed, closing connection")
                        connection_active = False
                        break
                        
                except Empty:
                    # Send heartbeat
                    current_time = time.time()
                    if current_time - last_heartbeat > 8:  # Heartbeat every 8 seconds
                        heartbeat = {
                            'type': 'heartbeat',
                            'timestamp': datetime.now().isoformat(),
                            'process_id': process_id
                        }
                        yield f"data: {json.dumps(heartbeat)}\n\n"
                        last_heartbeat = current_time
                        print(f"Heartbeat sent")
                    
                    # Check if process still exists
                    if process_id not in self.processes:
                        print(f"Process no longer exists")
                        connection_active = False
                        break
                
        except Exception as e:
            print(f"SSE error: {e}")
            connection_active = False
        finally:
            # Clean up connection tracking
            self.active_connections.pop(process_id, None)
            print(f"SSE connection closed")
    
    def is_connection_active(self, process_id: str) -> bool:
        """Check if SSE connection is still active"""
        return process_id in self.active_connections
    
    def cleanup_process(self, process_id: str):
        """Clean up process resources"""
        print(f"Cleaning up: {process_id[:8]}...")
        self.processes.pop(process_id, None)
        self.event_queues.pop(process_id, None)
        self.active_connections.pop(process_id, None)

# Global process manager
process_manager = ProcessManager()

def run_etl_pipeline_with_progress(process_id: str, config_file: str, schema_file: Optional[str] = None):
    """Run ETL pipeline with real-time progress tracking"""
    print(f"Starting ETL for: {process_id[:8]}...")
    print(f"Config: {config_file}")
    
    try:
        # Step 1: Parsing
        process_manager.update_process(
            process_id, 
            'parsing', 
            'running', 
            'Parsing configuration files...'
        )
        time.sleep(1)  # Brief pause for UI update
        
        # Step 2: Generation (this is where the real work happens)
        process_manager.update_process(
            process_id, 
            'generation', 
            'running', 
            'Generating ETL code with AI agents...'
        )
        
        # Actually run the ETL pipeline
        print("Calling run_etl_pipeline...")
        final_state = run_etl_pipeline(
            config_file=config_file,
            schema_file=schema_file,
            max_iterations=10
        )
        
        if final_state and final_state.get('generated_code'):
            generated_code = final_state.get('generated_code', '')
            print(f"Success: {len(generated_code)} chars generated")
            
            # Store results
            process_manager.processes[process_id].update({
                'generated_code': generated_code,
                'validation_report': final_state.get('validation_report', {}),
                'test_report': final_state.get('test_report', []),
                'export_files': final_state.get('export_files', {}),
                'errors': final_state.get('errors', [])
            })
            
            # Update progress through each stage
            process_manager.update_process(
                process_id, 
                'generation', 
                'completed', 
                f'Code generated successfully ({len(generated_code)} characters)',
                code_preview=generated_code[:500] + '...' if len(generated_code) > 500 else generated_code
            )
            time.sleep(0.5)
            
            # Validation update
            validation_report = final_state.get('validation_report', {})
            passed = sum(1 for v in validation_report.values() if 'PASS' in v.get('status', ''))
            total = len(validation_report)
            
            process_manager.update_process(
                process_id, 
                'validation', 
                'running', 
                'Running code validation...'
            )
            time.sleep(0.5)
            
            process_manager.update_process(
                process_id, 
                'validation', 
                'completed', 
                f'Validation complete: {passed}/{total} checks passed'
            )
            time.sleep(0.5)
            
            # Testing update
            test_report = final_state.get('test_report', [])
            test_passed = sum(1 for t in test_report if t.get('status') == 'PASS')
            test_total = len(test_report)
            
            process_manager.update_process(
                process_id, 
                'testing', 
                'running', 
                'Running test suite...'
            )
            time.sleep(0.5)
            
            process_manager.update_process(
                process_id, 
                'testing', 
                'completed', 
                f'Testing complete: {test_passed}/{test_total} tests passed'
            )
            time.sleep(0.5)
            
            # Completion
            process_manager.update_process(
                process_id,
                'complete',
                'completed',
                'ETL pipeline generation completed successfully!',
                code_id=process_id,
                validation=validation_report
            )
            
            print(f"Pipeline completed for: {process_id[:8]}...")
            
        else:
            print("Pipeline failed or returned empty")
            process_manager.update_process(
                process_id, 
                'generation', 
                'failed', 
                'Code generation failed - please check configuration'
            )
            
    except Exception as e:
        error_msg = f'Error: {str(e)}'
        print(f"Pipeline error: {error_msg}")
        import traceback
        traceback.print_exc()
        
        process_manager.update_process(
            process_id,
            'error',
            'failed',
            error_msg
        )

# ==================== API ROUTES ====================

@app.route('/api/upload-config', methods=['POST'])
def upload_config():
    """Handle configuration file upload and start ETL generation"""
    print("\n=== UPLOAD REQUEST ===")
    
    if 'file' not in request.files:
        return jsonify({'success': False, 'error': 'No file provided'}), 400
    
    file = request.files['file']
    if file.filename == '':
        return jsonify({'success': False, 'error': 'No file selected'}), 400
    
    if not allowed_file(file.filename):
        return jsonify({
            'success': False, 
            'error': f'File type not allowed. Supported: {", ".join(ALLOWED_EXTENSIONS)}'
        }), 400
    
    try:
        # Generate process ID
        process_id = str(uuid.uuid4())
        print(f"Process ID: {process_id}")
        
        # Save uploaded file
        filename = secure_filename(file.filename)
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        safe_filename = f"{timestamp}_{filename}"
        file_path = UPLOAD_FOLDER / safe_filename
        file.save(file_path)
        print(f"File saved: {file_path}")
        
        # Create process
        process_manager.create_process(process_id, str(file_path))
        
        # Start ETL generation in background thread
        thread = threading.Thread(
            target=run_etl_pipeline_with_progress,
            args=(process_id, str(file_path), None),
            daemon=True
        )
        thread.start()
        print(f"Background thread started")
        
        return jsonify({
            'success': True,
            'process_id': process_id,
            'filename': filename,
            'message': 'File uploaded and processing started'
        })
        
    except Exception as e:
        print(f"Upload error: {str(e)}")
        return jsonify({
            'success': False,
            'error': f'Upload failed: {str(e)}'
        }), 500

@app.route('/api/download-template', methods=['GET'])
def download_template():
    """Serve the Excel template file from backend/template folder"""
    print("\n=== TEMPLATE DOWNLOAD REQUEST ===")
    
    try:
        # Construct path to template file
        template_path = TEMPLATE_FOLDER / 'Config_Template.xlsx'
        
        # Check if file exists
        if not template_path.exists():
            print(f"Template file not found at: {template_path}")
            # Try alternative location
            alt_template_path = Path(__file__).parent.parent / 'template' / 'Config_Template.xlsx'
            if alt_template_path.exists():
                template_path = alt_template_path
                print(f"Using alternative path: {template_path}")
            else:
                return jsonify({
                    'success': False,
                    'error': 'Template file not found. Please ensure Config_Template.xlsx exists in backend/template/'
                }), 404
        
        print(f"Serving template from: {template_path}")
        
        # Send the file
        return send_file(
            str(template_path),
            mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
            as_attachment=True,
            download_name='Config_Template.xlsx',
            max_age=0
        )
        
    except Exception as e:
        print(f"Error serving template: {str(e)}")
        import traceback
        traceback.print_exc()
        return jsonify({
            'success': False,
            'error': f'Failed to download template: {str(e)}'
        }), 500

@app.route('/api/process/<process_id>/events')
def process_events(process_id):
    """Server-sent events endpoint"""
    print(f"\n=== SSE CONNECTION: {process_id[:8]}... ===")
    
    if process_id not in process_manager.processes:
        print("Process not found")
        return jsonify({'error': 'Process not found'}), 404
    
    def event_stream():
        # Send connection confirmation
        initial_event = {
            'type': 'connected',
            'process_id': process_id,
            'timestamp': datetime.now().isoformat()
        }
        yield f"data: {json.dumps(initial_event)}\n\n"
        print("Connection confirmed")
        
        # Stream events
        try:
            for event in process_manager.get_events(process_id):
                yield event
        except Exception as e:
            print(f"Event stream error: {e}")
            error_event = {
                'type': 'error',
                'message': str(e),
                'process_id': process_id,
                'timestamp': datetime.now().isoformat()
            }
            yield f"data: {json.dumps(error_event)}\n\n"
    
    response = Response(
        event_stream(),
        mimetype='text/event-stream',
        headers={
            'Cache-Control': 'no-cache',
            'Connection': 'keep-alive',
            'Access-Control-Allow-Origin': 'http://localhost:3000',
            'Access-Control-Allow-Credentials': 'true',
        }
    )
    
    return response

@app.route('/api/code-preview', methods=['POST'])
def get_code_preview():
    """Get generated code preview"""
    data = request.get_json()
    code_id = data.get('code_id')
    
    if not code_id or code_id not in process_manager.processes:
        return jsonify({'success': False, 'error': 'Invalid code ID'}), 404
    
    process = process_manager.processes[code_id]
    generated_code = process.get('generated_code', '')
    
    return jsonify({
        'success': True,
        'code': generated_code,
        'validation': process.get('validation_report', {}),
        'tests': process.get('test_report', [])
    })

@app.route('/api/download/py', methods=['POST'])
def download_python():
    """Download generated Python file"""
    data = request.get_json()
    code_id = data.get('code_id')
    
    if not code_id or code_id not in process_manager.processes:
        return jsonify({'error': 'Invalid code ID'}), 404
    
    process = process_manager.processes[code_id]
    generated_code = process.get('generated_code', '')
    
    if not generated_code:
        return jsonify({'error': 'No code generated'}), 404
    
    # Create temporary file
    with tempfile.NamedTemporaryFile(mode='w', suffix='.py', delete=False, encoding='utf-8') as f:
        f.write(generated_code)
        f.write(f"\n\n# Generated: {datetime.now()}")
        f.write(f"\n# Process: {code_id}")
        temp_file = f.name
    
    return send_file(
        temp_file,
        as_attachment=True,
        download_name='etl_pipeline.py',
        mimetype='text/x-python'
    )

@app.route('/api/download/ipynb', methods=['POST'])
def download_notebook():
    """Download generated Jupyter notebook"""
    data = request.get_json()
    code_id = data.get('code_id')
    
    if not code_id or code_id not in process_manager.processes:
        return jsonify({'error': 'Invalid code ID'}), 404
    
    process = process_manager.processes[code_id]
    generated_code = process.get('generated_code', '')
    
    if not generated_code:
        return jsonify({'error': 'No code generated'}), 404
    
    # Create notebook
    notebook = {
        "cells": [
            {
                "cell_type": "markdown",
                "metadata": {},
                "source": [
                    "# ETL Pipeline - Auto Generated\n",
                    f"\n**Generated:** {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n",
                    f"**Process ID:** {code_id}\n",
                    "\n## Overview\n",
                    "Auto-generated ETL pipeline for Oracle to Databricks migration.\n"
                ]
            },
            {
                "cell_type": "code",
                "execution_count": None,
                "metadata": {},
                "outputs": [],
                "source": [generated_code]
            }
        ],
        "metadata": {
            "kernelspec": {"display_name": "Python 3", "language": "python", "name": "python3"},
            "language_info": {"name": "python", "version": "3.8.0"}
        },
        "nbformat": 4,
        "nbformat_minor": 4
    }
    
    with tempfile.NamedTemporaryFile(mode='w', suffix='.ipynb', delete=False, encoding='utf-8') as f:
        json.dump(notebook, f, indent=2)
        temp_file = f.name
    
    return send_file(
        temp_file,
        as_attachment=True,
        download_name='etl_pipeline.ipynb',
        mimetype='application/json'
    )

@app.route('/api/health')
def health_check():
    """Health check endpoint"""
    return jsonify({
        'status': 'healthy',
        'timestamp': datetime.now().isoformat(),
        'active_processes': len(process_manager.processes),
        'active_connections': len(process_manager.active_connections),
        'template_available': (TEMPLATE_FOLDER / 'Config_Template.xlsx').exists()
    })

if __name__ == '__main__':
    print("=" * 60)
    print("ETL PIPELINE GENERATOR - PRODUCTION SERVER")
    print("=" * 60)
    print("Server starting on http://localhost:5000")
    print("SSE connections optimized for stability")
    print(f"Template folder: {TEMPLATE_FOLDER}")
    print(f"Template exists: {(TEMPLATE_FOLDER / 'Config_Template.xlsx').exists()}")
    print("-" * 60)
    
    # CRITICAL: Run without debug mode to prevent auto-restarts
    app.run(host='0.0.0.0', port=5000, debug=False, threaded=True)