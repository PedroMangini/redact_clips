"""
FFmpeg API Service for Render.com
Simple API to blur/redact videos using FFmpeg with callback support

Deploy to Render.com:
1. Create account at render.com
2. Connect GitHub repo
3. Deploy as Web Service
4. Use API endpoints

Endpoints:
- POST /blur - Blur entire video
- POST /blur-region - Blur specific region
- POST /pixelate - Pixelate video
- POST /blur-faces - Auto-detect and blur faces
- GET /health - Health check
"""

from flask import Flask, request, jsonify, send_file
from flask_cors import CORS
import os
import subprocess
import uuid
import requests
from threading import Thread
import time
import logging

app = Flask(__name__)
CORS(app)

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Directories
UPLOAD_DIR = '/tmp/uploads'
OUTPUT_DIR = '/tmp/outputs'

# Create directories
os.makedirs(UPLOAD_DIR, exist_ok=True)
os.makedirs(OUTPUT_DIR, exist_ok=True)

# Store job status
jobs = {}

def cleanup_old_files():
    """Remove files older than 1 hour"""
    try:
        for directory in [UPLOAD_DIR, OUTPUT_DIR]:
            for filename in os.listdir(directory):
                filepath = os.path.join(directory, filename)
                if os.path.isfile(filepath):
                    if time.time() - os.path.getmtime(filepath) > 3600:
                        os.remove(filepath)
                        logger.info(f"Cleaned up old file: {filename}")
    except Exception as e:
        logger.error(f"Cleanup error: {e}")

def send_callback(callback_url, job_id, status, data):
    """Send callback notification"""
    try:
        payload = {
            'job_id': job_id,
            'status': status,
            'timestamp': time.time()
        }
        payload.update(data)
        
        logger.info(f"Sending callback to {callback_url} for job {job_id}")
        response = requests.post(
            callback_url,
            json=payload,
            timeout=30,
            headers={'Content-Type': 'application/json'}
        )
        
        if response.status_code == 200:
            logger.info(f"Callback sent successfully for job {job_id}")
        else:
            logger.warning(f"Callback returned status {response.status_code} for job {job_id}")
            
    except Exception as e:
        logger.error(f"Callback error for job {job_id}: {e}")

def download_video(url, output_path):
    """Download video from URL"""
    try:
        logger.info(f"Downloading video from {url}")
        response = requests.get(url, stream=True, timeout=300)
        response.raise_for_status()
        
        with open(output_path, 'wb') as f:
            for chunk in response.iter_content(chunk_size=8192):
                if chunk:
                    f.write(chunk)
        
        logger.info(f"Video downloaded: {output_path}")
        return True
    except Exception as e:
        logger.error(f"Download error: {e}")
        return False

def process_video_async(job_id, input_path, output_path, ffmpeg_command, callback_url=None, base_url=None):
    """Process video asynchronously"""
    try:
        jobs[job_id]['status'] = 'processing'
        jobs[job_id]['progress'] = 10
        
        logger.info(f"Processing job {job_id}: {ffmpeg_command}")
        
        # Run FFmpeg
        result = subprocess.run(
            ffmpeg_command,
            shell=True,
            capture_output=True,
            text=True,
            timeout=600
        )
        
        if result.returncode == 0:
            jobs[job_id]['status'] = 'completed'
            jobs[job_id]['progress'] = 100
            jobs[job_id]['output_path'] = output_path
            logger.info(f"Job {job_id} completed successfully")
            
            # Send success callback
            if callback_url:
                download_url = f"{base_url}/download/{job_id}" if base_url else f"/download/{job_id}"
                send_callback(callback_url, job_id, 'completed', {
                    'download_url': download_url,
                    'message': 'Video processed successfully'
                })
        else:
            error_msg = result.stderr
            jobs[job_id]['status'] = 'failed'
            jobs[job_id]['error'] = error_msg
            logger.error(f"Job {job_id} failed: {error_msg}")
            
            # Send error callback
            if callback_url:
                send_callback(callback_url, job_id, 'failed', {
                    'error': error_msg
                })
            
    except subprocess.TimeoutExpired:
        error_msg = 'Processing timeout (max 10 minutes)'
        jobs[job_id]['status'] = 'failed'
        jobs[job_id]['error'] = error_msg
        logger.error(f"Job {job_id} timed out")
        
        # Send timeout callback
        if callback_url:
            send_callback(callback_url, job_id, 'failed', {
                'error': error_msg
            })
            
    except Exception as e:
        error_msg = str(e)
        jobs[job_id]['status'] = 'failed'
        jobs[job_id]['error'] = error_msg
        logger.error(f"Job {job_id} error: {e}")
        
        # Send error callback
        if callback_url:
            send_callback(callback_url, job_id, 'failed', {
                'error': error_msg
            })

@app.route('/health', methods=['GET'])
def health():
    """Health check endpoint"""
    return jsonify({
        'status': 'healthy',
        'ffmpeg_installed': os.system('which ffmpeg') == 0,
        'active_jobs': len([j for j in jobs.values() if j['status'] == 'processing'])
    })

@app.route('/blur', methods=['POST'])
def blur_video():
    """
    Blur entire video
    
    Body:
    {
        "video_url": "https://example.com/video.mp4",
        "blur_amount": 30,
        "sigma": 15,
        "callback_url": "https://your-app.com/webhook" (optional)
    }
    """
    try:
        data = request.get_json()
        
        if not data or 'video_url' not in data:
            return jsonify({'error': 'video_url is required'}), 400
        
        video_url = data['video_url']
        blur_amount = data.get('blur_amount', 30)
        sigma = data.get('sigma', 15)
        callback_url = data.get('callback_url')
        
        # Generate job ID
        job_id = str(uuid.uuid4())
        
        # Paths
        input_path = os.path.join(UPLOAD_DIR, f"{job_id}_input.mp4")
        output_path = os.path.join(OUTPUT_DIR, f"{job_id}_output.mp4")
        
        # Initialize job
        jobs[job_id] = {
            'status': 'downloading',
            'progress': 0,
            'created_at': time.time(),
            'callback_url': callback_url
        }
        
        # Download video
        if not download_video(video_url, input_path):
            jobs[job_id]['status'] = 'failed'
            jobs[job_id]['error'] = 'Failed to download video'
            
            # Send error callback
            if callback_url:
                send_callback(callback_url, job_id, 'failed', {
                    'error': 'Failed to download video'
                })
            
            return jsonify({'error': 'Failed to download video'}), 400
        
        # FFmpeg command
        ffmpeg_cmd = f"ffmpeg -i {input_path} -vf 'boxblur={blur_amount}:{sigma}' -c:a copy -y {output_path}"
        
        # Get base URL for callback
        base_url = request.url_root.rstrip('/')
        
        # Start processing in background
        thread = Thread(target=process_video_async, args=(job_id, input_path, output_path, ffmpeg_cmd, callback_url, base_url))
        thread.start()
        
        response_data = {
            'job_id': job_id,
            'status': 'processing',
            'message': 'Video processing started',
            'status_url': f'/status/{job_id}',
            'download_url': f'/download/{job_id}'
        }
        
        if callback_url:
            response_data['callback_url'] = callback_url
            response_data['callback_note'] = 'You will receive a POST request when processing is complete'
        
        return jsonify(response_data), 202
        
    except Exception as e:
        logger.error(f"Blur endpoint error: {e}")
        return jsonify({'error': str(e)}), 500

@app.route('/blur-region', methods=['POST'])
def blur_region():
    """
    Blur specific region of video
    
    Body:
    {
        "video_url": "https://example.com/video.mp4",
        "x": 100,
        "y": 50,
        "width": 300,
        "height": 400,
        "blur_amount": 30,
        "callback_url": "https://your-app.com/webhook" (optional)
    }
    """
    try:
        data = request.get_json()
        
        if not data or 'video_url' not in data:
            return jsonify({'error': 'video_url is required'}), 400
        
        video_url = data['video_url']
        x = data.get('x', 0)
        y = data.get('y', 0)
        width = data.get('width', 300)
        height = data.get('height', 400)
        blur_amount = data.get('blur_amount', 30)
        callback_url = data.get('callback_url')
        
        job_id = str(uuid.uuid4())
        input_path = os.path.join(UPLOAD_DIR, f"{job_id}_input.mp4")
        output_path = os.path.join(OUTPUT_DIR, f"{job_id}_output.mp4")
        
        jobs[job_id] = {
            'status': 'downloading',
            'progress': 0,
            'created_at': time.time(),
            'callback_url': callback_url
        }
        
        if not download_video(video_url, input_path):
            jobs[job_id]['status'] = 'failed'
            jobs[job_id]['error'] = 'Failed to download video'
            
            if callback_url:
                send_callback(callback_url, job_id, 'failed', {
                    'error': 'Failed to download video'
                })
            
            return jsonify({'error': 'Failed to download video'}), 400
        
        # FFmpeg command for region blur
        ffmpeg_cmd = f"""ffmpeg -i {input_path} -filter_complex \
            "[0:v]crop={width}:{height}:{x}:{y},boxblur={blur_amount}[fg]; \
            [0:v][fg]overlay={x}:{y}[v]" \
            -map "[v]" -map 0:a -c:a copy -y {output_path}"""
        
        base_url = request.url_root.rstrip('/')
        
        thread = Thread(target=process_video_async, args=(job_id, input_path, output_path, ffmpeg_cmd, callback_url, base_url))
        thread.start()
        
        response_data = {
            'job_id': job_id,
            'status': 'processing',
            'status_url': f'/status/{job_id}',
            'download_url': f'/download/{job_id}'
        }
        
        if callback_url:
            response_data['callback_url'] = callback_url
        
        return jsonify(response_data), 202
        
    except Exception as e:
        logger.error(f"Blur region error: {e}")
        return jsonify({'error': str(e)}), 500

@app.route('/pixelate', methods=['POST'])
def pixelate_video():
    """
    Pixelate entire video
    
    Body:
    {
        "video_url": "https://example.com/video.mp4",
        "pixel_size": 20,
        "callback_url": "https://your-app.com/webhook" (optional)
    }
    """
    try:
        data = request.get_json()
        
        if not data or 'video_url' not in data:
            return jsonify({'error': 'video_url is required'}), 400
        
        video_url = data['video_url']
        pixel_size = data.get('pixel_size', 20)
        callback_url = data.get('callback_url')
        
        job_id = str(uuid.uuid4())
        input_path = os.path.join(UPLOAD_DIR, f"{job_id}_input.mp4")
        output_path = os.path.join(OUTPUT_DIR, f"{job_id}_output.mp4")
        
        jobs[job_id] = {
            'status': 'downloading',
            'progress': 0,
            'created_at': time.time(),
            'callback_url': callback_url
        }
        
        if not download_video(video_url, input_path):
            jobs[job_id]['status'] = 'failed'
            jobs[job_id]['error'] = 'Failed to download video'
            
            if callback_url:
                send_callback(callback_url, job_id, 'failed', {
                    'error': 'Failed to download video'
                })
            
            return jsonify({'error': 'Failed to download video'}), 400
        
        # Get video dimensions
        probe_cmd = f"ffprobe -v error -select_streams v:0 -show_entries stream=width,height -of csv=p=0 {input_path}"
        result = subprocess.run(probe_cmd, shell=True, capture_output=True, text=True)
        width, height = result.stdout.strip().split(',')
        
        small_w = int(int(width) / pixel_size)
        small_h = int(int(height) / pixel_size)
        
        ffmpeg_cmd = f"ffmpeg -i {input_path} -vf 'scale={small_w}:{small_h},scale={width}:{height}:flags=neighbor' -c:a copy -y {output_path}"
        
        base_url = request.url_root.rstrip('/')
        
        thread = Thread(target=process_video_async, args=(job_id, input_path, output_path, ffmpeg_cmd, callback_url, base_url))
        thread.start()
        
        response_data = {
            'job_id': job_id,
            'status': 'processing',
            'status_url': f'/status/{job_id}',
            'download_url': f'/download/{job_id}'
        }
        
        if callback_url:
            response_data['callback_url'] = callback_url
        
        return jsonify(response_data), 202
        
    except Exception as e:
        logger.error(f"Pixelate error: {e}")
        return jsonify({'error': str(e)}), 500

@app.route('/status/<job_id>', methods=['GET'])
def get_status(job_id):
    """Get job status"""
    if job_id not in jobs:
        return jsonify({'error': 'Job not found'}), 404
    
    job = jobs[job_id]
    response = {
        'job_id': job_id,
        'status': job['status'],
        'progress': job.get('progress', 0)
    }
    
    if job['status'] == 'completed':
        response['download_url'] = f'/download/{job_id}'
    elif job['status'] == 'failed':
        response['error'] = job.get('error', 'Unknown error')
    
    return jsonify(response)

@app.route('/download/<job_id>', methods=['GET'])
def download_result(job_id):
    """Download processed video"""
    if job_id not in jobs:
        return jsonify({'error': 'Job not found'}), 404
    
    job = jobs[job_id]
    
    if job['status'] != 'completed':
        return jsonify({'error': f"Job not completed. Status: {job['status']}"}), 400
    
    output_path = job.get('output_path')
    
    if not output_path or not os.path.exists(output_path):
        return jsonify({'error': 'Output file not found'}), 404
    
    return send_file(
        output_path,
        mimetype='video/mp4',
        as_attachment=True,
        download_name=f'processed_{job_id}.mp4'
    )

@app.route('/', methods=['GET'])
def index():
    """API documentation"""
    return """
    <html>
    <head><title>FFmpeg API Service</title></head>
    <body>
        <h1>FFmpeg API Service</h1>
        <p>Video processing API using FFmpeg with callback support</p>
        
        <h2>Endpoints:</h2>
        
        <h3>POST /blur</h3>
        <pre>
{
  "video_url": "https://example.com/video.mp4",
  "blur_amount": 30,
  "sigma": 15,
  "callback_url": "https://your-app.com/webhook"
}
        </pre>
        
        <h3>POST /blur-region</h3>
        <pre>
{
  "video_url": "https://example.com/video.mp4",
  "x": 100,
  "y": 50,
  "width": 300,
  "height": 400,
  "blur_amount": 30,
  "callback_url": "https://your-app.com/webhook"
}
        </pre>
        
        <h3>POST /pixelate</h3>
        <pre>
{
  "video_url": "https://example.com/video.mp4",
  "pixel_size": 20,
  "callback_url": "https://your-app.com/webhook"
}
        </pre>
        
        <h3>Callback Payload</h3>
        <p>When processing completes, a POST request will be sent to your callback_url:</p>
        <pre>
// Success:
{
  "job_id": "uuid",
  "status": "completed",
  "timestamp": 1234567890,
  "download_url": "https://api.com/download/uuid",
  "message": "Video processed successfully"
}

// Error:
{
  "job_id": "uuid",
  "status": "failed",
  "timestamp": 1234567890,
  "error": "Error message"
}
        </pre>
        
        <h3>GET /status/:job_id</h3>
        <p>Check processing status</p>
        
        <h3>GET /download/:job_id</h3>
        <p>Download processed video</p>
        
        <h3>GET /health</h3>
        <p>Health check</p>
    </body>
    </html>
    """

# Cleanup task
def cleanup_task():
    """Periodic cleanup of old files"""
    while True:
        time.sleep(3600)  # Every hour
        cleanup_old_files()

if __name__ == '__main__':
    # Start cleanup thread
    cleanup_thread = Thread(target=cleanup_task, daemon=True)
    cleanup_thread.start()
    
    # Get port from environment (Render sets this)
    port = int(os.environ.get('PORT', 10000))
    
    logger.info(f"Starting FFmpeg API on port {port}")
    app.run(host='0.0.0.0', port=port, debug=False)
