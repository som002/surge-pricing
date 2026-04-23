import http.server
import socketserver
import os
import json
import subprocess
import threading
import sys
from google.cloud import storage

PORT = 8000
DIRECTORY = os.path.dirname(os.path.abspath(__file__))
BUCKET_NAME = os.environ.get("GCS_BUCKET", "YOUR_BUCKET_NAME")
STATE_BLOB_NAME = "surge-results/surge_results.json"
STORAGE_CLIENT = storage.Client(project=os.environ.get("GCP_PROJECT_ID", "YOUR_PROJECT_ID"))

class Handler(http.server.SimpleHTTPRequestHandler):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, directory=DIRECTORY, **kwargs)

    def do_GET(self):
        if self.path == '/api/state':
            self.send_response(200)
            self.send_header("Content-type", "application/json")
            self.send_header("Access-Control-Allow-Origin", "*")
            self.send_header("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
            self.send_header("Access-Control-Allow-Headers", "Content-Type")
            self.send_header("Cache-Control", "no-cache, no-store, must-revalidate")
            self.send_header("Pragma", "no-cache")
            self.send_header("Expires", "0")
            self.end_headers()
            try:
                bucket = STORAGE_CLIENT.bucket(BUCKET_NAME)
                blob = bucket.blob(STATE_BLOB_NAME)
                
                if blob.exists():
                    raw_data = blob.download_as_text().strip()
                    # Strip legacy bracket prefix if present
                    if raw_data.startswith('[]'):
                        raw_data = raw_data[2:].strip()
                        
                    if raw_data:
                        # Convert NDJSON to a JSON array if it's not already one
                        if raw_data.startswith('{'):
                            lines = [line.strip() for line in raw_data.split('\n') if line.strip()]
                            json_array = f"[{','.join(lines)}]"
                            self.wfile.write(json_array.encode())
                        else:
                            self.wfile.write(raw_data.encode())
                    else:
                        self.wfile.write(b"[]")
                else:
                    self.wfile.write(b"[]")
            except Exception as e:
                print(f"Error reading from GCS: {e}")
                self.wfile.write(json.dumps({"error": str(e)}).encode())
        else:
            if self.path == '/':
                self.path = '/index.html'
            super().do_GET()

    def do_OPTIONS(self):
        self.send_response(200)
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
        self.send_header("Access-Control-Allow-Headers", "Content-Type")
        self.end_headers()

    def do_POST(self):
        if self.path == '/api/clear':
            try:
                content_length = int(self.headers.get('Content-Length', 0))
                body = self.rfile.read(content_length).decode('utf-8')
                data = json.loads(body) if body else {}

                expected_password = os.environ.get('ADMIN_PASSWORD')
                if not expected_password or data.get('password') != expected_password:
                    self.send_response(401)
                    self.send_header("Content-type", "application/json")
                    self.send_header("Access-Control-Allow-Origin", "*")
                    self.send_header("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
                    self.send_header("Access-Control-Allow-Headers", "Content-Type")
                    self.end_headers()
                    self.wfile.write(json.dumps({"status": "unauthorized", "message": "Incorrect password"}).encode())
                    return

                bucket = STORAGE_CLIENT.bucket(BUCKET_NAME)
                blob = bucket.blob(STATE_BLOB_NAME)
                blob.upload_from_string("", content_type="application/json")
                
                self.send_response(200)
                self.send_header("Content-type", "application/json")
                self.send_header("Access-Control-Allow-Origin", "*")
                self.send_header("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
                self.send_header("Access-Control-Allow-Headers", "Content-Type")
                self.end_headers()
                self.wfile.write(json.dumps({"status": "success"}).encode())
            except Exception as e:
                print(f"Error clearing GCS: {e}")
                self.send_response(500)
                self.send_header("Content-type", "application/json")
                self.send_header("Access-Control-Allow-Origin", "*")
                self.send_header("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
                self.send_header("Access-Control-Allow-Headers", "Content-Type")
                self.end_headers()
                self.wfile.write(json.dumps({"status": "error", "message": str(e)}).encode())
        else:
            self.send_response(404)
            self.end_headers()


def get_ip():
    try:
        cmd = ["curl", "-s", "-H", "Metadata-Flavor: Google", 
               "http://metadata.google.internal/computeMetadata/v1/instance/network-interfaces/0/access-configs/0/external-ip"]
        return subprocess.check_output(cmd, timeout=2).decode("utf-8").strip()
    except subprocess.SubprocessError:
        try:
            return subprocess.check_output(["curl", "-s", "ifconfig.me"], timeout=2).decode("utf-8").strip()
        except:
            return "localhost"

IP_ADDR = get_ip()

socketserver.ThreadingTCPServer.allow_reuse_address = True
with socketserver.ThreadingTCPServer(("0.0.0.0", PORT), Handler) as httpd:
    print(f"🌟 UI Server running! Check it out at: http://{IP_ADDR}:{PORT}")
    print(f"   Reading state from: gs://{BUCKET_NAME}/{STATE_BLOB_NAME}")
    try:
        httpd.serve_forever()
    except KeyboardInterrupt:
        print("\n🛑 Server stopping gracefully...")