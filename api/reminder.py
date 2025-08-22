import os
import pandas as pd
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from datetime import datetime, timedelta
import requests
import io
import json
from typing import Optional, List
from http.server import BaseHTTPRequestHandler

class ReminderService:
    """Core reminder functionality"""
    
    def convert_sharepoint_url_to_direct_download(self, shared_url: str) -> str:
        """Convert SharePoint shared URL to direct download URL"""
        try:
            print(f"Original URL: {shared_url}")
            
            if "sharepoint.com" in shared_url and ("/:x:/" in shared_url or "/:b:/" in shared_url):
                if "/:x:/" in shared_url:
                    download_url = shared_url.replace("/:x:/", "/:b:/")
                else:
                    download_url = shared_url
                    
                if "download=1" not in download_url:
                    separator = "&" if "?" in download_url else "?"
                    download_url = f"{download_url}{separator}download=1"
                    
                print(f"Download URL: {download_url}")
                return download_url
                
            return shared_url
                
        except Exception as e:
            print(f"Error converting SharePoint URL: {e}")
            return shared_url
    
    def download_excel_file(self, sharepoint_url: str) -> Optional[io.BytesIO]:
        """Download Excel file from SharePoint shared link"""
        try:
            urls_to_try = [
                sharepoint_url,
                self.convert_sharepoint_url_to_direct_download(sharepoint_url),
            ]
            
            base_url = sharepoint_url.split('?')[0] if '?' in sharepoint_url else sharepoint_url
            additional_urls = [
                f"{base_url}?download=1",
                f"{sharepoint_url}&download=1" if "?" in sharepoint_url else f"{sharepoint_url}?download=1",
            ]
            urls_to_try.extend(additional_urls)
            
            # Remove duplicates
            urls_to_try = list(dict.fromkeys(urls_to_try))
            
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
                'Accept': 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet,application/vnd.ms-excel,*/*',
            }
            
            for i, url in enumerate(urls_to_try):
                try:
                    print(f"Attempt {i+1}: Downloading from {url}")
                    
                    response = requests.get(url, headers=headers, timeout=30)
                    print(f"Response status: {response.status_code}")
                    
                    if response.status_code == 200:
                        content_type = response.headers.get('content-type', '').lower()
                        
                        if ('excel' in content_type or 
                            'spreadsheet' in content_type or 
                            'vnd.openxmlformats' in content_type or
                            response.content.startswith(b'PK')):
                            
                            print(f"✅ Downloaded Excel file ({len(response.content)} bytes)")
                            return io.BytesIO(response.content)
                            
                except Exception as e:
                    print(f"Download attempt {i+1} failed: {e}")
                    continue
            
            print("❌ Failed to download Excel file")
            return None
            
        except Exception as e:
            print(f"Download error: {e}")
            return None
    
    def parse_excel_data(self, excel_file: io.BytesIO) -> Optional[pd.DataFrame]:
        """Parse Excel file and extract cheque payments"""
        try:
            excel_file.seek(0)
            
            # Try different header rows
            for header_row in range(5):
                try:
                    df = pd.read_excel(excel_file, header=header_row)
                    
                    if df.empty:
                        continue
                    
                    # Clean column names
                    df.columns = [str(col).strip() for col in df.columns]
                    
                    # Find payment mode and date columns
                    payment_col = None
                    date_col = None
                    
                    for col in df.columns:
                        col_lower = str(col).lower()
                        
                        # Find payment mode column
                        if not payment_col and ('payment' in col_lower and 'mode' in col_lower):
                            payment_col = col
                        elif not payment_col and 'mode' in col_lower and 'pay' in col_lower:
                            payment_col = col
                        
                        # Find date column
                        if not date_col and ('due' in col_lower and 'date' in col_lower):
                            date_col = col
                        elif not date_col and ('payment' in col_lower and 'date' in col_lower and 'due' in col_lower):
                            date_col = col
                    
                    if payment_col and date_col:
                        print(f"✅ Found columns: {payment_col}, {date_col}")
                        
                        # Filter for cheque payments
                        df_filtered = df[df[payment_col].astype(str).str.lower().str.contains('cheque|check', na=False)]
                        
                        if df_filtered.empty:
                            print("No cheque payments found")
                            return pd.DataFrame()
                        
                        # Parse dates
                        df_filtered[date_col] = pd.to_datetime(df_filtered[date_col], errors='coerce')
                        df_filtered = df_filtered.dropna(subset=[date_col])
                        
                        print(f"Found {len(df_filtered)} cheque payments with valid dates")
                        return df_filtered[['Mode of Payment' if 'mode' in payment_col.lower() else payment_col, 
                                         'Payment Due [Date]' if 'due' in date_col.lower() else date_col]].rename(columns={
                            payment_col: 'Mode of Payment',
                            date_col: 'Payment Due [Date]'
                        })
                    
                    excel_file.seek(0)
                    
                except Exception as e:
                    excel_file.seek(0)
                    continue
            
            print("❌ Could not parse Excel file")
            return None
            
        except Exception as e:
            print(f"Parse error: {e}")
            return None
    
    def find_reminders_needed(self, df: pd.DataFrame) -> pd.DataFrame:
        """Find cheques due in 3 days"""
        if df is None or df.empty:
            return pd.DataFrame()
        
        today = datetime.now().date()
        target_date = today + timedelta(days=3)
        
        reminders = df[df['Payment Due [Date]'].dt.date == target_date]
        
        print(f"Checking for reminders on {target_date}: Found {len(reminders)}")
        return reminders
    
    def create_email_body(self, reminder_data: pd.DataFrame) -> str:
        """Create HTML email body"""
        html_body = f"""
        <html>
        <body style="font-family: Arial, sans-serif; margin: 0; padding: 20px; background-color: #f5f5f5;">
            <div style="max-width: 600px; margin: 0 auto; background-color: white; border-radius: 10px; padding: 30px; box-shadow: 0 4px 6px rgba(0,0,0,0.1);">
                <h2 style="color: #2c3e50; margin-bottom: 20px; text-align: center;">🏦 Cheque Payment Due Reminder</h2>
                <p style="font-size: 16px; color: #34495e; margin-bottom: 20px;">
                    The following <strong style="color: #e74c3c;">{len(reminder_data)}</strong> cheque payment(s) are due in 3 days:
                </p>
                <table style="width: 100%; border-collapse: collapse; margin: 20px 0;">
                    <thead>
                        <tr style="background-color: #3498db; color: white;">
        """
        
        for col in reminder_data.columns:
            html_body += f"<th style='padding: 12px; text-align: left; border: 1px solid #ddd;'>{col}</th>"
        
        html_body += "</tr></thead><tbody>"
        
        for i, (_, row) in enumerate(reminder_data.iterrows()):
            bg_color = "#f8f9fa" if i % 2 == 0 else "#ffffff"
            html_body += f"<tr style='background-color: {bg_color};'>"
            for col in reminder_data.columns:
                value = row[col]
                if pd.isna(value):
                    value = ""
                elif col == 'Payment Due [Date]' and hasattr(value, 'strftime'):
                    value = value.strftime('%d-%b-%Y')
                else:
                    value = str(value)
                html_body += f"<td style='padding: 10px; border: 1px solid #ddd;'>{value}</td>"
            html_body += "</tr>"
        
        today_str = datetime.now().strftime('%d-%b-%Y')
        target_str = (datetime.now() + timedelta(days=3)).strftime('%d-%b-%Y')
        
        html_body += f"""
                    </tbody>
                </table>
                <div style="background-color: #ecf0f1; padding: 15px; border-radius: 5px; margin-top: 20px;">
                    <p style="margin: 5px 0;"><strong>📅 Reminder Date:</strong> {today_str}</p>
                    <p style="margin: 5px 0;"><strong>🎯 Payment Due Date:</strong> {target_str}</p>
                    <p style="margin: 5px 0; color: #7f8c8d;"><em>⚡ Automated reminder from SharePoint</em></p>
                </div>
            </div>
        </body>
        </html>
        """
        
        return html_body
    
    def send_email(self, reminder_data: pd.DataFrame, config: dict) -> tuple[bool, int]:
        """Send reminder emails"""
        if reminder_data.empty:
            print("No reminders to send")
            return True, 0
        
        recipient_emails = [email.strip() for email in config['recipient_emails'].split(',')]
        success_count = 0
        
        for recipient in recipient_emails:
            try:
                msg = MIMEMultipart()
                msg['From'] = config['email_username']
                msg['To'] = recipient
                msg['Subject'] = f"🏦 Cheque Payment Due Reminder - {len(reminder_data)} payment(s)"
                
                body = self.create_email_body(reminder_data)
                msg.attach(MIMEText(body, 'html'))
                
                with smtplib.SMTP(config['smtp_server'], int(config['smtp_port'])) as server:
                    server.starttls()
                    server.login(config['email_username'], config['email_password'])
                    server.send_message(msg)
                
                print(f"✅ Email sent to {recipient}")
                success_count += 1
                
            except Exception as e:
                print(f"❌ Failed to send email to {recipient}: {e}")
        
        return success_count > 0, success_count
    
    def get_config(self) -> dict:
        """Get configuration from environment variables"""
        config = {
            'sharepoint_url': os.getenv('SHAREPOINT_SHARED_URL'),
            'smtp_server': os.getenv('SMTP_SERVER', 'smtp.gmail.com'),
            'smtp_port': os.getenv('SMTP_PORT', '587'),
            'email_username': os.getenv('EMAIL_USERNAME'),
            'email_password': os.getenv('EMAIL_PASSWORD'),
            'recipient_emails': os.getenv('RECIPIENT_EMAILS')
        }
        return config
    
    def check_config_status(self, config: dict) -> dict:
        """Check which configuration items are set"""
        return {
            'sharepoint_configured': bool(config.get('sharepoint_url')),
            'email_configured': bool(config.get('email_username') and config.get('email_password')),
            'recipients_configured': bool(config.get('recipient_emails')),
            'all_configured': all([
                config.get('sharepoint_url'),
                config.get('email_username'),
                config.get('email_password'),
                config.get('recipient_emails')
            ])
        }
    
    def run_reminder_check(self) -> dict:
        """Main reminder check logic"""
        print(f"🚀 Starting reminder check at {datetime.now().isoformat()}")
        
        config = self.get_config()
        config_status = self.check_config_status(config)
        
        # Validate configuration
        missing = [k for k, v in config.items() if not v]
        if missing:
            error_msg = f"Missing configuration: {missing}"
            print(f"❌ {error_msg}")
            return {
                'success': False,
                'error': error_msg,
                'config': config_status,
                'emails_sent': 0,
                'reminders_found': 0
            }
        
        try:
            # Download and parse Excel file
            excel_file = self.download_excel_file(config['sharepoint_url'])
            if not excel_file:
                return {
                    'success': False,
                    'error': 'Failed to download Excel file from SharePoint',
                    'config': config_status,
                    'emails_sent': 0,
                    'reminders_found': 0
                }
            
            df = self.parse_excel_data(excel_file)
            if df is None:
                return {
                    'success': False,
                    'error': 'Failed to parse Excel file',
                    'config': config_status,
                    'emails_sent': 0,
                    'reminders_found': 0
                }
            
            # Find and send reminders
            reminders = self.find_reminders_needed(df)
            emails_sent = 0
            
            if not reminders.empty:
                email_success, emails_sent = self.send_email(reminders, config)
                if not email_success:
                    return {
                        'success': False,
                        'error': 'Failed to send emails',
                        'config': config_status,
                        'emails_sent': 0,
                        'reminders_found': len(reminders)
                    }
            
            return {
                'success': True,
                'message': f'Reminder check completed. {len(reminders)} reminders found, {emails_sent} emails sent.',
                'config': config_status,
                'emails_sent': emails_sent,
                'reminders_found': len(reminders)
            }
            
        except Exception as e:
            error_msg = f"Unexpected error: {str(e)}"
            print(f"❌ {error_msg}")
            return {
                'success': False,
                'error': error_msg,
                'config': config_status,
                'emails_sent': 0,
                'reminders_found': 0
            }


class handler(BaseHTTPRequestHandler):
    """Vercel Serverless Function Handler"""
    
    def do_GET(self):
        """Handle GET requests (for cron jobs and status checks)"""
        self._handle_request()
    
    def do_POST(self):
        """Handle POST requests (for manual triggers)"""
        self._handle_request()
    
    def _handle_request(self):
        """Handle both GET and POST requests"""
        try:
            service = ReminderService()
            
            # Get request body for POST requests
            content_length = int(self.headers.get('Content-Length', 0))
            if content_length > 0:
                post_data = self.rfile.read(content_length)
                try:
                    request_data = json.loads(post_data.decode('utf-8'))
                except:
                    request_data = {}
            else:
                request_data = {}
            
            # Run the reminder check
            result = service.run_reminder_check()
            
            # Add timestamp and request info
            result['timestamp'] = datetime.now().isoformat()
            result['method'] = self.command
            result['manual_trigger'] = request_data.get('manual', False)
            
            self.send_response(200)
            self.send_header('Content-type', 'application/json')
            self.send_header('Access-Control-Allow-Origin', '*')
            self.send_header('Access-Control-Allow-Methods', 'GET, POST, OPTIONS')
            self.send_header('Access-Control-Allow-Headers', 'Content-Type')
            self.end_headers()
            
            self.wfile.write(json.dumps(result, indent=2).encode())
            
        except Exception as e:
            self.send_response(500)
            self.send_header('Content-type', 'application/json')
            self.send_header('Access-Control-Allow-Origin', '*')
            self.end_headers()
            
            error_response = {
                'success': False,
                'error': str(e),
                'timestamp': datetime.now().isoformat(),
                'method': self.command
            }
            
            self.wfile.write(json.dumps(error_response, indent=2).encode())
    
    def do_OPTIONS(self):
        """Handle OPTIONS requests for CORS"""
        self.send_response(200)
        self.send_header('Access-Control-Allow-Origin', '*')
        self.send_header('Access-Control-Allow-Methods', 'GET, POST, OPTIONS')
        self.send_header('Access-Control-Allow-Headers', 'Content-Type')
        self.end_headers()
    
    def log_message(self, format, *args):
        """Custom logging"""
        print(f"[{datetime.now().isoformat()}] {format % args}")


# Alternative function-based handler for Vercel (if class-based doesn't work)
def api_handler(request):
    """Function-based handler for Vercel"""
    try:
        service = ReminderService()
        
        # Parse request data
        request_data = {}
        if hasattr(request, 'json') and request.json:
            request_data = request.json
        elif hasattr(request, 'body') and request.body:
            try:
                request_data = json.loads(request.body.decode('utf-8'))
            except:
                request_data = {}
        
        # Run the reminder check
        result = service.run_reminder_check()
        
        # Add metadata
        result['timestamp'] = datetime.now().isoformat()
        result['method'] = getattr(request, 'method', 'UNKNOWN')
        result['manual_trigger'] = request_data.get('manual', False)
        
        return {
            'statusCode': 200,
            'headers': {
                'Content-Type': 'application/json',
                'Access-Control-Allow-Origin': '*',
                'Access-Control-Allow-Methods': 'GET, POST, OPTIONS',
                'Access-Control-Allow-Headers': 'Content-Type'
            },
            'body': json.dumps(result, indent=2)
        }
        
    except Exception as e:
        return {
            'statusCode': 500,
            'headers': {
                'Content-Type': 'application/json',
                'Access-Control-Allow-Origin': '*'
            },
            'body': json.dumps({
                'success': False,
                'error': str(e),
                'timestamp': datetime.now().isoformat()
            }, indent=2)
        }