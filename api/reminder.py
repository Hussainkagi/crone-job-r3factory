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

class SharePointReminderHandler(BaseHTTPRequestHandler):
    """Vercel Serverless Function Handler"""
    
    def do_GET(self):
        """Handle GET requests (for cron jobs)"""
        try:
            result = self.run_reminder_check()
            
            self.send_response(200)
            self.send_header('Content-type', 'application/json')
            self.end_headers()
            
            response = {
                'success': result,
                'timestamp': datetime.now().isoformat(),
                'message': 'Reminder check completed successfully' if result else 'Reminder check failed'
            }
            
            self.wfile.write(json.dumps(response).encode())
            
        except Exception as e:
            self.send_response(500)
            self.send_header('Content-type', 'application/json')
            self.end_headers()
            
            response = {
                'success': False,
                'error': str(e),
                'timestamp': datetime.now().isoformat()
            }
            
            self.wfile.write(json.dumps(response).encode())
    
    def do_POST(self):
        """Handle POST requests"""
        self.do_GET()
    
    def log_message(self, format, *args):
        """Custom logging"""
        print(f"[{datetime.now().isoformat()}] {format % args}")
    
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
        <body>
            <h2>🏦 Cheque Payment Due Reminder</h2>
            <p>The following <strong>{len(reminder_data)}</strong> cheque payment(s) are due in 3 days:</p>
            <table border="1" style="border-collapse: collapse; width: 100%;">
                <thead>
                    <tr style="background-color: #f2f2f2;">
        """
        
        for col in reminder_data.columns:
            html_body += f"<th style='padding: 12px;'>{col}</th>"
        
        html_body += "</tr></thead><tbody>"
        
        for _, row in reminder_data.iterrows():
            html_body += "<tr>"
            for col in reminder_data.columns:
                value = row[col]
                if pd.isna(value):
                    value = ""
                elif col == 'Payment Due [Date]' and hasattr(value, 'strftime'):
                    value = value.strftime('%d-%b-%Y')
                else:
                    value = str(value)
                html_body += f"<td style='padding: 10px;'>{value}</td>"
            html_body += "</tr>"
        
        today_str = datetime.now().strftime('%d-%b-%Y')
        target_str = (datetime.now() + timedelta(days=3)).strftime('%d-%b-%Y')
        
        html_body += f"""
                </tbody>
            </table>
            <p><strong>📅 Reminder Date:</strong> {today_str}</p>
            <p><strong>🎯 Payment Due Date:</strong> {target_str}</p>
            <p><em>⚡ Automated reminder from SharePoint</em></p>
        </body>
        </html>
        """
        
        return html_body
    
    def send_email(self, reminder_data: pd.DataFrame, config: dict) -> bool:
        """Send reminder emails"""
        if reminder_data.empty:
            print("No reminders to send")
            return True
        
        recipient_emails = [email.strip() for email in config['recipient_emails'].split(',')]
        success_count = 0
        
        for recipient in recipient_emails:
            try:
                msg = MIMEMultipart()
                msg['From'] = config['email_username']
                msg['To'] = recipient
                msg['Subject'] = f"Cheque Payment Due Reminder - {len(reminder_data)} payment(s)"
                
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
        
        return success_count > 0
    
    def run_reminder_check(self) -> bool:
        """Main reminder check logic"""
        print(f"🚀 Starting reminder check at {datetime.now().isoformat()}")
        
        # Get configuration from environment variables
        config = {
            'sharepoint_url': os.getenv('SHAREPOINT_SHARED_URL'),
            'smtp_server': os.getenv('SMTP_SERVER', 'smtp.gmail.com'),
            'smtp_port': os.getenv('SMTP_PORT', '587'),
            'email_username': os.getenv('EMAIL_USERNAME'),
            'email_password': os.getenv('EMAIL_PASSWORD'),
            'recipient_emails': os.getenv('RECIPIENT_EMAILS')
        }
        
        # Validate configuration
        missing = [k for k, v in config.items() if not v]
        if missing:
            print(f"❌ Missing configuration: {missing}")
            return False
        
        # Download and parse Excel file
        excel_file = self.download_excel_file(config['sharepoint_url'])
        if not excel_file:
            return False
        
        df = self.parse_excel_data(excel_file)
        if df is None:
            return False
        
        # Find and send reminders
        reminders = self.find_reminders_needed(df)
        
        if not reminders.empty:
            return self.send_email(reminders, config)
        else:
            print("ℹ️ No reminders needed today")
            return True


# Vercel handler function
def handler(request, response):
    """Vercel serverless function entry point"""
    handler_instance = SharePointReminderHandler()
    
    # Mock the request/response for BaseHTTPRequestHandler
    handler_instance.command = request.method
    handler_instance.path = request.url
    
    if request.method == 'GET':
        handler_instance.do_GET()
    else:
        handler_instance.do_POST()
    
    return response