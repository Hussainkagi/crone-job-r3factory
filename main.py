import os
import pandas as pd
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from datetime import datetime, timedelta
import requests
import io
import logging
import time
from typing import Optional, Tuple, Dict, Any, List

# Configure logging
logging.basicConfig(
    level=logging.INFO, 
    format='%(asctime)s - %(levelname)s - %(message)s'
)


class SharePointSharedLinkReminder:
    """SharePoint Shared Link Reminder system for Render Cron Jobs"""
    
    def __init__(self, sharepoint_shared_url: str, smtp_server: str, smtp_port: int, 
                 email_username: str, email_password: str, recipient_emails: List[str]):
        """Initialize the SharePoint Shared Link Reminder system"""
        self.sharepoint_shared_url = sharepoint_shared_url
        self.smtp_server = smtp_server
        self.smtp_port = smtp_port
        self.email_username = email_username
        self.email_password = email_password
        self.recipient_emails = recipient_emails
        
    def convert_sharepoint_url_to_direct_download(self, shared_url: str) -> str:
        """Convert SharePoint shared URL to direct download URL"""
        try:
            logging.info(f"Original URL: {shared_url}")
            
            if "sharepoint.com" in shared_url and ("/:x:/" in shared_url or "/:b:/" in shared_url):
                if "/:x:/" in shared_url:
                    download_url = shared_url.replace("/:x:/", "/:b:/")
                else:
                    download_url = shared_url
                    
                if "download=1" not in download_url:
                    separator = "&" if "?" in download_url else "?"
                    download_url = f"{download_url}{separator}download=1"
                    
                logging.info(f"Download URL: {download_url}")
                return download_url
                
            return shared_url
                
        except Exception as e:
            logging.error(f"Error converting SharePoint URL: {e}")
            return shared_url
    
    def download_excel_file(self) -> Optional[io.BytesIO]:
        """Download Excel file from SharePoint shared link with enhanced error handling"""
        try:
            urls_to_try = [
                self.sharepoint_shared_url,
                self.convert_sharepoint_url_to_direct_download(self.sharepoint_shared_url),
            ]
            
            base_url = self.sharepoint_shared_url.split('?')[0] if '?' in self.sharepoint_shared_url else self.sharepoint_shared_url
            additional_urls = [
                f"{base_url}?download=1",
                f"{self.sharepoint_shared_url}&download=1" if "?" in self.sharepoint_shared_url else f"{self.sharepoint_shared_url}?download=1",
            ]
            urls_to_try.extend(additional_urls)
            
            # Remove duplicates while preserving order
            urls_to_try = list(dict.fromkeys(urls_to_try))
            
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
                'Accept': 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet,application/vnd.ms-excel,application/octet-stream,*/*',
                'Accept-Language': 'en-US,en;q=0.9',
            }
            
            session = requests.Session()
            
            for i, url in enumerate(urls_to_try):
                try:
                    logging.info(f"Attempt {i+1}: Trying to download from: {url}")
                    
                    response = session.get(url, headers=headers, timeout=60, allow_redirects=True)
                    logging.info(f"Response status: {response.status_code}")
                    logging.info(f"Content-Type: {response.headers.get('content-type', 'N/A')}")
                    
                    if response.status_code == 200:
                        content_type = response.headers.get('content-type', '').lower()
                        
                        if 'text/html' in content_type or response.content.startswith(b'<!DOCTYPE'):
                            logging.warning("Received HTML instead of Excel file")
                            continue
                        
                        if (len(response.content) > 1000 and  
                            ('excel' in content_type or 
                             'spreadsheet' in content_type or 
                             'vnd.openxmlformats' in content_type or
                             'application/octet-stream' in content_type or
                             response.content.startswith(b'PK'))):
                            
                            logging.info(f"✅ Successfully downloaded Excel file ({len(response.content)} bytes)")
                            return io.BytesIO(response.content)
                            
                except requests.exceptions.RequestException as e:
                    logging.warning(f"Request failed for {url}: {e}")
                    continue
                
                time.sleep(2)  # Increased delay for Render environment
            
            logging.error("Failed to download Excel file")
            return None
            
        except Exception as e:
            logging.error(f"Failed to download Excel file: {e}")
            return None
    
    def find_header_row_and_columns(self, excel_file: io.BytesIO) -> Tuple[Optional[pd.DataFrame], Optional[int]]:
        """Intelligently find the header row and identify required columns"""
        try:
            excel_file.seek(0)
            
            # Read first few rows to analyze structure
            sample_df = pd.read_excel(excel_file, header=None, nrows=10)
            logging.info(f"Sample data shape: {sample_df.shape}")
            
            # Look for rows that contain our target column names
            target_columns = ['mode of payment', 'payment due [date]']
            header_candidates = []
            
            for row_idx in range(min(6, len(sample_df))):
                row_data = sample_df.iloc[row_idx].astype(str).str.lower().str.strip()
                matches = 0
                column_positions: Dict[str, int] = {}
                
                for col_idx, cell_value in enumerate(row_data):
                    if pd.notna(cell_value) and cell_value != 'nan' and cell_value.strip():
                        # Clean the cell value
                        clean_cell = ' '.join(cell_value.split())
                        
                        for target in target_columns:
                            target_clean = target.replace(' ', '').replace('[', '').replace(']', '').lower()
                            cell_clean = clean_cell.replace(' ', '').replace('[', '').replace(']', '').lower()
                            
                            # Exact match (no spaces, brackets)
                            if cell_clean == target_clean:
                                matches += 2
                                column_positions[target] = col_idx
                            # Exact match (with spaces/brackets)
                            elif clean_cell.lower() == target:
                                matches += 2
                                column_positions[target] = col_idx
                            # Contains all words
                            elif all(word in clean_cell for word in target.replace('[', '').replace(']', '').split()):
                                matches += 1
                                if target not in column_positions:
                                    column_positions[target] = col_idx
                
                if matches > 0:
                    header_candidates.append((row_idx, matches, column_positions))
                    logging.info(f"Row {row_idx} has {matches} column matches")
            
            # Sort by best match
            header_candidates.sort(key=lambda x: x[1], reverse=True)
            
            if header_candidates:
                best_header_row, best_score, _ = header_candidates[0]
                logging.info(f"Selected header row: {best_header_row} with score: {best_score}")
                
                # Read the file with the identified header row
                excel_file.seek(0)
                df = pd.read_excel(excel_file, header=best_header_row)
                
                # Clean column names
                df.columns = [
                    ' '.join(str(col).strip().replace('\n', ' ').replace('\r', ' ').replace('\xa0', ' ').split())
                    for col in df.columns
                ]
                
                logging.info(f"Cleaned columns: {list(df.columns)}")
                return df, best_header_row
            else:
                # Fallback: try each row as header
                for header_row in range(min(5, len(sample_df))):
                    try:
                        excel_file.seek(0)
                        df = pd.read_excel(excel_file, header=header_row)
                        if len(df) > 0 and not df.empty:
                            logging.info(f"Using header row {header_row} as fallback")
                            # Clean column names
                            df.columns = [
                                ' '.join(str(col).strip().replace('\n', ' ').replace('\r', ' ').replace('\xa0', ' ').split())
                                for col in df.columns
                            ]
                            return df, header_row
                    except Exception:
                        continue
                
                return None, None
                
        except Exception as e:
            logging.error(f"Error finding header row: {e}")
            return None, None
    
    def parse_excel_data(self, excel_file: io.BytesIO) -> Optional[pd.DataFrame]:
        """Parse Excel file with intelligent header detection"""
        try:
            # Find the correct header row and read data
            df, header_row = self.find_header_row_and_columns(excel_file)
            
            if df is None:
                logging.error("Could not identify proper header row")
                return None

            logging.info(f"Excel file loaded successfully. Shape: {df.shape}")
            logging.info(f"Columns found: {list(df.columns)}")
            
            # Find required columns with flexible matching
            required_columns = ['Mode of Payment', 'Payment Due [Date]']
            column_mapping: Dict[str, str] = {}
            
            for req_col in required_columns:
                found_col = None
                req_col_lower = req_col.lower().strip()
                req_words = req_col_lower.replace('[', '').replace(']', '').split()
                
                # Strategy 1: Exact match (case insensitive, whitespace normalized)
                for df_col in df.columns:
                    df_col_clean = str(df_col).lower().strip()
                    df_col_clean = ' '.join(df_col_clean.split())
                    
                    if df_col_clean == req_col_lower:
                        found_col = df_col
                        logging.info(f"✅ Exact match for '{req_col}': '{df_col}'")
                        break
                
                # Strategy 2: Contains all words (ignoring brackets)
                if not found_col:
                    for df_col in df.columns:
                        df_col_clean = str(df_col).lower().strip().replace('[', '').replace(']', '')
                        if all(word in df_col_clean for word in req_words):
                            found_col = df_col
                            logging.info(f"✅ Word match for '{req_col}': '{df_col}'")
                            break
                
                # Strategy 3: Keyword matching
                if not found_col:
                    for df_col in df.columns:
                        df_col_clean = str(df_col).lower().strip()
                        
                        if req_col.lower() == 'mode of payment':
                            if any(keyword in df_col_clean for keyword in ['payment', 'pay', 'mode']):
                                if not any(exclude in df_col_clean for exclude in ['amount', 'due', 'reference', 'related']):
                                    found_col = df_col
                                    logging.info(f"✅ Keyword match for '{req_col}': '{df_col}'")
                                    break
                        
                        elif req_col.lower() == 'payment due [date]':
                            # Look for payment due date variations
                            if ('payment' in df_col_clean and 'due' in df_col_clean and 'date' in df_col_clean):
                                found_col = df_col
                                logging.info(f"✅ Payment due date match for '{req_col}': '{df_col}'")
                                break
                            elif ('due' in df_col_clean and 'date' in df_col_clean):
                                found_col = df_col
                                logging.info(f"✅ Due date match for '{req_col}': '{df_col}'")
                                break
                            elif ('payment' in df_col_clean and 'date' in df_col_clean and 
                                  not any(exclude in df_col_clean for exclude in ['transfer', 'created', 'create'])):
                                found_col = df_col
                                logging.info(f"✅ Payment date fallback match for '{req_col}': '{df_col}'")
                                break
                
                if found_col:
                    column_mapping[req_col] = found_col
                    logging.info(f"✅ Final mapping: '{req_col}' -> '{found_col}'")
                else:
                    logging.error(f"❌ Required column '{req_col}' not found")
                    logging.error(f"Available columns: {list(df.columns)}")
                    logging.info("First few rows of data:")
                    logging.info(df.head(3).to_string())
                    return None
            
            # Create a copy and rename columns
            df_work = df.copy()
            df_work = df_work.rename(columns=column_mapping)
            
            # Verify the required columns exist after renaming
            missing_cols = [col for col in required_columns if col not in df_work.columns]
            if missing_cols:
                logging.error(f"❌ Missing columns after renaming: {missing_cols}")
                return None
            
            logging.info("✅ Column mapping successful")
            
            # Remove completely empty rows
            df_work = df_work.dropna(subset=required_columns, how='all')
            
            # Filter for cheque payments
            df_work['Mode of Payment'] = df_work['Mode of Payment'].astype(str)
            cheque_mask = df_work['Mode of Payment'].str.lower().str.contains(
                'cheque|check', na=False, regex=True
            )
            cheque_df = df_work[cheque_mask].copy()
            
            if cheque_df.empty:
                logging.info("No cheque payments found")
                unique_payments = df_work['Mode of Payment'].value_counts()
                logging.info(f"Available payment modes: {unique_payments.to_dict()}")
                return pd.DataFrame()
            
            logging.info(f"Found {len(cheque_df)} cheque payments")
            
            # Parse dates with improved format handling
            date_formats = [
                '%d-%b-%y',    # 22-Feb-25
                '%d-%b-%Y',    # 22-Feb-2025
                '%d/%m/%y',    # 22/02/25
                '%d/%m/%Y',    # 22/02/2025
                '%Y-%m-%d',    # 2025-02-22
                '%m/%d/%Y',    # 02/22/2025
                '%d.%m.%Y',    # 22.02.2025
                '%Y/%m/%d',    # 2025/02/22
                '%d-%m-%y',    # 22-02-25
                '%d-%m-%Y'     # 22-02-2025
            ]
            
            parsed_dates = 0
            for date_format in date_formats:
                try:
                    temp_dates = pd.to_datetime(
                        cheque_df['Payment Due [Date]'], 
                        format=date_format, 
                        errors='coerce'
                    )
                    valid_count = temp_dates.notna().sum()
                    
                    if valid_count > parsed_dates:
                        cheque_df['Payment Due [Date]'] = temp_dates
                        parsed_dates = valid_count
                        logging.info(f"Parsed {valid_count} dates using format {date_format}")
                        
                        if parsed_dates == len(cheque_df):
                            break
                            
                except Exception as e:
                    logging.debug(f"Date format {date_format} failed: {e}")
                    continue
            
            # If no format worked well, try automatic parsing
            if parsed_dates < len(cheque_df) * 0.5:
                try:
                    cheque_df['Payment Due [Date]'] = pd.to_datetime(
                        cheque_df['Payment Due [Date]'], errors='coerce'
                    )
                    final_parsed = cheque_df['Payment Due [Date]'].notna().sum()
                    logging.info(f"Automatic parsing resulted in {final_parsed} valid dates")
                except Exception as e:
                    logging.error(f"Automatic date parsing failed: {e}")
            
            # Remove rows with invalid dates
            initial_count = len(cheque_df)
            cheque_df = cheque_df.dropna(subset=['Payment Due [Date]'])
            final_count = len(cheque_df)
            
            logging.info(f"Final: {final_count} cheque payments with valid dates (from {initial_count})")
            
            if final_count > 0:
                logging.info("Sample processed data:")
                sample_data = cheque_df[['Mode of Payment', 'Payment Due [Date]']].head(3)
                logging.info(sample_data.to_string())
                
                # Show date range
                min_date = cheque_df['Payment Due [Date]'].min()
                max_date = cheque_df['Payment Due [Date]'].max()
                logging.info(f"Date range: {min_date.strftime('%Y-%m-%d')} to {max_date.strftime('%Y-%m-%d')}")
            
            return cheque_df
            
        except Exception as e:
            logging.error(f"Failed to parse Excel file: {e}")
            import traceback
            logging.error(traceback.format_exc())
            return None
    
    def find_reminders_needed(self, df: pd.DataFrame) -> pd.DataFrame:
        """Find cheques that need reminders (3 days before payment due date)"""
        if df is None or df.empty:
            return pd.DataFrame()
        
        today = datetime.now().date()
        target_date = today + timedelta(days=3)
        
        reminders_needed = df[df['Payment Due [Date]'].dt.date == target_date].copy()
        
        logging.info(f"Today: {today}")
        logging.info(f"Checking for reminders needed for: {target_date}")
        logging.info(f"Found {len(reminders_needed)} reminders needed")
        
        if len(reminders_needed) > 0:
            logging.info("Reminders for:")
            for _, row in reminders_needed.iterrows():
                payment_mode = row.get('Mode of Payment', 'N/A')
                due_date = row['Payment Due [Date]'].strftime('%Y-%m-%d')
                logging.info(f"  - {payment_mode} on {due_date}")
        
        return reminders_needed
    
    def create_email_body(self, reminder_data: pd.DataFrame) -> str:
        """Create HTML email body"""
        html_body = """
        <html>
        <body>
            <h2>🏦 Cheque Payment Due Reminder</h2>
            <p>The following cheque payments are <strong>due in 3 days</strong>:</p>
            <table border="1" style="border-collapse: collapse; width: 100%; margin: 20px 0;">
                <thead>
                    <tr style="background-color: #f2f2f2;">
        """
        
        for col in reminder_data.columns:
            html_body += f"<th style='padding: 12px; text-align: left; font-weight: bold;'>{col}</th>"
        
        html_body += """
                    </tr>
                </thead>
                <tbody>
        """
        
        for i, (_, row) in enumerate(reminder_data.iterrows()):
            row_style = "background-color: #f9f9f9;" if i % 2 == 0 else ""
            html_body += f"<tr style='{row_style}'>"
            for col in reminder_data.columns:
                value = row[col]
                if pd.isna(value):
                    value = ""
                elif col == 'Payment Due [Date]' and hasattr(value, 'strftime'):
                    value = value.strftime('%d-%b-%Y')
                else:
                    value = str(value)
                html_body += f"<td style='padding: 10px; border-bottom: 1px solid #ddd;'>{value}</td>"
            html_body += "</tr>"
        
        today_str = datetime.now().strftime('%d-%b-%Y')
        target_str = (datetime.now() + timedelta(days=3)).strftime('%d-%b-%Y')
        
        html_body += f"""
                </tbody>
            </table>
            <div style="margin-top: 20px; padding: 15px; background-color: #e8f4fd; border-left: 4px solid #2196F3;">
                <p><strong>📅 Reminder Date:</strong> {today_str}</p>
                <p><strong>🎯 Payment Due Date:</strong> {target_str}</p>
            </div>
            <br>
            <p style="color: #666; font-size: 14px;">
                <em>⚡ This is an automated reminder generated from your SharePoint file.</em><br>
                <em>Please ensure these cheque payments are processed on time to avoid any delays.</em>
            </p>
        </body>
        </html>
        """
        
        return html_body
    
    def send_email_reminder(self, reminder_data: pd.DataFrame) -> bool:
        """Send email reminder to multiple recipients"""
        if reminder_data.empty:
            logging.info("No reminders to send")
            return True
        
        success_count = 0
        
        for recipient_email in self.recipient_emails:
            try:
                msg = MIMEMultipart()
                msg['From'] = self.email_username
                msg['To'] = recipient_email
                msg['Subject'] = f"Cheque Payment Due Reminder - {len(reminder_data)} payment(s) due in 3 days"
                
                body = self.create_email_body(reminder_data)
                msg.attach(MIMEText(body, 'html'))
                
                with smtplib.SMTP(self.smtp_server, self.smtp_port) as server:
                    server.starttls()
                    server.login(self.email_username, self.email_password)
                    server.send_message(msg)
                
                logging.info(f"✅ Email reminder sent successfully to {recipient_email}")
                success_count += 1
                
            except Exception as e:
                logging.error(f"Failed to send email to {recipient_email}: {e}")
        
        if success_count > 0:
            logging.info(f"✅ Email reminders sent to {success_count}/{len(self.recipient_emails)} recipients")
            return True
        else:
            logging.error("❌ Failed to send emails to all recipients")
            return False
    
    def run_reminder_check(self) -> bool:
        """Main method to run the reminder check"""
        logging.info("🚀 Starting SharePoint cheque payment due reminder check...")
        logging.info(f"Current time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        
        excel_file = self.download_excel_file()
        if not excel_file:
            logging.error("❌ Failed to download Excel file")
            return False
        
        df = self.parse_excel_data(excel_file)
        if df is None:
            logging.error("❌ Failed to parse Excel data")
            return False
        
        reminders = self.find_reminders_needed(df)
        
        if not reminders.empty:
            success = self.send_email_reminder(reminders)
            if success:
                logging.info("✅ Reminder email sent successfully")
            else:
                logging.error("❌ Failed to send reminder email")
            return success
        else:
            logging.info("ℹ️  No cheque payment due reminders needed today")
            return True


def main() -> bool:
    """Main function for Render Cron Job"""
    logging.info("📋 SharePoint Cheque Payment Due Reminder Starting on Render...")
    
    # Parse recipient emails (comma-separated)
    recipient_emails_str = os.getenv('RECIPIENT_EMAILS', '')
    recipient_emails = [email.strip() for email in recipient_emails_str.split(',') if email.strip()]
    
    if not recipient_emails:
        logging.error("❌ No recipient emails provided")
        return False
    
    config = {
        'sharepoint_shared_url': os.getenv('SHAREPOINT_SHARED_URL'),
        'smtp_server': os.getenv('SMTP_SERVER'),
        'smtp_port': int(os.getenv('SMTP_PORT', '587')),
        'email_username': os.getenv('EMAIL_USERNAME'),
        'email_password': os.getenv('EMAIL_PASSWORD'),
        'recipient_emails': recipient_emails
    }
    
    missing_config = [key for key, value in config.items() if value is None or (isinstance(value, str) and value == '') or (isinstance(value, list) and not value)]
    if missing_config:
        logging.error(f"❌ Missing required environment variables: {missing_config}")
        return False
    
    logging.info("✅ Configuration loaded successfully")
    logging.info(f"Recipients: {', '.join(recipient_emails)}")
    
    reminder_system = SharePointSharedLinkReminder(**config)
    success = reminder_system.run_reminder_check()
    
    if success:
        logging.info("🎉 Reminder check completed successfully")
    else:
        logging.error("💥 Reminder check failed")
        
    return success


# Health check endpoint for Render
def health_check():
    """Simple health check function"""
    return {"status": "healthy", "timestamp": datetime.now().isoformat()}


if __name__ == "__main__":
    success = main()
    exit(0 if success else 1)