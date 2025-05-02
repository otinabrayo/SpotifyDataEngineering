import smtplib
from email.mime.text import MIMEText
from dotenv import load_dotenv
import os

# Load environment variables
load_dotenv()

MAIL_USERNAME = os.getenv("MAIL_USERNAME")
MAIL_PASSWORD = os.getenv("MAIL_PASSWORD")
MAIL_RECEIVER = os.getenv("MAIL_RECEIVER")


def EmailTrigger(subject: str, body:str):
# SMTP configuration
    smtp_host = "smtp.gmail.com"
    smtp_port = 587
    smtp_user = MAIL_USERNAME
    smtp_password = MAIL_PASSWORD
    smtp_receiver = MAIL_RECEIVER

    # Create a test email
    msg = MIMEText(body)
    msg["Subject"] = subject
    msg["From"] = smtp_user
    msg["To"] = smtp_receiver

    # Send the email
    try:
        with smtplib.SMTP(smtp_host, smtp_port) as server:
            server.starttls()  # Enable TLS encryption
            server.login(smtp_user, smtp_password)
            server.sendmail(smtp_user, smtp_receiver, msg.as_string())
            server.log.info(f"Sending email to: {smtp_receiver}, subject: {subject}")
        print('...Email sent')
    except Exception as e:
        print(f'!!! Email not sent: {e}')
