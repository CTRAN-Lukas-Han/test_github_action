import smtplib
from email.message import EmailMessage

SMTPserver='relay.c-tran.org'
serviceAccount = 'C-TRAN System <PowerBI@ctran365.onmicrosoft.com>'

def send_email(recipient, subject, body):
    msg = EmailMessage()
    msg.set_content(body)
    msg['Subject'] = subject
    msg['To'] = recipient
    msg['From'] = serviceAccount

    s = smtplib.SMTP(SMTPserver)
    s.send_message(msg)
    s.quit()

