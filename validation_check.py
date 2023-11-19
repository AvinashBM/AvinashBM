import psycopg2
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

# PostgreSQL database connection parameters
db_params = {
    'dbname': 'your_database_name',
    'user': 'your_username',
    'password': 'your_password',
    'host': 'your_host',
    'port': 'your_port'
}

# Email configuration
sender_email = 'your_sender_email@gmail.com'
sender_password = 'your_sender_password'
recipient_emails = ['user1@example.com', 'user2@example.com']
subject = 'Query Results'
body = 'Please find attached the query results.'

# Database query
query = """
SELECT * FROM your_table;
"""

# Connect to the database
try:
    conn = psycopg2.connect(**db_params)
except psycopg2.Error as e:
    print("Error connecting to the database:", e)
    exit(1)

# Execute the query and fetch results
try:
    cursor = conn.cursor()
    cursor.execute(query)
    results = cursor.fetchall()
except psycopg2.Error as e:
    print("Error executing the query:", e)
    conn.close()
    exit(1)

# Save query results to a file
with open('query_results.txt', 'w') as file:
    for row in results:
        file.write(', '.join(map(str, row)) + '\n')

# Send email with the query results as an attachment
message = MIMEMultipart()
message['From'] = sender_email
message['To'] = ', '.join(recipient_emails)
message['Subject'] = subject

# Attach the query results file
with open('query_results.txt', 'r') as file:
    attachment = MIMEText(file.read())
attachment.add_header('Content-Disposition', 'attachment', filename='query_results.txt')
message.attach(attachment)

# Send the email
try:
    server = smtplib.SMTP('smtp.gmail.com', 587)
    server.starttls()
    server.login(sender_email, sender_password)
    server.sendmail(sender_email, recipient_emails, message.as_string())
    server.quit()
    print("Email sent successfully.")
except smtplib.SMTPException as e:
    print("Error sending email:", e)

# Close the database connection
conn.close()
