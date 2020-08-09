import logging
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

'''
Sends email using Gmail SMTP
'''

class Emailer():
	def __init__(self, sender_address, sender_pass, receiver_address):
		#The mail addresses and password
		self.sender_address = sender_address
		self.sender_pass = sender_pass
		self.receiver_address = receiver_address

	def send(self, email_content):
		#Setup the MIME
		message = MIMEMultipart()
		message['From'] = self.sender_address
		message['To'] = self.receiver_address
		message['Subject'] = 'Algo Error'   #The subject line
		#The body and the attachments for the mail
		message.attach(MIMEText(email_content, 'plain'))
		#Create SMTP session for sending the mail
		session = smtplib.SMTP('smtp.gmail.com', 587) #use gmail with port
		session.starttls() #enable security
		session.login(self.sender_address, self.sender_pass) #login with mail_id and password
		text = message.as_string()
		session.sendmail(self.sender_address, self.receiver_address, text)
		session.quit()
		logging.info("Mail sent: {}".format(email_content))

#Emailer('test message')
