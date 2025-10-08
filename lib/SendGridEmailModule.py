"""##################################################################
# Script        : SendGridEmailModule.py
# Description   : This module uses the SendGrid API to emails to recipients defined 
#                 in the ENV.cfg file.
# Modifications 
# 1/7/2024      : Added header comments
#####################################################################"""
import base64
import os

from sendgrid import SendGridAPIClient, Attachment, FileContent, Mail, Email
from lib.Variables import Variables

class SendGridEmailModule:
    __sendgrid_api_key = os.environ.get('SENDGRID_API_KEY')
    __email_from = os.environ.get('EMAIL_FROM')
    __email_from_name = os.environ.get('EMAIL_FROM_NAME')

    if not __email_from_name:
        __email_from_name = 'Robling DevOps'

    v = Variables("ENV.cfg")
    __default_recipient_email_list = v.get('DEFAULT_EMAIL_RECIPIENT').split(',')
    
    @classmethod
    def send_email(cls, subject: str, msg: str, recipient_email_list=[]):
        if not recipient_email_list:
            recipient_email_list = cls.__default_recipient_email_list

        message = Mail(
            from_email=Email(email=cls.__email_from, name=cls.__email_from_name),
            to_emails=recipient_email_list,
            subject=subject,
            html_content=msg)
        print('SG API KEY',cls.__sendgrid_api_key)
        sg_client = SendGridAPIClient(cls.__sendgrid_api_key)
        sg_client.send(message)

    @classmethod
    def send_email_with_attachment(cls, subject: str, msg: str, attachment_list: [], recipient_email_list=[]):
        if not recipient_email_list:
            recipient_email_list = cls.__default_recipient_email_list

        message = Mail(
            from_email=Email(email=cls.__email_from, name=cls.__email_from_name),
            to_emails=recipient_email_list,
            subject=subject,
            html_content=msg)

        for attachment_iterator in attachment_list:
            sg_attachment = Attachment()
            with open(attachment_iterator, "rb") as attachment:
                data = attachment.read()
                encoded_data = base64.b64encode(data).decode()

            sg_attachment.file_content = FileContent(encoded_data)
            sg_attachment.file_name = os.path.basename(attachment_iterator)
            message.add_attachment(sg_attachment)

        sg_client = SendGridAPIClient(cls.__sendgrid_api_key)
        sg_client.send(message)
        
