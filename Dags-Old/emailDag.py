# -*- coding: utf-8 -*-
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

from past.builtins import basestring

import importlib
import os
import smtplib

from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.application import MIMEApplication
from email.utils import formatdate

from airflow import configuration
from airflow.exceptions import AirflowConfigException
from airflow.utils.log.logging_mixin import LoggingMixin
import airflow
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, date
from airflow.operators.email import EmailOperator

default_args = {
    'owner': 'admin',
    'start_date': datetime(2023, 5, 7),
    # 'end_date': datetime(),
    'depends_on_past': False,
    'email': ['shehzad.lalani@iblgrp.com'],
    'email_on_failure': True,
    # 'email_on_retry': False,
    # If a task fails, retry it once after waiting
    # at least 5 minutes
    # 'retries': 1,
    # 'retry_delay': timedelta(minutes=5),
}

emailAlert = DAG(
    dag_id='emailDag',
    default_args=default_args,
    catchup=False,
    # schedule_interval='0 10 * * *',
    schedule_interval=None,
    # dagrun_timeout=timedelta(minutes=60),
    # description='Markitt Alerts',
    # on_success_callback=send_email,
    # on_failure_callback=send_email_failure,
    max_active_runs=1
)


def send_email(to, subject, html_content,
                    files=None, dryrun=False, cc=None, bcc=None,
                    mime_subtype='mixed', mime_charset='us-ascii', **kwargs):
    """
    Send email using backend specified in EMAIL_BACKEND.
    """
    path, attr = configuration.conf.get(
        'email', 'EMAIL_BACKEND').rsplit('.', 1)
    module = importlib.import_module(path)
    backend = getattr(module, attr)
    to = get_email_address_list(to)
    to = ", ".join(to)

    return backend(to, subject, html_content, files=files,
                dryrun=dryrun, cc=cc, bcc=bcc,
                mime_subtype=mime_subtype, mime_charset=mime_charset, **kwargs)


def send_email_smtp(to, subject, html_content, files=None,
                    dryrun=False, cc=None, bcc=None,
                    mime_subtype='mixed', mime_charset='us-ascii',
                    **kwargs):
    """
    Send an email with html content

    >>> send_email('test@example.com', 'foo', '<b>Foo</b> bar', ['/dev/null'], dryrun=True)
    """
    smtp_mail_from = configuration.conf.get('smtp', 'SMTP_MAIL_FROM')

    to = get_email_address_list(to)

    msg = MIMEMultipart(mime_subtype)
    msg['Subject'] = subject
    msg['From'] = smtp_mail_from
    msg['To'] = ", ".join(to)
    recipients = to
    if cc:
        cc = get_email_address_list(cc)
        msg['CC'] = ", ".join(cc)
        recipients = recipients + cc

    if bcc:
        # don't add bcc in header
        bcc = get_email_address_list(bcc)
        recipients = recipients + bcc

    msg['Date'] = formatdate(localtime=True)
    mime_text = MIMEText(html_content, 'html', mime_charset)
    msg.attach(mime_text)

    for fname in files or []:
        basename = os.path.basename(fname)
        with open(fname, "rb") as f:
            part = MIMEApplication(
                f.read(),
                Name=basename
            )
            part['Content-Disposition'] = 'attachment; filename="%s"' % basename
            part['Content-ID'] = '<%s>' % basename
            msg.attach(part)

    send_MIME_email(smtp_mail_from, recipients, msg, dryrun)

def send_MIME_email(e_from, e_to, mime_msg, dryrun=False):
    log = LoggingMixin().log

    SMTP_HOST = configuration.conf.get('smtp', 'SMTP_HOST')
    SMTP_PORT = configuration.conf.getint('smtp', 'SMTP_PORT')
    SMTP_STARTTLS = configuration.conf.getboolean('smtp', 'SMTP_STARTTLS')
    SMTP_SSL = configuration.conf.getboolean('smtp', 'SMTP_SSL')
    SMTP_USER = None
    SMTP_PASSWORD = None

    try:
        SMTP_USER = configuration.conf.get('smtp', 'SMTP_USER')
        SMTP_PASSWORD = configuration.conf.get('smtp', 'SMTP_PASSWORD')
    except AirflowConfigException:
        log.debug(
            "No user/password found for SMTP, so logging in with no authentication.")

    if not dryrun:
        s = smtplib.SMTP_SSL(SMTP_HOST, SMTP_PORT) if SMTP_SSL else smtplib.SMTP(
            SMTP_HOST, SMTP_PORT)
        if SMTP_STARTTLS:
            s.starttls()
        if SMTP_USER and SMTP_PASSWORD:
            s.login(SMTP_USER, SMTP_PASSWORD)
        log.info("Sent an alert email to %s", e_to)
        s.sendmail(e_from, e_to, mime_msg.as_string())
        s.quit()

def get_email_address_list(address_string):
    if isinstance(address_string, basestring):
        if ',' in address_string:
            address_string = [address.strip()
                              for address in address_string.split(',')]
        elif ';' in address_string:
            address_string = [address.strip()
                              for address in address_string.split(';')]
        else:
            address_string = [address_string]

    return address_string

# def sendEmail():
#     send_email_smtp('muhammad.suhail@iblgrp.com','email dag...','test email html',None,None,'orasyscojava@gmail.com',None,'dddd',None)

# markittAltersItems = PythonOperator(
#     task_id="EmailDag",
#     python_callable=sendEmail,
#     dag=dagAlert
# )


send_email = EmailOperator(
    task_id='send_email',
    to='muhammad.suhail@iblgrp.com',
    cc='orasyscojava@gmail.com',
    subject='ingestion complete',
    html_content="Date: {{ ds }}",
    dag=emailAlert)
