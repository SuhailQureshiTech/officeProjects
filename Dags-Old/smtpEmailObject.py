import smtplib
from email.mime.text import MIMEText

def smtpSendEmail(from_,recipients_,subject_,body_):

    s = smtplib.SMTP('mail.iblgroup.biz')
    # s.set_debuglevel(1)
    # msg = MIMEText("""body""")
    msg = MIMEText(f'''{body_} ''')
    # sender = 'me@example.com'
    sender = from_

    # recipients = 'Muhammad.Younus@iblgrp.com,muhammad.suhail@iblgrp.com'
    recipients =recipients_

    # msg['Subject'] = "subject line"
    msg['Subject'] = subject_
    msg['From'] = sender
    msg['To'] = recipients
    s.sendmail(sender, recipients.split(','), msg.as_string())


smtpSendEmail('dna@iblgrp.com', 'shehzad.lalani@iblgrp.com,muhammad.arslan@iblgrp.com'
             ,'Sales Merging','Hello')




