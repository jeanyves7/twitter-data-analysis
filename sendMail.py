import pandas as pd,os
from app import conn
from Rds_Handle import update_requested_study, get_hash_id
from sendgrid import SendGridAPIClient
from sendgrid.helpers.mail import Mail

SENDGRID_API_KEY = os.getenv('sendGripKey')

def send_mail(study):
    try:
        conn.commit()
        query = "SELECT email FROM update_requested_study where study='{}'".format(study)
        df = pd.read_sql(sql=query, con=conn)
        list_mail = []
        hashed = get_hash_id(study, conn)

        # getting the emails of the users that requested this study
        for i in range(len(df)):
            e = df.iloc[i]
            list_mail.append(e[0])
        html = "https://mdp-data-analysis.web.app/study/{}".format(str(hashed))

        # building the html content to the mail
        message = Mail(
            from_email='mdptwitter20@hotmail.com',
            to_emails=list_mail,
            subject='Report Generated',
            html_content='You can review the report of your study via this link \n {}'.format(html)
        )
        # sending the mail to the users
        sg = SendGridAPIClient(SENDGRID_API_KEY)
        sg.send(message)
        update_requested_study(study=study, insert=False, conn=conn)
    except Exception as e:
        print("error occurred: in ", e.__str__())
