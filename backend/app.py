import requests
import subprocess

import os
import pymysql
from flask import Flask, jsonify
from flask_cors import CORS, cross_origin
from sendgrid import SendGridAPIClient
from sendgrid.helpers.mail import Mail

from .Rds_Handle import (
    get_available_search_engine,
    ongoing_search,
    update_ongoing_search,
    post_waiting_query,
    update_requested_study,
    get_date,
    update_previous_study,
    get_previous_studies,
    get_analysed_study,
)


SENDGRID_API_KEY = os.getenv('sendGripKey')

message = Mail(
    from_email='mdptwitter20@hotmail.com',
    to_emails=['jano2_youssef3@outlook.com', 'jean_yves2017@outlook.com'],
    subject='Sending with Twilio SendGrid is Fun',
    html_content='<strong>and easy to do anywhere, even with Python</strong> <link>https://mdp-data-analysis.web.app/ </link>')

app = Flask(__name__)
app.config['SEND_FILE_MAX_AGE_DEFAULT'] = 0
cors = CORS(app, resources={r"/*": {"origins": "*"}})



host = os.getenv('host_RDS')
PORT = int(os.getenv('RDS_port'))
USER = os.getenv('RDS_user')
PASS = os.getenv('RDS_pass')
REGION = os.getenv('RDS_region')
DBNAME = os.getenv('RDS_db')


mail_settings = {
    "MAIL_SERVER": 'smtp.gmail.com',
    "MAIL_PORT": 465,
    "MAIL_USE_TLS": False,
    "MAIL_USE_SSL": True,
    "MAIL_USERNAME": 'mdptwitter20@gmail.com',
    "MAIL_PASSWORD": os.getenv('RDS_pass')
}

app.config.update(mail_settings)

conn = pymysql.connect(host=host, user=USER, port=PORT, passwd=PASS, database=DBNAME)
sg = SendGridAPIClient(SENDGRID_API_KEY)


@app.route('/')
@cross_origin()
def hello_world():
    try:
        print(get_date())
    except Exception as e:
        print(e.__str__())

    user = os.getenv('KEY')
    term = "THE USER VARIABLE IS {}".format(user)
    return 'Hello World! {}'.format(term)


@app.route('/search/<entity>/<email>/<date>')
@cross_origin()
def search(entity, email, date):
    conn.commit()
    try:
        if not ongoing_search(conn, check_email=False, name=entity):
            if not ongoing_search(conn, check_email=True, email=email):
                per = get_available_search_engine(conn)
                if per is not None:
                    update_ongoing_search(entity, conn, per, email)
                    update_requested_study(study=entity, email=email, insert=True, conn=conn)
                    update_previous_study(study=entity, report=False, start=True, conn=conn)
                    print(entity, " ", email)
                    stop_date = date
                    subprocess.Popen(
                        'python backend/SearchTweets.py {} {} {} {}'.format(
                            entity, email, stop_date, per
                        ),
                        shell=True,
                    )
                    return jsonify(
                        {
                            "message": "Your request has been successfully submitted a custom link will be sent to you by email once the results are ready!",
                            "status": "success"}), 200
                else:
                    post_waiting_query(email, entity, date, conn)
                    return jsonify({
                                       "message": "All our search engines are busy! However, we added your request to the studies queue it will be processed once an engine is available and a custom link with the search results will be sent to you by email",
                                       "status": "warning"}), 200
            else:
                return jsonify({
                                   "message": "You already requested a study that has not been completed yet, please request a new one once the current request is done".format(
                                       email), "status": "error"}), 200
        else:
            update_requested_study(study=entity, email=email, insert=True, conn=conn)
            return jsonify({
                               "message": "Your request has already been launched by another user, we will send you a custom email when it is completed",
                               "status": "success"}), 200

    except Exception as e:
        return jsonify({"message": "Internal error, please try again later", "status": "error"}), 404


@app.route('/recaptcha/<token>')
@cross_origin()
def verifyRecaptcha(token):
    try:
        headers = {"Accept": "application/json", "Content-Type": "application/x-www-form-urlencoded; charset=utf-8"}
        data = {"secret": os.getenv('secret_recaptcha'), "response": str(token)}
        res = requests.post("https://www.google.com/recaptcha/api/siteverify", headers=headers, data=data)
        if res.json()["success"]:
            return jsonify({"success": "true"}), 200
        elif not (res.json()["success"]):
            return jsonify({"success": "false", "error-codes": res.json()["error-codes"]}), 400
        else:
            raise Exception("Bad request to google API")
    except Exception as e:
        return jsonify({"message": "Internal error, please try again later"}), 404


@app.route('/studies')
@cross_origin()
def get_studies():
    conn.commit()
    try:
        data = get_previous_studies(conn)
        print(data)
        return jsonify(data), 200
    except Exception as e:
        return jsonify({"message": "Internal error, please try again later {}".format(e)}), 404


@app.route('/study/<study_id>')
@cross_origin()
def get_study(study_id):
    conn.commit()
    try:
        data = get_analysed_study(study_id, conn)
        print(data)
        return jsonify(data), 200
    except Exception as e:
        return jsonify({"message": "Internal error, please try again later {}".format(e)}), 404




if __name__ == '__main__':
    app.run(debug=True)
