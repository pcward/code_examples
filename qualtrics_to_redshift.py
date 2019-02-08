import requests
import json
import psycopg2
import datetime as dt
import boto
import string

"""CONNECTION PARAMETERS"""
USER = "cward"
PWD = ""
URL = ""
PORT = "5493"
DB = "dev_analytics"

AWS_KEY = ''
AWS_SECRET = ''
AWS_BUCKET = 'cward'

lms_dict = {
    2: "Blackboard (any version)",
    3: "Desire2Learn",
    4: "Moodle",
    5: "Pearson eCollege",
    6: "Sakai",
    7: "WebCT",
    8: "Other",
    9: "None"
}


def read_json(survey_responses, student=True, old_student_survey=False):
    db_conn = psycopg2.connect(database=DB, user=USER, password=PWD, host=URL, port=PORT)
    db_cur = db_conn.cursor()

    query = "SELECT * FROM dept_product.qualtrics_nps_survey_domains;"
    db_cur.execute(query)
    domains = dict(db_cur.fetchall())

    db_cur.close()
    db_conn.close()

    survey_data = {}
    for r in survey_responses:
        if student and survey_responses[r]["Q2"] == 2 and not old_student_survey:
            continue
        elif student and not old_student_survey and survey_responses[r]["Q1"] > 2:
            continue

        survey_data[r] = {}
        survey_data[r]["survey_date"] = survey_responses[r]["StartDate"]
        survey_data[r]["update_date"] = dt.datetime.today().strftime("%Y-%m-%d")
        survey_data[r]["domain"] = survey_responses[r]["domain"]

        if survey_data[r]["domain"] not in domains:
            db_conn = psycopg2.connect(database=DB, user=USER, password=PWD, host=URL, port=PORT)
            db_cur = db_conn.cursor()

            query = "INSERT INTO dept_product.qualtrics_nps_survey_domains (domain, first_survey_date) VALUES (\'%s\', \'%s\')" % (survey_data[r]["domain"], survey_data[r]["survey_date"])
            db_cur.execute(query)
            db_cur.close()
            db_conn.commit()
            db_conn.close()

        if student:
            if old_student_survey:
                survey_data[r]["survey_type"] = "Student"

                try:
                    int(survey_responses[r]["Q1"])
                    survey_data[r]["nps"] = survey_responses[r]["Q1"]
                except ValueError:
                    survey_data[r]["nps"] = -500

                survey_data[r]["nps_comment"] = survey_responses[r]["Q2"]

                if survey_responses[r]["Q5"] in lms_dict:
                    survey_data[r]["previous_lms"] = survey_responses[r]["Q5"]
                    try:
                        int(survey_responses[r]["Q6"])
                        survey_data[r]["previous_nps"] = survey_responses[r]["Q6"]
                    except ValueError:
                        survey_data[r]["previous_nps"] = -500

                else:
                    survey_data[r]["previous_lms"] = "None"
                    survey_data[r]["previous_nps"] = -500

                survey_data[r]["canvas_experience"] = survey_responses[r]["Q4"]

            else:
                if survey_responses[r]["Q1"] == 1:
                    survey_data[r]["survey_type"] = "Student"
                elif survey_responses[r]["Q1"] == 2:
                    survey_data[r]["survey_type"] = "Teacher"

                try:
                    int(survey_responses[r]["Q3"])
                    survey_data[r]["nps"] = survey_responses[r]["Q3"]
                except ValueError:
                    survey_data[r]["nps"] = -500

                survey_data[r]["nps_comment"] = survey_responses[r]["Q4"]

                if survey_responses[r]["Q7"] in lms_dict:
                    survey_data[r]["previous_lms"] = survey_responses[r]["Q7"]
                    try:
                        int(survey_responses[r]["Q8"])
                        survey_data[r]["previous_nps"] = survey_responses[r]["Q8"]
                    except ValueError:
                        survey_data[r]["previous_nps"] = -500

                else:
                    survey_data[r]["previous_lms"] = "None"
                    survey_data[r]["previous_nps"] = -500

                survey_data[r]["canvas_experience"] = survey_responses[r]["Q6"]

        else:
            survey_data[r]["survey_type"] = "Teacher"
            try:
                int(survey_responses[r]["Q1"])
                survey_data[r]["nps"] = survey_responses[r]["Q1"]
            except ValueError:
                survey_data[r]["nps"] = -500

            survey_data[r]["nps_comment"] = survey_responses[r]["Q2"]

            if len(survey_responses[r]["Q11"]) > 0:
                plms = int(survey_responses[r]["Q11"])
                if plms in lms_dict:
                    survey_data[r]["previous_lms"] = lms_dict[plms]
                    try:
                        int(survey_responses[r]["Q15"])
                        survey_data[r]["previous_nps"] = survey_responses[r]["Q15"]
                    except ValueError:
                        survey_data[r]["previous_nps"] = -500
            else:
                survey_data[r]["previous_lms"] = "None"
                survey_data[r]["previous_nps"] = -500

            survey_data[r]["canvas_experience"] = survey_responses[r]["Q7"]
    return survey_data


def parse(requests_params, student=True, old_student_survey=False):
    if student:
        if old_student_survey:
            survey_id = 'SV_7WE8EZpkywznr3n'
        else:
            survey_id = 'SV_5u4jzFvCkXGqhMh'  # old student survey ID: 'SV_7WE8EZpkywznr3n'        #{NPS Student: SV_7WE8EZpkywznr3n; NPS Teacher: SV_3qH660ku8qXC1ZH}
    else:
        survey_id = 'SV_3qH660ku8qXC1ZH'

    if survey_id == '':
        print "Provide survey ID."
        exit()
    else:
        requests_params["SurveyID"] = survey_id

    r = requests.post(
        "https://survey.qualtrics.com//WRAPI/ControlPanel/api.php?API_SELECT=ControlPanel&Request=getLegacyResponseData",
        params=requests_params)
    survey_responses = r.json()

    if student:
        if old_student_survey:
            survey_data = read_json(survey_responses, old_student_survey=True)
            print "API data pull complete. %d student responses from old survey received." % len(survey_responses)
        else:
            survey_data = read_json(survey_responses)
            print "API data pull complete. %d student responses received." % len(survey_responses)
    else:
        survey_data = read_json(survey_responses, student=False)
        print "API data pull complete. %d teacher responses received." % len(survey_responses)

    s3 = boto.connect_s3(AWS_KEY, AWS_SECRET)
    s3_bucket = s3.get_bucket(AWS_BUCKET)
    s3_key = s3_bucket.get_key('qualtrics_nps_survey_responses.json')

    if s3_key is None:
        s3_key = s3_bucket.new_key('qualtrics_nps_survey_responses.json')
        s3_key.content_type = 'application/json'

    parsed_for_s3 = []
    for key in survey_data.keys():
        parsed_for_s3.append(survey_data[key])

    json_obj = json.dumps(parsed_for_s3)
    json_obj_aws = string.replace(json_obj, "}, ", "}")
    json_obj_aws = json_obj_aws[1:len(json_obj_aws) - 1]

    s3_key.set_contents_from_string(json_obj_aws, replace=True)
    print "\tAWS S3 upload complete."

    db_conn2 = psycopg2.connect(database=DB, user=USER, password=PWD, host=URL, port=PORT)
    cur = db_conn2.cursor()

    query = "copy dept_product.qualtrics_nps_survey_responses from 's3://cward/qualtrics_nps_survey_responses.json' credentials 'aws_access_key_id=%s;aws_secret_access_key=%s' json 'auto';" % (
    AWS_KEY, AWS_SECRET)
    cur.execute(query)
    db_conn2.commit()
    cur.close()
    db_conn2.close()
    print "\tRedshift ETL complete."


user_data = {"User": "theadmin", "Token": "BZaff8LMeHAuMVMTzlKdGvPscnhj2ml9IhwrvAp8", "Format": "JSON",
             "Version": "2.5"}

limit = 0  # set to 0 for no limit

conn = psycopg2.connect(database=DB, user=USER, password=PWD, host=URL, port=PORT)
cur = conn.cursor()
cur.execute('SELECT MAX(update_date) FROM dept_product.qualtrics_nps_survey_responses;')
max_date = cur.fetchall()
cur.close()
conn.close()

if max_date[0][0] is None:
    start_date = '2014-08-20 00:00:00'
else:
    start_date = (max_date[0][0] + dt.timedelta(days=1)).strftime("%Y-%m-%d 00:00:00")

user_data["StartDate"] = start_date
user_data["EndDate"] = dt.datetime.today().strftime("%Y-%m-%d 23:59:59")

print "Pulling data from %s onward." % start_date

if limit > 0:
    user_data["Limit"] = str(limit)

parse(user_data, student=True)
parse(user_data, student=False)
#parse(user_data, old_student_survey=True)
