import argparse
import psycopg2
import pathos.multiprocessing as mp
import time
import os


"""CONNECTION PARAMETERS"""
USER = "cward-ro"
PWD = ""
URL = ""
PORT = "5432"


def execute_sql(db):
	t0 = time.time()
	def do_work(query):
		filename = 'results/results_%s.csv' % query[0]
		if not os.path.exists(os.path.dirname(filename)):
			os.makedirs(os.path.dirname(filename))

		try:
			conn = psycopg2.connect(database=db, user=USER, password=PWD, host=URL, port=PORT)
			cur = conn.cursor('py_cw_cur')
			#cur.execute(query[1])
			#print "Writing results for %s." % query[0]

			outputquery = 'copy ({0}) to stdout with csv header'.format(query[1])

			with open(filename, 'w') as f:
				cur.copy_expert(outputquery, f)

			cur.close()
			conn.close()
		except psycopg2.Error as e:
			print e.pgerror
			pass



	schema_query = """
		SELECT nspname FROM pg_namespace WHERE nspname LIKE 'cluster%' order by random() limit 10;
	"""
	queries = []
	try:
		conn = psycopg2.connect(database=db, user=USER, password=PWD, host=URL, port=PORT)
		cur = conn.cursor('py_cw_cur')
		cur.execute(schema_query)
		schemas = cur.fetchall()
		cur.close()
		conn.close()

		if len(schemas) > 0:
			for schema in schemas:
				base_query = """
					select '%s' as cluster, '%s' as namespace, id
					, points_possible
					, shuffle_answers
					, show_correct_answers
					, time_limit
					, allowed_attempts
					, scoring_policy
					, quiz_type
					, could_be_locked
					, date(created_at) as created_date
					, date(updated_at) as update_date
					, date(lock_at) as lock_date
					, date(unlock_at) as unlock_date
					, access_code
					, unpublished_question_count
					, date(due_at) as due_date
					, question_count
					, date(published_at) as published_date
					, date(last_edited_at) as last_edit_date
					, anonymous_submissions
					, hide_results
					, ip_filter
					, require_lockdown_browser
					, require_lockdown_browser_for_results
					, one_question_at_a_time
					, cant_go_back
					, date(show_correct_answers_at) as show_correct_answers_date
					, date(hide_correct_answers_at) as hide_correct_answers_date
					, require_lockdown_browser_monitor
					, lockdown_browser_monitor_data
					, only_visible_to_overrides
					, one_time_results
					, show_correct_answers_last_attempt
					, quiz_data
				from %s.quizzes
				where workflow_state in ('available')
				order by random()
				limit 1000
				""" % (db[:-2], schema[0], schema[0])
				queries.append((schema[0],base_query))

		else:
			base_query = """
				select '%s' as cluster, '%s' as namespace, id
				, points_possible
				, shuffle_answers
				, show_correct_answers
				, time_limit
				, allowed_attempts
				, scoring_policy
				, quiz_type
				, could_be_locked
				, date(created_at) as created_date
				, date(updated_at) as update_date
				, date(lock_at) as lock_date
				, date(unlock_at) as unlock_date
				, access_code
				, unpublished_question_count
				, date(due_at) as due_date
				, question_count
				, date(published_at) as published_date
				, date(last_edited_at) as last_edit_date
				, anonymous_submissions
				, hide_results
				, ip_filter
				, require_lockdown_browser
				, require_lockdown_browser_for_results
				, one_question_at_a_time
				, cant_go_back
				, date(show_correct_answers_at) as show_correct_answers_date
				, date(hide_correct_answers_at) as hide_correct_answers_date
				, require_lockdown_browser_monitor
				, lockdown_browser_monitor_data
				, only_visible_to_overrides
				, one_time_results
				, show_correct_answers_last_attempt
				, quiz_data
			from canvas.quizzes
			where workflow_state in ('available')
			order by random()
			limit 1000
			""" % (db[:-2], db[:-2])
			queries.append((db,base_query))

		cur.close()
		conn.close()

		for query in queries:
			do_work(query)

	except psycopg2.Error as e:
		pass

	t1 = time.time()
	print db[:-2], t1-t0



parser = argparse.ArgumentParser(description='Pulls data from Canvas production PostgreSQL DB and writes to CSV file.')
parser.add_argument('--version', action='version', version='%(prog)s v. 0.1')


dbs = []
for i in range(1,52):
	dbs.append("cluster" + str(i) + "dr")

num_cores = mp.cpu_count()
pool = mp.Pool(num_cores)

pool.map(execute_sql, dbs)
pool.close()
pool.join()
