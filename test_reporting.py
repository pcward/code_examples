##	Imports Optimizely experiment summary results data and outputs Excel financial impact calculations with statistical significance/confidence intervals
##
##	Created by:  Chris Ward
##				chris.ward@clearlink.com
##
##	Last edit:	2015-02-13
##				Added option to check whether sample size requirement has been met
##				Pull visit numbers from Optimizely, not phone rotation
##				Changed browser URL to https://reporting.clearlink.com/dashboard.php
##				Re-wrote data pull to pull call metrics from AWS data warehouse instead of relying on Optimizely
##				Fixed bug that caused dict lookup to fail for conversions on Revenue goal (there are no conversions for this goal type)
## 				Replaced reading data from CSV with pulling from Optimizely API
##				Fixed bug in writing p-values to output file: wrote wrong field from experiment results array.
##
##	Version:		1.3.1


##	TO-DO:
##
##	Pull by LP and/or channel
##	FFL%: Divide the (sum of [accounts_paid] for orders with the sale creation type of core) by the (sum of [accounts_sheduled] for orders with the sale creation type of core)
##	CB%: Divide the (sum of revenue chargeback contractual for units with the sale creation type of core that have been scheduled) by the (sum of revenue gross product alternate for units with the sale creation type of core that have been scheduled)




from __future__ import division

import requests
import argparse
import psycopg2
import xlwt
from datetime import datetime
import abbastats as a
import webbrowser

parser = argparse.ArgumentParser(description='Grabs experiment data from Optimizely and spits out Excel file with results')
parser.add_argument('--version', action='version', version='%(prog)s v. 1.3')
parser.add_argument('-exp', '-x', nargs='*')
parser.add_argument('-group_by', '-g', nargs='*', choices = ['lp', 'ch', 'channel'])
parser.add_argument('--done', nargs=1)
args = parser.parse_args()

if args.exp == None:
	print "No experiment ID given. Defaulting to 1770580328.\n"
	exps = ['1770580328']
else:
	exps = args.exp

"""CONNECTION PARAMETERS"""
optimizely_token = ''
redshift_db = "datawarehouse"
redshift_usr = "tableaureporting"
redshift_pwd = ""
redshift_url = ""
redshift_port = "5439"


"""XLS Style Definitions"""
pct_style = xlwt.easyxf(num_format_str='0.00%')
num_style = xlwt.easyxf(num_format_str='#,##0')
bold_style = xlwt.easyxf('font: bold 1')
italic_style = xlwt.easyxf('font: italic 1')
italic_centered_style = xlwt.easyxf('font: italic 1; align: horizontal center')
italic_dollar_style = xlwt.easyxf('font: italic 1', num_format_str='$#,##0.00')
italic_pct_style = xlwt.easyxf('font: italic 1', num_format_str='0.00%')
italic_decimal_style = xlwt.easyxf('font: italic 1', num_format_str='0.00')
date_italic_style = xlwt.easyxf('font: italic 1; align: horizontal center', num_format_str='mm/dd/yy')
bottom_border_thin_italic_centered_style = xlwt.easyxf('font: italic 1; align: horizontal center; borders: bottom thin')
bottom_border_thin_bold_centered_style = xlwt.easyxf('font: bold 1; align: horizontal center; borders: bottom thin')
bottom_border_thick_bold_italic_style = xlwt.easyxf('font: bold 1, italic 1; borders: bottom medium')
bottom_border_thick_bold_italic_dollar_style = xlwt.easyxf('font: bold 1, italic 1; borders: bottom medium', num_format_str='$#,##0.00')
top_border_thin_italic_style = xlwt.easyxf('font: italic 1; borders: top thin')
top_border_thin_italic_pct_style = xlwt.easyxf('font: italic 1; borders: top thin', num_format_str='0.00%')
top_border_thin_dollar_italic_style = xlwt.easyxf('font: italic 1; borders: top thin', num_format_str='$#,##0.00')
top_border_thin_style = xlwt.easyxf('borders: top thin')
top_border_thin_dollar_style_red_italic_text = xlwt.easyxf('borders: top thin; font: color red, italic 1', num_format_str='$#,##0.00')
top_border_thick_dollar_bold_italic_style = xlwt.easyxf('font: bold 1, italic 1; borders: top medium', num_format_str='$#,##0.00')
top_border_thick_bold_italic_style = xlwt.easyxf('font: bold 1, italic 1; borders: top medium')
top_border_thick_bold = xlwt.easyxf('font: bold 1; borders: top medium')
left_border_thick = xlwt.easyxf('borders: left medium')
right_border_thick = xlwt.easyxf('borders: right medium')


for exp in exps:
	response = requests.get(
		    'https://www.optimizelyapis.com/experiment/v1/experiments/' + exp,
	    headers={'Token': optimizely_token}
	)

	if response.status_code == 200:
		experiment_name = response.json()['description']

		response = requests.get(
			    'https://www.optimizelyapis.com/experiment/v1/experiments/' + exp + '/results',
		    headers={'Token': optimizely_token}
		)

		if response.status_code == 200:
			api_results = response.json()

			variations = {}		#	varID : (excel_col_num, var_name, num_visitors)

			i = 4
			for variation_goal in api_results:
				if variation_goal['status'] == 'baseline':
					variations[variation_goal['variation_id']] = (3, variation_goal['variation_name'], variation_goal['visitors'])
				else:
					if variation_goal['variation_id'] not in variations:
						variations[variation_goal['variation_id']] = (i, variation_goal['variation_name'], variation_goal['visitors'])
						i = i + 1

			running_visitors = 0
			for v in variations:
				running_visitors = running_visitors + variations[v][2]

			if args.done != None:
				if running_visitors >= int(args.done[0]):
					experiment_status = "HAS"
				else:
					experiment_status = "HAS NOT"

				print "There are %d visitors in the experiment currently. \nExperiment %s met sample size threshold.\n" % (running_visitors, experiment_status)

			else:
				case_statement = """select case \n"""

				for var in variations:
					case_statement = case_statement + "\twhen pro.bucket ~ '" + str(var) + "' then '" + str(var) + "' \n"

				case_statement = case_statement + "\telse 'other'\nend as variation";
				channel_join = ""
				groupby_channel = ""
				groupby_lp = ""
				query = case_statement

				if args.group_by != None:
					for arg in args.group_by:
						if arg == "lp":
							groupby_lp = ", cpr.landing_url"

							query = query + "\n, cpr.landing_url"

						elif arg in ["ch", "channel"]:
							channel_join = "\nleft join data_warehouse.lookup_promo_codes p on p.promo_code = cpr.promo_code"
							groupby_channel = ", p.channel"

							query = query + "\n, p.channel"

				query = query + """
	, sum(case cd.call_type when 'I' then (case cd.call_purpose when 'Sales' then 1 when 'External Transfer' then 1 when  'Internal Transfer' then 1 else 0 end ) else 0 end) as gross_calls
	, COUNT(DISTINCT CASE cd.is_queue_opp WHEN TRUE THEN
			CASE WHEN cd.call_purpose IN ('Sales', 'External Transfer') THEN cd.id END END) AS queue_opp
	, COUNT(DISTINCT CASE cd.call_conclusion WHEN 'Answered' THEN
			CASE WHEN cd.call_type = 'I' THEN
				CASE WHEN cd.call_purpose IN ('Sales', 'External Transfer') THEN cd.id END END END) AS answered_call
	,  count(distinct case o.sale_creation_type when 'Core' then
			case o.is_order_overflow when false then
				case o.current_status when 'Scheduled' then o.id end end end ) as total_scheduled_orders
	,  count(distinct case o.sale_creation_type when 'Core' then
			case o.is_order_overflow when false then
				case o.current_status when 'Scheduled' then
					case o.sale_type when 'P' then o.id end end end end ) as offline_scheduled_orders
	, count(distinct case o.sale_creation_type when 'Core' then
			case o.is_order_overflow when false then
				case o.current_status when 'Scheduled' then
					case o.sale_type when 'I' then o.id end end end end) as online_scheduled_orders
	, count(distinct case ou.sale_creation_type when 'Core' then
			case ou.is_order_overflow when false then
				case ou.sale_type when 'P' then
					case ou.current_status when 'Scheduled' then ou.id end end end end ) as offline_scheduled_units
	, count(distinct case ou.sale_creation_type when 'Core' then
			case ou.is_order_overflow when false then
					case ou.sale_type when 'I' then
						case ou.current_status when 'Scheduled' then ou.id end end end end ) as online_scheduled_units
	, sum(case ou.sale_creation_type when 'Core' then
			case ou.is_order_overflow when false then
				case ou.sale_type when 'P' then
					case ou.current_status when 'Scheduled' then ou.revenue_contractual end end end end ) as offline_revenue
	, sum(case ou.sale_creation_type when 'Core' then
			case ou.is_order_overflow when false then
				case ou.sale_type when 'I' then
					case ou.current_status when 'Scheduled' then ou.revenue_contractual end end end end ) as online_revenue
	, sum(case cd.call_conclusion when 'Answered' then
			case when cd.call_type = 'I' then
				case when cd.call_purpose in ('Sales', 'External Transfer') then cd.cost_rep_staff end end end) as rep_cost
	from data_warehouse.conversion_phone_requests_optimizely pro
	left join data_warehouse.conversion_phone_requests cpr on cpr.request_id = pro.request_id
	left join data_warehouse.conversion_phone_requests_rel_orders pr_o on pr_o.request_id = pro.request_id
	left join data_warehouse.conversion_phone_requests_rel_call_detail prcd on prcd.request_id = pro.request_id
	left join data_warehouse.orders o on pr_o.orders_id = o.id
	left join data_warehouse.call_detail cd on cd.contact_id = prcd.contact_id
	left join data_warehouse.orders_units ou on ou.orders_id = o.id
	%s
	where pro.bucket ~ '%s'
	group by variation %s %s
	;
	""" % (channel_join, exp, groupby_channel, groupby_lp)

				conn = psycopg2.connect(database = redshift_db, user = redshift_usr, password = redshift_pwd, host = redshift_url, port = redshift_port)
				cur = conn.cursor()
				cur.execute(query)

				sql_results = cur.fetchall()

				cur.close()
				conn.close()

				workbook = xlwt.Workbook(encoding = 'ascii')
				results_sheet = workbook.add_sheet('Results')

				keepcharacters = (' ','.','_')
				filename = "".join(c for c in experiment_name if c.isalnum() or c in keepcharacters).rstrip()
				filename = 'AB Test - Financial Impact - ' + ' '.join(filename.split()) + '.xls'

				"""Write header info and row names"""
				results_sheet.write(0, 0, "Test:", bold_style)
				results_sheet.write(0, 1, experiment_name, bold_style)
				results_sheet.write(2, 1, "Dates")
				results_sheet.write(2, 2, datetime.strptime(str(api_results[0]['begin_time']), '%Y-%m-%dT%H:%M:%SZ').strftime('%m/%d/%Y'), date_italic_style)
				results_sheet.write(2, 3, "to", italic_centered_style)
				results_sheet.write(2, 4, datetime.strptime(str(api_results[0]['end_time']), '%Y-%m-%dT%H:%M:%SZ').strftime('%m/%d/%Y'), date_italic_style)

				results_sheet.write(8, 1, "Visits", top_border_thin_style)	#	Sessions
				results_sheet.write(8, 2, "", top_border_thin_style)	#	Sessions
				results_sheet.write(9, 1, "Annualized visits (est.)")	#	Annualized sessions (est.)
				results_sheet.write(10, 1, "GC")	#	GC
				results_sheet.write(11, 1, "QO")	#	QO
				results_sheet.write(12, 1, "AC")	#	AC
				results_sheet.write(13, 1, "SO")	#	SO_offline
				results_sheet.write(13, 2, "(offline)", italic_style)	#	SO_online
				results_sheet.write(14, 1, "SO")	#	SO_offline
				results_sheet.write(14, 2, "(online)", italic_style)	#	SO_online
				results_sheet.write(15, 1, "Ans%", italic_style)	#	Ans%

				results_sheet.write(16, 1, "QO/V")	#	QO/V
				results_sheet.write(17, 2, "Change", italic_style)	#	QO/V change
				results_sheet.write(18, 2, "High", italic_style)	#	QO/V, high
				results_sheet.write(19, 2, "Low", italic_style)	#	QO/V, low
				results_sheet.write(20, 2, "p-value", italic_style)	#	QO/V p-value

				results_sheet.write(21, 1, "SO/AC")	#	SO/AC
				results_sheet.write(22, 2, "Change", italic_style)	#	SO/AC change
				results_sheet.write(23, 2, "High", italic_style)	#	SO/AC, high
				results_sheet.write(24, 2, "Low", italic_style)	#	SO/AC, low
				results_sheet.write(25, 2, "p-value", italic_style)	#	SO/AC p-value

				results_sheet.write(26, 1, "SO(online)/V")	#	SO(online)/V
				results_sheet.write(27, 2, "Change", italic_style)	#	SO(online)/V change
				results_sheet.write(28, 2, "High", italic_style)	#	SO(online)/V, high
				results_sheet.write(29, 2, "Low", italic_style)	#	SO(online)/V, low
				results_sheet.write(30, 2, "p-value", italic_style)	#	SO(online)/V p-value

				results_sheet.write(31, 1, "FFL%", top_border_thin_italic_style)	#	FFL%
				results_sheet.write(31, 2, "", top_border_thin_italic_style)	#	FFL%
				results_sheet.write(32, 1, "CB%", italic_style)	#	CB%

				results_sheet.write(33, 1, "Rep cost", top_border_thin_italic_style)	#	Rep cost
				results_sheet.write(33, 2, "", top_border_thin_style)	#	Rep cost
				results_sheet.write(34, 1, "Revenue", italic_style)	#	Revenue
				results_sheet.write(34, 2, "(offline)", italic_style)	#	Revenue, offline
				results_sheet.write(35, 2, "(online)", italic_style)	#	Revenue, online

				results_sheet.write(36, 1, "Revenue net rep cost", top_border_thin_italic_style)	#	Revenue net rep cost
				results_sheet.write(36, 2, "", top_border_thin_italic_style)	#	Revenue net rep cost
				results_sheet.write(37, 1, "Net revenue adjusted for FFL/CB%", italic_style)	#	Net revenue adjusted for FFL/CB%
				results_sheet.write(38, 1, "Change", italic_style)	#	Change

				results_sheet.write(39, 0, "", right_border_thick)
				results_sheet.write(39, 1, "Net revenue (annualized)", top_border_thick_bold_italic_style)	#	Net revenue (annualized)
				results_sheet.write(39, 2, "", top_border_thick_bold_italic_style)
				results_sheet.write(40, 0, "", right_border_thick)
				results_sheet.write(40, 1, "Change (annualized)", bottom_border_thick_bold_italic_style)	#	Change (annualized)
				results_sheet.write(40, 2, "", bottom_border_thick_bold_italic_style)

				num_variations = len(sql_results)

				results_sheet.write(39, 3 + num_variations, "", left_border_thick)
				results_sheet.write(40, 3 + num_variations, "", left_border_thick)

				visits_baseline = 0
				qo_baseline = 0
				ac_baseline = 0
				so_offline_baseline = 0
				so_online_baseline = 0

				for r in sql_results:
					col = variations[r[0]][0]

					data = []
					for row in r:
						if row == None:
							data.append(0)
						else:
							data.append(row)

					if col == 3:
						visits_baseline = variations[r[0]][2]
						qo_baseline = data[2]
						ac_baseline = data[3]
						so_offline_baseline = data[5]
						so_online_baseline = data[6]

						results_sheet.write(6, col, "Control", bottom_border_thin_italic_centered_style)	#	Header
						results_sheet.write(7, col, "", bottom_border_thin_italic_centered_style)

						total_visitors_formula = "(SUM("
						annualized_visitors_formula = "(SUM($"

						for var in range(68, 68 + num_variations):
							if var < (67 + num_variations):
								total_visitors_formula = total_visitors_formula + chr(var) + "9,"
								annualized_visitors_formula = annualized_visitors_formula + chr(var) + "$9,$"
							else:
								total_visitors_formula = total_visitors_formula + chr(var) + "9)"
								annualized_visitors_formula = annualized_visitors_formula + chr(var) + "$9)"

						annualized_visitors_formula = annualized_visitors_formula + "/($E$3-$C$3))*365"
						results_sheet.write(9, col, xlwt.Formula(annualized_visitors_formula), num_style)	# Est. annualized visitors
						results_sheet.write(31, col, "", top_border_thin_italic_pct_style)	#	FFL%
						results_sheet.write(32, col, "", italic_pct_style)	#	CB%

					else:
						experiment_vq = a.Experiment(num_variations - 1, qo_baseline, visits_baseline)
						experiment_as = a.Experiment(num_variations - 1, so_offline_baseline, ac_baseline)
						experiment_vs = a.Experiment(num_variations - 1, so_online_baseline, visits_baseline)

						vq_results = experiment_vq.get_results(data[2], variations[r[0]][2])
						as_results = experiment_as.get_results(data[5], data[3])
						vs_results = experiment_vs.get_results(data[6], variations[r[0]][2])

						results_sheet.write(6, col, "Variation", bottom_border_thin_italic_centered_style)	#	Header
						results_sheet.write(7, col, variations[r[0]][1], bottom_border_thin_bold_centered_style)	#	Variation name
						results_sheet.write(9, col, xlwt.Formula("$D$10"), num_style)	# Est. annualized visitors

						results_sheet.write(17, col, xlwt.Formula("(" + chr(65 + col) + "17-$D$17)/$D$17"), italic_pct_style)	#	V/Q change
						results_sheet.write(18, col, vq_results[4][2], italic_pct_style)	# V/Q high estimate, improvement
						results_sheet.write(19, col, vq_results[4][1], italic_pct_style)	# V/Q low estimate, improvement
						results_sheet.write(20, col, vq_results[5]/2, italic_decimal_style)	# V/Q one-tailed p-value, where we're testing H1: p_baseline < p_variation instead

						results_sheet.write(22, col, xlwt.Formula("(" + chr(65 + col) + "22-$D$22)/$D$22"), italic_pct_style)	# A/S change
						results_sheet.write(23, col, as_results[4][2], italic_pct_style)	# A/S high estimate, improvement
						results_sheet.write(24, col, as_results[4][1], italic_pct_style)	# A/S low estimate, improvement
						results_sheet.write(25, col, as_results[5]/2, italic_decimal_style)	# A/S one-tailed p-value, where we're testing H1: p_baseline < p_variation instead

						results_sheet.write(27, col, xlwt.Formula("(" + chr(65 + col) + "27-$D$27)/$D$27"), italic_pct_style)	# SO (online)/Visits change
						results_sheet.write(28, col, vs_results[4][2], italic_pct_style)	# V/S high estimate, improvement
						results_sheet.write(29, col, vs_results[4][1], italic_pct_style)	# V/S low estimate, improvement
						results_sheet.write(30, col, vs_results[5]/2, italic_decimal_style)	# V/S one-tailed p-value, where we're testing H1: p_baseline < p_variation instead

						results_sheet.write(31, col, xlwt.Formula("D32"), top_border_thin_italic_pct_style)	#	FFL%
						results_sheet.write(32, col, xlwt.Formula("D33"), italic_pct_style)	#	CB%
						results_sheet.write(38, col, xlwt.Formula(chr(65 + col) + "38-$D$38"), italic_dollar_style)	#	Net revenue change
						results_sheet.write(40, col, xlwt.Formula(chr(65 + col) + "40-$D$40"), bottom_border_thick_bold_italic_dollar_style)	#	Annualized net revenue change

					results_sheet.write(8, col, variations[r[0]][2])	#	Sessions
					results_sheet.write(10, col, data[1])	#	GC
					results_sheet.write(11, col, data[2])	#	QO
					results_sheet.write(16, col, xlwt.Formula(chr(65 + col) + "12/" + chr(65 + col) + "9"), pct_style)
					results_sheet.write(12, col, data[3])	#	AC
					results_sheet.write(13, col, data[5])	#	SO_offline
					results_sheet.write(14, col, data[6])	#	SO_online
					results_sheet.write(15, col, xlwt.Formula(chr(65 + col) + "13/" + chr(65 + col) + "12"), italic_pct_style)	#	Ans%
					results_sheet.write(21, col, xlwt.Formula(chr(65 + col) + "14/" + chr(65 + col) + "13"), pct_style)	# SO/AC
					results_sheet.write(26, col, xlwt.Formula(chr(65 + col) + "15/" + chr(65 + col) + "9"), pct_style)	# SO (online)/Visits
					results_sheet.write(33, col, data[11], top_border_thin_dollar_style_red_italic_text)	#	Rep cost

					results_sheet.write(34, col, data[9], italic_dollar_style)	#	Revenue, offline
					results_sheet.write(35, col, data[10], italic_dollar_style)	#	Revenue, online

					results_sheet.write(36, col, xlwt.Formula("SUM(" + chr(65 + col) + "35:" + chr(65 + col) + "36)-" + chr(65 + col) + "34"), top_border_thin_dollar_italic_style)	#	Revenue net rep cost
					results_sheet.write(37, col, xlwt.Formula(chr(65 + col) + "37*" + chr(65 + col) + "32*(1-" + chr(65 + col) + "33)"), italic_dollar_style)	# Net revenue adjusted for FFL% and CB%

					results_sheet.write(39, col, xlwt.Formula("(" + chr(65 + col) + "38/" + chr(65 + col) + "9)*" + chr(65 + col) + "10"), top_border_thick_dollar_bold_italic_style)	#	Net revenue, annualized

					results_sheet.write(41, col, "", top_border_thick_bold)

				workbook.save(filename)
				webbrowser.open("https://reporting.clearlink.com/dashboard.php", new=2, autoraise=False)
		else:
			print response.status_code
