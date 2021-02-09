#!/bin/env python

import sqlite3
from sqlite3 import Error

import voila_tools
import multiprocessing
import run_explore_base_flavor

from voila_tools import latex_frac

def create_connection(db_file):
	conn = None
	try:
		conn = sqlite3.connect(db_file)
	except Error as e:
		print(e)

	return conn

from print_perf_baseline import query_baselines
import run_related_work_imv
import argparse


def get_rows(args, dataframe):
	conn = create_connection(args.db)
	with conn:
		cur = conn.cursor()
		sql = """
			SELECT default_blend, min_time, total_count
			FROM
				(SELECT default_blend, MIN(time_ms) AS min_time, COUNT(*) AS total_count
				FROM runs
				WHERE query='imv1' AND mode='{mode}' AND threads={threads}
					AND aggregates_blend='' AND key_check_blend=''
					AND result!= 'TIMEOUT' AND result != 'CRASH'
					AND scale_factor = {scale}
				GROUP BY default_blend)
			ORDER BY min_time
			""".format(query=args.query, mode=args.mode, threads=args.threads,
				scale=args.scale_factor)

		if dataframe:
			return pd.read_sql_query(sql,conn)
		else:
			cur.execute(sql)
			return cur.fetchall()


# explore_base_flavor.db
# query=imv1

def fix_ms(x):
	return "{:.1f}".format(float(x))

def main():
	parser = argparse.ArgumentParser(description='Prints base flavors')
	parser.add_argument('--file', dest='db', default="voila.db",
		help='Database file')
	parser.add_argument('--baseline', dest='baseline', default=run_related_work_imv.get_filename(),
		help='Baseline CSV')

	## Re-use results from base exploration!!!!
	parser.add_argument('--mode', dest='mode',
		default=run_explore_base_flavor.get_tag(), help='Tag to use')
	parser.add_argument('-q','--query', dest='query', default="imv1",
		help='Queries seperated by comma')
	parser.add_argument('--num_threads', dest='threads', default=multiprocessing.cpu_count(),
		help='#Threads')

	args = parser.parse_args()

	with open(args.baseline, 'r') as b:
		csv = b.readlines()

		print("\\begin{tabular}{l | r }")
		print("\\toprule")
		print("Flavor & IMV1 \\\\")
		print("\\midrule")

		# print("Query & Hyper & Scalar &  & Vector & Vector & Tectorwise~\\cite{kersten2018everything} \\\\")

		queries = args.query.split(",")

		flavors = [
				("compilation", "Data-Centric", True),
				("imv", "IMV", True),
				("rof", "ROF", True),
				("rof-imv", "ROF-IMV", True),
			]

		for scale in [100]: # [10, 100]:
			args.scale_factor = scale
			for (f, latex_name, base) in flavors:
				line = latex_name
				first = False

				for query in queries:
					args.query = query

					if base:
						typer = query_baselines(args, csv, f)
						# print(typer)
						val = typer[2]
					else:
						assert(False)

					val = float(val)

					this = "{ms}".format(ms=fix_ms(val))

					line = "{prev}{delim}{this}".format(prev=line,
						delim="" if first else "&", this=this)

					first = False
				print(line + "\\\\")

			print("\\midrule")
			# (best, total_count) = [0]

			min_times = {}

			for row in get_rows(args, False):
				# print(row)
				(name, time, total_count) = row

				comp_type = voila_tools.parse_blend(name)["computation_type"]
				if comp_type not in min_times:
					min_times[comp_type] = row
				else:
					(mname, mtime, mtotal_count) = min_times[comp_type] 
					if time < mtime:
						min_times[comp_type] = row

			for key in min_times:
				row = min_times[key]
				(name, time, total_count) = row

				fsm = voila_tools.parse_blend(name)["concurrent_fsms"]
				prefetch = voila_tools.parse_blend(name)["prefetch"] 

				print("Best FUJI ({}, {}, {}) & {}\\\\".format(key, fsm, prefetch, fix_ms(time)))

		print("\\bottomrule")
		print("\\end{tabular}")

 
if __name__ == '__main__':
	main()