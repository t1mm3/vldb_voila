#!/bin/env python

import sqlite3
from sqlite3 import Error
import pandas as pd
import numpy as np

import voila_tools
import run_explore_pipeline_flavor
import multiprocessing

from voila_tools import latex_frac

def create_connection(db_file):
	conn = None
	try:
		conn = sqlite3.connect(db_file)
	except Error as e:
		print(e)

	return conn

def get_rows(args, dataframe):
	conn = create_connection(args.db)
	with conn:
		cur = conn.cursor()
		sql = """
			SELECT MIN(time_ms) AS min_time, AVG(time_ms) AS avg_time, MAX(time_ms) AS max_time,
				pipeline_flavor AS flavor, AVG(cycles), AVG(tuples), AVG(instructions), AVG(L1misses), AVG(LLCmisses), AVG(branchmisses)
			FROM runs
			WHERE query='{query}' AND mode='{mode}' AND threads={threads}
				AND aggregates_blend='' AND key_check_blend=''
				AND result!= 'TIMEOUT' AND result != 'CRASH'
				AND scale_factor = {scale}
			GROUP BY pipeline_flavor
			ORDER BY avg_time ASC
			""".format(query=args.query, mode=args.mode, threads=args.threads,
				scale=args.scale_factor)

		if dataframe:
			return pd.read_sql_query(sql,conn)
		else:
			cur.execute(sql)
			return cur.fetchall()

def plot(args):
	import matplotlib as mpl
	mpl.use('pgf')
	pgf_with_pgflatex = {
	    "pgf.texsystem": "pdflatex",
	    "pgf.rcfonts": False,
	    "pgf.preamble": [
	         r"\usepackage[utf8x]{inputenc}",
	         r"\usepackage[T1]{fontenc}",
	         # r"\usepackage{cmbright}",
	         ]
	}
	mpl.rcParams.update(pgf_with_pgflatex)
	mpl.rcParams['axes.axisbelow'] = True
	mpl.rcParams.update({'font.size': 14})
	from mpl_toolkits.mplot3d import axes3d
	import matplotlib.pyplot as plt
	import matplotlib.ticker as mticker


	fig = plt.figure()
	ax = fig.add_subplot(111)

	import pandas as pd
	import sqlite3

	df = get_rows(args, True)

	num_bins = 100
	n, bins, patches = ax.hist(df['avg_time'], num_bins, alpha=0.5, label='Average')
	n, bins, patches = ax.hist(df['min_time'], num_bins, alpha=0.5, label='Minimum')

	legend=plt.legend(loc='upper right', ncol=2)

	ax.set_xlabel('Runtime (ms)')
	ax.set_ylabel('Count')
	
	fig.tight_layout()
	fig.savefig(args.plot)
	plt.close(fig)

 
import argparse

def main():
	parser = argparse.ArgumentParser(description='Prints base flavors')
	parser.add_argument('--file', dest='db', default="voila.db",
		help='Database file')
	parser.add_argument('--mode', dest='mode',
		default=run_explore_pipeline_flavor.get_tag(), help='Tag to use')
	parser.add_argument('-q','--query', dest='query', default="q9",
		help='Queries seperated by comma')
	parser.add_argument('--num_threads', dest='threads', default=multiprocessing.cpu_count(),
		help='#Threads')
	parser.add_argument('--plot', help='Plot into file', dest='plot', default="")
	parser.add_argument('-s','--scale_factor', dest='scale_factor', default=100,
		help='Scale factor')


	args = parser.parse_args()
	# print args.accumulate(args.integers)

	queries = args.query.split(",")

	if args.plot != "":
		for query in queries:
			args.query = query
			plot(args)

 
if __name__ == '__main__':
	main()
