#!/bin/env python

import sqlite3
from sqlite3 import Error

import voila_tools
import multiprocessing
import run_perf_baseline

from voila_tools import latex_frac

def create_connection(db_file):
	conn = None
	try:
		conn = sqlite3.connect(db_file)
	except Error as e:
		print(e)

	return conn

def query_baselines(args, csv, baseflavor):
	name = "{q} {b}".format(q=args.query, b=baseflavor)
	for csv_line in csv:
		values = csv_line.split("|")
		if values[0] != name:
			continue

		time_ms = values[1]
		cyc_tup = values[2]
		threads = values[3]
		sf = values[4]

		# print(values)

		try:
			ts = int(threads)
			scale = int(sf)
		except ValueError:
			ts = float(threads)
			ts = int(ts)

			scale = float(sf)
			scale = int(scale)

		if int(ts) != int(args.threads):
			continue
		if int(scale) != int(args.scale_factor):
			continue

		return (baseflavor, cyc_tup, time_ms)

	return None

def query_baseflavors(args, baseflavor, fuji):
	conn = create_connection(args.db)
	with conn:
		cur = conn.cursor()

		if fuji:
			sql = """
				SELECT MIN(time_ms) AS min_time, AVG(time_ms) AS avg_time, MAX(time_ms) AS max_time,
					default_blend AS flavor, AVG(cycles), AVG(tuples), AVG(instructions), AVG(L1misses), AVG(LLCmisses), AVG(branchmisses)
				FROM runs
				WHERE query='{query}' AND mode='{mode}' AND threads={threads}
					AND aggregates_blend='' AND key_check_blend=''
					AND result!= 'TIMEOUT' AND result != 'CRASH' AND backend = 'fuji'
					AND scale_factor = {scale}
				GROUP BY default_blend
				""".format(query=args.query, mode=args.mode, threads=args.threads, scale=args.scale_factor)
		else:
			sql = """
				SELECT MIN(time_ms) AS min_time, AVG(time_ms) AS avg_time, MAX(time_ms) AS max_time,
					default_blend AS flavor, AVG(cycles), AVG(tuples), AVG(instructions), AVG(L1misses), AVG(LLCmisses), AVG(branchmisses)
				FROM runs
				WHERE query='{query}' AND mode='{mode}' AND threads={threads}
					AND aggregates_blend='' AND key_check_blend=''
					AND result!= 'TIMEOUT' AND result != 'CRASH' 
					AND default_blend='' AND backend = '{backend}'
					AND scale_factor = {scale}
				GROUP BY backend
				""".format(query=args.query, mode=args.mode, threads=args.threads,
					backend=baseflavor, scale=args.scale_factor)



		cur.execute(sql)
		rows = cur.fetchall()

		for row in rows:
			if fuji:
				flavor = voila_tools.parse_blend(row[3])

				if int(flavor["concurrent_fsms"]) != 1 or int(flavor["prefetch"])!=0:
					continue

				if baseflavor == "hyper":
					if flavor["computation_type"] != "scalar":
						continue

				if baseflavor == "vectorwise":
					if flavor["computation_type"] != "vector(1024)":
						continue
			min_time = row[0]
			avg_time = row[1]
			max_time = row[2]

			cycles = row[4]
			tuples = row[5]
			instructions = row[6]
			l1miss = row[7]
			llcmiss = row[8]
			branchmiss = row[9]
			return (baseflavor, cycles/tuples, min_time, avg_time)
	return None
 
import argparse

def fix_ms(x):
	return "{:.1f}".format(float(x))

def main():
	parser = argparse.ArgumentParser(description='Prints base flavors')
	parser.add_argument('--file', dest='db', default="voila.db",
		help='Database file')
	parser.add_argument('--baseline', dest='baseline', default=run_perf_baseline.get_filename(),
		help='Baseline CSV')
	parser.add_argument('--mode', dest='mode',
		default=run_perf_baseline.get_tag(), help='Tag to use')
	parser.add_argument('-q','--query', dest='query', default="q1,q3,q9",
		help='Queries seperated by comma')
	parser.add_argument('--num_threads', dest='threads', default=multiprocessing.cpu_count(),
		help='#Threads')

	args = parser.parse_args()

	with open(args.baseline, 'r') as b:
		csv = b.readlines()

		print("\\begin{tabular}{l | r | r | r | r }")
		print("\\toprule")
		print("Flavor & Q1 & Q3 & Q6 & Q9\\\\")
		# print("Query & Hyper & Scalar &  & Vector & Vector & Tectorwise~\\cite{kersten2018everything} \\\\")

		queries = args.query.split(",")

		flavors = [
				("typer", "Typer~\\cite{kersten2018everything}", True),
				("dhyper", "Direct Hyper", False),
				("fscalar", "FUJI Scalar", False),
				("tectorwise", "Tectorwise~\\cite{kersten2018everything}", True),
				("dvector", "Direct Vector", False),
				("fvector", "FUJI Vector", False)
			]

		for scale in [10, 100]:
			print("\\multicolumn{{5}}{{l}}{{\\textbf{{SF {scale}}}}}\\\\".format(scale=scale))
			args.scale_factor = scale
			for (f, latex_name, base) in flavors:
				if base:
					print("\\midrule")

				line = latex_name
				first = False

				if base:
					base_values = {}
				for query in queries:
					args.query = query

					if f == "typer":
						typer = query_baselines(args, csv, "hyper")
						val = typer[2]
					elif f == "tectorwise":
						tectorwise = query_baselines(args, csv, "vectorwise")
						val = tectorwise[2]
					elif f == "fscalar":
						vhyper = query_baseflavors(args, "hyper", True)
						val = vhyper[3]
					elif f == "fvector":
						vvector = query_baseflavors(args, "vectorwise", True)
						val = vvector[3]
					elif f == "dhyper":
						vhyper = query_baseflavors(args, "hyper", False)
						val = vhyper[3]
					elif f == "dvector":
						vvector = query_baseflavors(args, "vectorwise", False)
						val = vvector[3]
					else:
						assert(False)

					val = float(val) / 1000.0 # secs

					if base:
						base_values[query] = val

					this = "{ms} (${s}\\times$)".format(ms=fix_ms(val),
						s="{:.1f}".format(base_values[query]/val))
					if base:
						this = fix_ms(val)

					line = "{prev}{delim}{this}".format(prev=line,
						delim="" if first else "&", this=this)

					first = False
				print(line + "\\\\")


		print("\\bottomrule")
		print("\\end{tabular}")

 
if __name__ == '__main__':
	main()