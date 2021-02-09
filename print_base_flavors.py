#!/bin/env python

import sqlite3
from sqlite3 import Error
import pandas as pd
import numpy as np

import voila_tools
import run_explore_base_flavor
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
		lim = ""
		if args.limit > 0:
			lim = "LIMIT {}".format(args.limit)

		sql = """
			SELECT MIN(time_ms) AS min_time, AVG(time_ms) AS avg_time, MAX(time_ms) AS max_time,
				default_blend AS flavor, AVG(cycles), AVG(tuples), AVG(instructions), AVG(L1misses), AVG(LLCmisses), AVG(branchmisses)
			FROM runs
			WHERE query='{query}' AND mode='{mode}' AND threads={threads}
				AND aggregates_blend='' AND key_check_blend=''
				AND result!= 'TIMEOUT' AND result != 'CRASH'
				AND scale_factor = {scale}
			GROUP BY default_blend
			ORDER BY avg_time ASC
			{lim}
			""".format(query=args.query, mode=args.mode, threads=args.threads, lim=lim,
				scale=args.scale_factor)

		if dataframe:
			return pd.read_sql_query(sql,conn)
		else:
			cur.execute(sql)
			return cur.fetchall()

def produce_rows(args):
	rows = get_rows(args, False)

	for row in rows:
		min_time = row[0]
		avg_time = row[1]
		max_time = row[2]
		flavor = voila_tools.parse_blend(row[3])
		cycles = row[4]
		tuples = row[5]
		instructions = row[6]
		l1miss = row[7]
		llcmiss = row[8]
		branchmiss = row[9]

		if args.color:
			comp = flavor["computation_type"]
			comp = comp.replace("(", "").replace(")", "")
			print("\\rowcolor{colorFlavor" + comp.upper() + "}")


		s = "{base} & {fsm} & {prefetch} & {avg} & {ipc} & {cyctup} & {l1miss} & {llcmiss} & {branchmiss}\\\\"

		print(s.format(
			avg="{:.1f}".format(avg_time),
			min="{:.1f}".format(min_time),
			cyctup="{:.1f}".format(cycles/tuples),
			ipc="{:.1f}".format(instructions/cycles),
			l1miss="{:.1f}".format(l1miss/tuples),
			llcmiss="{:.1f}".format(llcmiss/tuples),
			branchmiss="{:.1f}".format(branchmiss/tuples),
			base=flavor["computation_type"], prefetch=flavor["prefetch"],
			fsm=flavor["concurrent_fsms"]))

def export_legend(legend, filename="legend.png", expand=[-5,-5,5,5]):
	# from https://stackoverflow.com/a/47749903
	fig  = legend.figure
	fig.canvas.draw()
	bbox  = legend.get_window_extent()
	bbox = bbox.from_extents(*(bbox.extents + np.array(expand)))
	bbox = bbox.transformed(fig.dpi_scale_trans.inverted())
	fig.savefig(filename, dpi="figure", bbox_inches=bbox)

def plot3d(args):
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
	ax = fig.add_subplot(111, projection='3d')

	import pandas as pd
	import sqlite3

	df = get_rows(args, True)

	df['gComp'] = df['flavor'].apply(lambda x: voila_tools.parse_blend(x)["computation_type"])
	df['gFsm'] = df['flavor'].apply(lambda x: voila_tools.parse_blend(x)["concurrent_fsms"])
	df['gPref'] = df['flavor'].apply(lambda x: voila_tools.parse_blend(x)["prefetch"])

	# Plot a basic wireframe.
	#ax.scatter(comp, df['gFsm'], df['gPref'], s=20, c='b')

	for pf, color in [(0, 'red'), (1, 'blue'), (2, 'green'), (3, 'orange'), (4, 'yellow')]:
		x = df[df['gPref'] == str(pf)]

		compUniques, comp = np.unique(x['gComp'], return_inverse=True)

		txt = "Prefetch = {}".format(pf)

		# print(x)

		p = ax.scatter(comp,
				np.log2((x['gFsm']).apply(float)),
				x['avg_time'].apply(float),
				#c=x['gPref'].apply(float), cmap='RdYlBu',
				color=color,
				s=20, marker='o', label=txt,
				edgecolor='black', lw=0.1, depthshade=False
			)

	if not args.only_colorbar:
		ax.set(xticks=range(len(compUniques)), xticklabels=compUniques)

		def log_tick_formatter(val, pos=None):
			return int(2**val)
			# return "{:.2e}".format(2**val)

		ax.yaxis.set_major_formatter(mticker.FuncFormatter(log_tick_formatter))
		
		ax.set_xlabel('Computation')
		ax.xaxis.labelpad = 20
		ax.set_ylabel('FSMs')
		ax.set_zlabel('Time (ms)')

	# ax.set_zticks([0, 1, 2, 3, 4])
	# ax.set_yticks([1, 2, 4, 8, 16, 32])

	legend=plt.legend(loc='lower center', ncol=3)
	
	fig.tight_layout()
	if args.no_colorbar:
		legend.remove()
		pass

	if args.only_colorbar:
		export_legend(legend, args.plot)
	else:
		# ax.set_title(args.query.upper())
		fig.savefig(args.plot)
	plt.close(fig)

 

def hist(args, htype):
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
	mpl.rcParams.update({'font.size': 20})
	from mpl_toolkits.mplot3d import axes3d
	import matplotlib.pyplot as plt
	import matplotlib.ticker as mticker


	fig = plt.figure()
	ax = fig.add_subplot(111)

	import pandas as pd
	import sqlite3

	df = get_rows(args, True)
	min1 = df['avg_time'].min()	
	max1 = df['avg_time'].max()	

	num_bins = 50

	if htype == 1:
		df['gComp'] = df['flavor'].apply(lambda x: voila_tools.parse_blend(x)["computation_type"])

		groups, comp = np.unique(df['gComp'], return_inverse=True)

		dataframes = []
		labels = []
		for grp in groups:
			print("group = " + grp)
			x = df[df['gComp'] == grp]
			dataframes.append(x['avg_time'])
			labels.append(grp.replace("vector", "v"))
	elif htype == 2:
		df['gPref'] = df['flavor'].apply(lambda x: voila_tools.parse_blend(x)["prefetch"])

		groups, comp = np.unique(df['gPref'], return_inverse=True)

		dataframes = []
		labels = []
		for grp in groups:
			print("group = " + grp)
			x = df[df['gPref'] == grp]
			dataframes.append(x['avg_time'])
			labels.append(grp)
	elif htype == 3:
		df['_gFsm'] = df['flavor'].apply(lambda x: voila_tools.parse_blend(x)["concurrent_fsms"])
		df['gFsm'] = df['_gFsm'].apply(lambda x: int(x))

		groups, comp = np.unique(df['gFsm'], return_inverse=True)

		dataframes = []
		labels = []
		for grp in groups:
			print("group = {}".format(grp))
			x = df[df['gFsm'] == grp]
			dataframes.append(x['avg_time'])
			labels.append(grp)
	else:
		assert(False)

	n, bins, patches = ax.hist(dataframes, num_bins, stacked=True, label=labels)

	# plt.vlines(x=min1, ymin=0, ymax=10, color='black', label="Min")
	# n, bins, patches = ax.hist(df['avg_time'], num_bins, alpha=0.5, label='Average')

	# legend=plt.legend(loc='upper center', ncol=2)

	if args.query == "q1":
		legend=plt.legend(loc='upper right', ncol=1)
	else:
		legend=plt.legend(loc='upper right', ncol=1)
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
		default=run_explore_base_flavor.get_tag(), help='Tag to use')
	parser.add_argument('-q','--query', dest='query', default="q1,q3,q9",
		help='Queries seperated by comma')
	parser.add_argument('--num_threads', dest='threads', default=multiprocessing.cpu_count(),
		help='#Threads')
	parser.add_argument('--latex', dest='latex', action='store_true')
	parser.add_argument('--color', dest='color', action='store_true')
	parser.add_argument('--limit', dest='limit', default=0,
		help='Limit to top X. <=0 means no limit')

	parser.add_argument('--type', help='Plot type: 3d, hist1, or hist2', dest='type', default="3d")
	parser.add_argument('--plot', help='Plot into file', dest='plot', default="")
	parser.add_argument('--no-colorbar', help='Hide colormap', dest='no_colorbar', action='store_true')
	parser.add_argument('--only-colorbar', help='Only print colormap', dest='only_colorbar', action='store_true')
	parser.add_argument('-s','--scale_factor', dest='scale_factor', default=100,
		help='Scale factor')


	args = parser.parse_args()
	# print args.accumulate(args.integers)

	queries = args.query.split(",")

	if args.latex:
		print("\\begin{tabular}{l r r | r | r r r r r}")
		print("\\toprule")
		print("Computation & FSMs & Pref & $t_{avg}$ & IPC & Cycles & L1- & " +
			"LLC- & Br.-\\\\")
		print("& & & (ms) & & & miss & miss & miss\\\\")
		# print("& & & (ms) & & \\multicolumn{4}{c}{(Normalized by \\#tuples processed)}\\\\")
		print("\\midrule")

		first = True

		for query in queries:
			args.query = query

			if not first:
				print("\\midrule")

			if len(queries) > 1:
				print("\\emph{" + query.upper() + "}\\\\")

			produce_rows(args)
			first = False

		
		print("\\bottomrule")
		print("\\end{tabular}")


	if args.plot != "":
		for query in queries:
			args.query = query
			if args.type == "3d":
				plot3d(args)
			elif args.type == "hist":
				hist(args, 1)
			elif args.type == "hist2":
				hist(args, 2)
			elif args.type == "hist3":
				hist(args, 3)
			else:
				assert(False)

 
if __name__ == '__main__':
	main()