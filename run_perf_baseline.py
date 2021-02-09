#! /bin/env python3

import multiprocessing
import subprocess
import build_config

def get_filename():
	return build_config.get_binary_path() + '/baselines.csv'

def get_tag():
	return "perf_baseline.db"

def main():
	max_num_cores = multiprocessing.cpu_count()

	scale_factors = [100, 10, 1]
	threads = [max_num_cores, int(max_num_cores/2), 1]
	# threads = list(set(powers2_until(max_num_cores) + [max_num_cores]))
	queries = ["q1", "q3", "q6", "q9", "imv1"]
	runs = 5

	with open(get_filename(), 'w') as f:
		f.write("#NAME|TIME_MS|CYC_TUP|THREADS|SF|RUNS\n")
		for scale in scale_factors:
			tpch_path = build_config.get_binary_path() + "/tpch_{scale}".format(scale=scale)
			exe_path = build_config.get_binary_path() + "/../../dbengineparadigms/build/run_tpch"

			for t in threads:
				cmd="{exe_path} {runs} {tpch_path} {threads}".format(
					exe_path=exe_path, runs=runs, tpch_path=tpch_path, threads=t)
				print(cmd)
				pipe = subprocess.Popen(cmd, encoding='utf8', shell=True, stdout=subprocess.PIPE)
				# out, err = pipe.communicate()

				for line in pipe.stdout:
					values = list(map(lambda x: x.strip(), line.split(",")))
					name = values[0]

					if not name.startswith("q"):
						continue

					f.write("{name}|{time}|{cycles}|{threads}|{scale}|{runs}\n".format(
						name=values[0], time=values[1], threads=t,
						scale=scale, runs=runs,
						cycles=values[8]))


	for scale in scale_factors:
		for t in threads:
			for q in queries:
				for flavor in ["hyper", "vector"]:
					cmd="""{voila} -s {scale} --mode {db} --flavor={flavor} --num_threads {t} -q {q} -r {runs}""".format(
						voila=build_config.get_main_executable(),
						db=get_tag(), t=t, q=q, scale=scale, runs=runs,
						flavor=flavor)
					print(cmd)
					subprocess.run(cmd, shell=True)

		for t in threads:
			for q in queries:
				for blend in ["scalar", "vector(1024)"]:
					cmd="""{voila} -s {scale} --mode {db} --{blend} --num_threads {t} -q {q} -r {runs}""".format(
						voila=build_config.get_main_executable(),
						db=get_tag(), t=t, q=q, scale=scale, runs=runs,
						blend="""default_blend="computation_type={comp},concurrent_fsms=1,prefetch=0" """.format(comp=blend))
					print(cmd)
					subprocess.run(cmd, shell=True)

if __name__ == '__main__':
	main()
