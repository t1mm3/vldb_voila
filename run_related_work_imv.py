#! /bin/env python3

import multiprocessing
import subprocess
import build_config

def get_filename():
	return build_config.get_binary_path() + '/related_work_imv.csv'

def main():
	max_num_cores = multiprocessing.cpu_count()

	scale_factors = [1, 10, 100]
	threads = [max_num_cores]
	# threads = list(set(powers2_until(max_num_cores) + [max_num_cores]))
	queries = ["imv1"]
	runs = 5

	imv = build_config.get_binary_path() + "/../../imv/build/engine"

	executables = [build_config.get_binary_path() + "/../../imv/build/run_tpch",
		imv]

	with open(get_filename(), 'w') as f:
		f.write("#NAME|TIME_MS|CYC_TUP|THREADS|SF|RUNS\n")
		for scale in scale_factors:
			for exe_path in executables:
				tpch_path = build_config.get_binary_path() + "/tpch_{scale}".format(scale=scale)

				for t in threads:
					cmd="{exe_path} {runs} {tpch_path} {threads}".format(
						exe_path=exe_path, runs=runs, tpch_path=tpch_path, threads=t)
					print(cmd)
					pipe = subprocess.Popen(cmd, encoding='utf8', shell=True, stdout=subprocess.PIPE)
					# out, err = pipe.communicate()

					for line in pipe.stdout:
						values = list(map(lambda x: x.strip(), line.split(",")))
						name = values[0]

						prefixes = ["q", "compilation", "imv", "rof", "rof-imv"]
						match = False

						for p in prefixes:
							if name.startswith(p):
								match = True
								break

						if not match:
							continue

						if exe_path == imv:
							name = "imv1 " + name

						f.write("{name}|{time}|{cycles}|{threads}|{scale}|{runs}\n".format(
							name=name, time=values[1], threads=t,
							scale=scale, runs=runs,
							cycles=values[8]))


if __name__ == '__main__':
	main()