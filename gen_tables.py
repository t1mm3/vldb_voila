#!/bin/env python3
import subprocess
import build_config
import run_explore_base_flavor
import multiprocessing
import argparse

def base_flavor(query, threads, scale):
	filename = "{bin}/table_base_flavor__q_{q}__t_{t}__s_{s}.tex".format(
		bin=build_config.get_binary_path(), t=threads, q=query, s=scale)
	print("Generating {}".format(filename))
	with open(filename, "w") as f:
		cmd="{bin}/print_base_flavors.py --limit 10 --color --latex --num_threads {t} -q {q} -s {scale}".format(
			bin=build_config.get_binary_path(),
			t=threads, q=query, scale=scale)
		print(cmd)
		p = subprocess.Popen(cmd, shell=True, stdout=f)
		p.wait()

def __plot_base_flavor(query, threads, only_cbar, scale, typ):
	filename = "{bin}/plot_base_flavor__q_{q}__t_{t}__sf_{scale}{postfix}.pgf".format(
		bin=build_config.get_binary_path(), t=threads, q=query, scale=scale, postfix="" if typ == "3d" else "_{}".format(typ))
	#if only_cbar:
	#	filename = "{bin}/plot_base_flavor__cmap.pgf".format(bin=build_config.get_binary_path())
	print("Generating {}".format(filename))
	cmd="{bin}/print_base_flavors.py --num_threads {t} -q {q} {args} --type={typ} --plot={file} -s {scale}".format(
		bin=build_config.get_binary_path(),
		t=threads, q=query, file=filename, scale=scale,
		args="" if only_cbar else "--no-colorbar",
		typ=typ)
	print(cmd)
	p = subprocess.Popen(cmd, shell=True)
	p.wait()

def plot_base_flavor(query, threads, only_cbar, scale):
	for t in ["3d", "hist", "hist2", "hist3"]:
		__plot_base_flavor(query, threads, only_cbar, scale, t)

def plot_pipeline_flavor(query, threads, scale):
	filename = "{bin}/plot_pipeline_flavor__q_{q}__t_{t}__sf_{scale}.pgf".format(
		bin=build_config.get_binary_path(), t=threads, q=query, scale=scale)
	#if only_cbar:
	#	filename = "{bin}/plot_base_flavor__cmap.pgf".format(bin=build_config.get_binary_path())
	print("Generating {}".format(filename))
	cmd="{bin}/print_explore_pipeline_flavors.py --num_threads {t} -q {q} --plot={file} -s {scale}".format(
		bin=build_config.get_binary_path(),
		t=threads, q=query, file=filename, scale=scale)
	print(cmd)
	p = subprocess.Popen(cmd, shell=True)
	p.wait()

def plot_blend_flavor(query, threads, scale, restrict):
	filename = "{bin}/plot_blend_flavor__q_{q}__t_{t}__sf_{scale}{postfix}.pgf".format(
		bin=build_config.get_binary_path(), t=threads, q=query, scale=scale,
		postfix="_restrict" if restrict else "")
	#if only_cbar:
	#	filename = "{bin}/plot_base_flavor__cmap.pgf".format(bin=build_config.get_binary_path())
	print("Generating {}".format(filename))
	cmd="{bin}/print_explore_blends.py --num_threads {t} -q {q} {args} --plot={file} -s {scale}".format(
		bin=build_config.get_binary_path(),
		t=threads, q=query, file=filename, scale=scale,
		args="--restrict" if restrict else "")
	print(cmd)
	p = subprocess.Popen(cmd, shell=True)
	p.wait()



def base_perf(query, threads):
	filename = "{bin}/table_baseline__t_{t}.tex".format(
		bin=build_config.get_binary_path(), t=threads, q=query)
	print("Generating {}".format(filename))
	with open(filename, "w") as f:
		cmd="{bin}/print_perf_baseline.py --num_threads {t} -q {q}".format(
			bin=build_config.get_binary_path(),
			t=threads, q=query)
		print(cmd)
		p = subprocess.Popen(cmd, shell=True, stdout=f)
		p.wait()


def code_metrics():
	filename = "{bin}/table_code_metrics.tex".format(
		bin=build_config.get_binary_path())
	print("Generating {}".format(filename))
	with open(filename, "w") as f:
		cmd="{bin}/print_code_metrics.py".format(
			bin=build_config.get_binary_path())
		print(cmd)
		p = subprocess.Popen(cmd, shell=True, stdout=f)
		p.wait()


def related_work(query, threads):
	filename = "{bin}/table_relatedwork__t_{t}.tex".format(
		bin=build_config.get_binary_path(), t=threads, q=query)
	print("Generating {}".format(filename))
	with open(filename, "w") as f:
		cmd="{bin}/print_related_work.py --num_threads {t}".format(
			bin=build_config.get_binary_path(),
			t=threads, q=query)
		print(cmd)
		p = subprocess.Popen(cmd, shell=True, stdout=f)
		p.wait()

def app(p, x, xs):
	# return p.apply_async(x, xs)
	return x(*xs)

def main():
	parser = argparse.ArgumentParser(description='Generate tables and figures')
	parser.add_argument('-n', '--num_threads', dest='num_threads',
		default=multiprocessing.cpu_count())

	args = parser.parse_args()
	p = ""

	app(p, code_metrics, ())
	app(p, related_work, ("", 24))

	scale = 100
	app(p, plot_pipeline_flavor, ("q9", 24, scale))

	for restrict in [True, False]:
		app(p, plot_blend_flavor, ("q9", 24, scale, restrict))

	# with multiprocessing.Pool(args.num_threads) as p:

	#p = multiprocessing.Pool(int(args.num_threads))
	app(p, base_perf, ("q1,q3,q6,q9", 1))


	for scale in [100, 10]:
		app(p, base_flavor, ("q1,q3,q6,q9", 1, scale))
		app(p, base_flavor, ("q1,q3,q6,q9", 24, scale))

		#app(p, base_flavor, ("q1,q9", 1, scale))
		#app(p, base_flavor, ("q1,q9", 24, scale))

	scale = 100
	app(p, plot_base_flavor, ("q1", 1, True, scale))
	app(p, plot_base_flavor, ("q1", 12, False, scale))
	app(p, plot_base_flavor, ("q1", 24, False, scale))

	app(p, plot_base_flavor, ("q3", 1, False, scale))
	app(p, plot_base_flavor, ("q3", 12, False, scale))
	app(p, plot_base_flavor, ("q3", 24, False, scale))

	app(p, plot_base_flavor, ("q9", 1, False, scale))
	app(p, plot_base_flavor, ("q9", 12, False, scale))
	app(p, plot_base_flavor, ("q9", 24, False, scale))

		# plot_base_flavor("q9", 24, True, scale)

	#p.join()
	#p.close()
	print("Done")

if __name__ == '__main__':
	main()
