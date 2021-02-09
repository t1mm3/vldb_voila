
def parse_blend(blend):
	r = {}
	pairs = blend.split(",")
	for pair in pairs:
		kv = pair.split("=")
		r[kv[0]] = kv[1]
	return r
 
def latex_frac(x, y):
 	return "$\\frac{\\text{" + x + "}}{\\text{" + y + "}}$"