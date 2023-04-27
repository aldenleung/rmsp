# def factorial(n):
# 	if n == 1 or n == 0:
# 		return 1
# 	else:
# 		return n * factorial(n - 1)

def read_number_from_file(i):
	with open(i, 'rt') as f:
		return int(f.readlines()[0])
	
def add_two_numbers(a, b):
	import time
	time.sleep(5)
	return a + b

def write_number_to_file(o, number):
	with open(o, 'wt') as f:
		f.write(f"{number}")
