import json
import sys

def main(conf):
	with open(conf) as fileconf:
		data = json.load(fileconf)
	print(data)

if __name__ == "__main__": main(sys.argv[1])