.PHONY: clean

clean:
	rm -rf output
	-rm examples/simulated.csv

data:
	python3 gen_data.py