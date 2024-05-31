.PHONY: clean db test

data:
	python3 gen_data.py

test:
	python3 -m unittest discover -s tests

db:
	flyway \
    	-url="jdbc:postgresql://127.0.0.1:5432/postgres" \
		-user="postgres" \
		-password="postgres" \
		-locations="filesystem:db/migrations/" \
		-validateMigrationNaming=true \
		migrate

clean-db:
	flyway \
    	-url="jdbc:postgresql://127.0.0.1:5432/postgres" \
		-user="postgres" \
		-password="postgres" \
		-cleanDisabled="false" \
		clean

clean:
	rm -rf output/*
	-rm examples/simulated.csv
