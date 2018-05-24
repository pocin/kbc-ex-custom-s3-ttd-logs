# test with -k flag
testk:
	docker-compose run --rm dev python3 -m pytest -k $(what)

test:
	docker-compose run --rm dev python3 -m pytest

clean:
	docker-compose down
