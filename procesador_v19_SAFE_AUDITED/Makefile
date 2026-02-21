.PHONY: test lint clean

test:
	python -m pytest -q

lint:
	python -m compileall -q procesador

clean:
	rm -rf .pytest_cache __pycache__ procesador/__pycache__ tests/__pycache__

.PHONY: selfcheck

selfcheck:
	python scripts/selfcheck.py

.PHONY: stress

stress:
	python scripts/stress_calc.py
