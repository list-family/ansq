bump_version:
	@echo 'Enter new version:'; \
	read new_version; \
	echo "__version__ = '$${new_version}'" > ansq/__version__.py

dist:
	rm -rf ansq.egg-info/ build/ dist/
	python setup.py sdist bdist_wheel

clean:
	rm -rf ansq.egg-info/ build/ dist/
