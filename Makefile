bump_version:
	@printf 'Enter new version: '; \
	read new_version; \
	sed -i"" -r "s/version = .*/version = $${new_version}/" setup.cfg

dist:
	rm -rf ansq.egg-info/ build/ dist/
	python setup.py sdist bdist_wheel

clean:
	rm -rf ansq.egg-info/ build/ dist/
