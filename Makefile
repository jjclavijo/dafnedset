PKGNAME=dafnedset

.PHONY: default
default: 
	${MAKE} python-wheel

.PHONY: wheel
wheel: build_env
	. ./build/flit/bin/activate &&\
	flit build
	
.PHONY: build_env
build_env: build/ build/flit/bin/activate

build/:
	mkdir $@

build/flit/bin/activate: build/
	cd build &&\
	python3 -m venv flit &&\
	. ./flit/bin/activate &&\
	https_proxy=${HTTPS_PROXY} pip install flit

whl-file:= $(wildcard dist/${PKGNAME}*.whl)
whl-target:= $(patsubst dist/%.whl,%.whl, $(wildcard dist/${PKGNAME}*.whl))
whl:= $(wildcard ./${PKGNAME}*.whl)

%.whl: $(whl-file)
	cp $(whl-file) .

copy-wheel: $(whl-target)

python-wheel: wheel 
	${MAKE} copy-wheel
	${MAKE} clean-build

clean-build:
	rm -rf build
	rm -rf dist

clean-wheel:
	for file in $(whl);\
	do rm -rf $${file};\
	done

clean: clean-build clean-wheel
