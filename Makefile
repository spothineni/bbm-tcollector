
all: lib/jmx-1.0.jar

lib/jmx-1.0.jar:
	cd src/stumbleupon/monitoring && $(MAKE)
	cp src/stumbleupon/monitoring/build/jmx-1.0.jar ./lib

clean: 
	cd src/stumbleupon/monitoring && $(MAKE) clean
	rm -f lib/jmx-1.0.jar
