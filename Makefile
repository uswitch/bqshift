.PHONY: release,clean

MAC = GOOS=darwin GOARCH=amd64
LINUX = GOOS=linux GOARCH=amd64
SOURCES = $(wildcard *.go) $(wildcard bigquery/*.go) $(wildcard redshift/*.go) $(wildcard storage/*.go) $(wildcard util/*.go) $(wildcard vendor/*.go)

ifndef VERSION
$(error VERSION is not set)
endif

SHA = $(shell git rev-parse --short HEAD)
FLAGS = -ldflags "-X main.versionNumber=${VERSION} -X main.sha=${SHA}"

release: release/bqshift-${VERSION}.tar.gz

release/bqshift-${VERSION}.tar.gz: target/mac/bqshift target/linux/bqshift
	rm -rf release/
	mkdir -p release/
	tar -zcf release/bqshift-${VERSION}.tar.gz -C target/ .

target/mac/bqshift: $(SOURCES)
	${MAC} go build ${FLAGS} -o target/mac/bqshift .

target/linux/bqshift: $(SOURCES)
	${LINUX} go build ${FLAGS} -o target/linux/bqshift .

clean:
	rm -rf release/
	rm -rf target/