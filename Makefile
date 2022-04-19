all:clean amd arm linux
	cp batch.sh dist/
	tar -zcvf dist.tar.gz dist
	@echo done
arm:
	@echo "Bulding for darwin arm "
	env GOOS=darwin GOARCH=arm64 go build -o dist/mqtt-benchmark-arm
amd:
	@echo "Bulding for darwin amd "
	env GOOS=darwin GOARCH=amd64 go build -o dist/mqtt-benchmark-amd
linux:
	env GOOS=linux GOARCH=amd64 go build -o dist/mqtt-benchmark-linux
clean:
	rm -rf dist/*
	rm -rf dist.tar.gz
