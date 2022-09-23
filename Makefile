build:
	GOOS=linux GOARCH=amd64 go build -o bin/main-linux-amd64
	GOOS=darwin GOARCH=amd64 go build -o bin/main-darwin-amd64