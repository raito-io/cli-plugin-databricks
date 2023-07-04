GO := go

generate:
	go generate ./...

build: generate
	 go build ./...

unit-test:
	go test -mod=readonly -coverpkg=./... -covermode=atomic -coverprofile=unit-test-coverage.txt ./...

lint:
	golangci-lint run ./...
	go fmt ./...

test:
	go test -mod=readonly -tags=integration -race -coverpkg=./... -covermode=atomic -coverprofile=coverage.txt ./...
	go tool cover -html=coverage.txt -o coverage.html
