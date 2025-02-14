GO := go

gotestsum := go run gotest.tools/gotestsum@latest

generate:
	go generate ./...

build: generate
	 go build ./...

unit-test:
	$(gotestsum) --debug --format testname -- -mod=readonly -coverpkg=./... -covermode=atomic -coverprofile=unit-test-coverage.txt ./...

lint:
	golangci-lint run ./...
	go fmt ./...

test:
	$(gotestsum) --debug --format testname -- -mod=readonly -tags=integration -race -coverpkg=./... -covermode=atomic -coverprofile=coverage.txt ./...
	go tool cover -html=coverage.txt -o coverage.html

test-sync:
	$(gotestsum) --debug --format testname -- -mod=readonly -tags=syncintegration -race -coverpkg=./... -covermode=atomic -coverprofile=coverage-sync.txt ./sync_test.go

gen-test-infra:
	if [ -z "${TARGET}" ]; then cd .infra/infra; terraform apply -auto-approve; else cd .infra/infra; terraform apply -auto-approve -target=${TARGET}; fi

gen-test-personas-infra:
	cd .infra/personas; terraform apply -auto-approve

destroy-test-infra:
	if [ -z "${TARGET}" ]; then cd .infra/infra;  terraform apply -destroy -auto-approve; else cd .infra/infra; terraform apply -destroy -auto-approve -target=${TARGET}; fi

destroy-test-personas-infra:
	cd .infra/personas; terraform apply -destroy -auto-approve

destroy-grants:
	cd .infra/infra; go run destroy.go --dbClientId "${dbClientId}" --dbClientSecret "${dbClientSecret}" --dbHost "${dbHost}" --catalogs="${dbCatalogs}" --drop

gen-test-usage:
	cd .infra/personas; terraform output -json | go run ../usage/usage.go --dbHost "${dbHost}" --dbWarehouseId "${dbWarehouseId}"