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

gen-test-infra:
	cd .infra/infra; terraform apply -auto-approve ${${${DEMO_INFRA}==true}:+-target=module.demo} ${${${TESTING_INFRA}==true}:+-target=module.testing}

destroy-test-infra:
	cd .infra/infra; terraform apply -destroy -auto-approve ${${${DEMO_INFRA}==true}:+-target=module.demo} ${${${TESTING_INFRA}==true}:+-target=module.testing}

destroy-grants:
	cd .infra/infra; go run destroy.go --dbUsername "${dbUsername}" --dbPassword "${dbPassword}" --dbHost "${dbHost}" --catalogs="${dbCatalogs}" --drop

gen-test-usage:
	cd .infra/infra; terraform output -json | go run ../usage/usage.go --dbHost ${dbHost} --dbWarehouseId ${dbWarehouseId} --dbUsers ${dbUsers}