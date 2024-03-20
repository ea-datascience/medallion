docker_run = docker run --rm --mount type=bind,source="$(shell pwd)/",target=/root/ parloa-data-engineering-challenge:0.0.1

.DEFAULT_GOAL := help

.PHONY: help
help: ## Show available targets
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-30s\033[0m %s\n", $$1, $$2}'

.PHONY: build-docker-image
build-docker-image: ## Build the docker image and install python dependencies
	docker build --no-cache -t parloa-data-engineering-challenge:0.0.1 .
	$(docker_run) pipenv install --dev

.PHONY: tidy
tidy: ## Tidy code
	$(docker_run) pipenv run tidy

.PHONY: lint
lint: ## Lint the code
	$(docker_run) pipenv run lint

.PHONY: test
test: ## Run tests
	$(docker_run) pipenv run test

.PHONY: execute
execute: ## Execute the solution
	$(docker_run) pipenv run python src/main.py data/customers.json

# Added to facilitate the deployment of the solution in azure using terraform
.PHONY: azure-login
azure-login: ## Authenticate with Azure
	az login

.PHONY: init-terraform
init-terraform: ## Initialize Terraform
	cd iac && terraform init

.PHONY: plan-terraform
plan-terraform: ## Plan Terraform changes
	cd iac && terraform plan

.PHONY: apply-terraform
apply-terraform: ## Apply Terraform changes
	cd iac && terraform apply -auto-approve

.PHONY: deploy
deploy: azure-login init-terraform plan-terraform apply-terraform ## Deploy resources to Azure
