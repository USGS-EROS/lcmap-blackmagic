VERSION :=`./bin/version`
IMAGE   := usgseros/lcmap-blackmagic
BRANCH     := $(or $(TRAVIS_BRANCH),`git rev-parse --abbrev-ref HEAD`)
BRANCH     := $(shell echo $(BRANCH) | tr / -)
BUILD_TAG  := $(IMAGE):build
TAG        := $(shell if [ "$(BRANCH)" = "master" ];\
                         then echo "$(IMAGE):$(VERSION)";\
                         else echo "$(IMAGE):$(VERSION)-$(BRANCH)";\
                      fi)

deps-up:
	docker-compose -f resources/docker-compose.yml up blackmagic-cassandra

deps-up-d:
	docker-compose -f resources/docker-compose.yml up -d blackmagic-cassandra

deps-down:
	docker-compose -f resources/docker-compose.yml down blackmagic-cassandra

test:
	pytest

docker-build:
	@docker build --build-arg version=$(VERSION) -t $(BUILD_TAG) --rm=true --compress $(PWD)

docker-tag:
	@docker tag $(BUILD_TAG) $(TAG)

docker-login:
	@$(if $(and $(DOCKER_USER), $(DOCKER_PASS)), docker login -u $(DOCKER_USER) -p $(DOCKER_PASS), docker login)

docker-push: docker-login
	docker push $(TAG)

all: deps-up-d test deps-down docker-build docker-tag docker-push

debug:
	@echo "VERSION:   $(VERSION)"
	@echo "IMAGE:     $(IMAGE)"
	@echo "BRANCH:    $(BRANCH)"
	@echo "BUILD_TAG: $(BUILD_TAG)"
	@echo "TAG:       $(TAG)"

