VERSION    :=`cat version.txt`
IMAGE      := usgseros/lcmap-blackmagic
BRANCH     := $(or $(TRAVIS_BRANCH),`git rev-parse --abbrev-ref HEAD`)
BRANCH     := $(shell echo $(BRANCH) | tr / -)
BUILD_TAG  := $(IMAGE):build
#TAG        := $(shell if [ "$(BRANCH)" = "master" ];\
#                         then echo "$(IMAGE):$(VERSION)";\
#                         else echo "$(IMAGE):$(VERSION)-$(BRANCH)";\
#                      fi)
SHORT_HASH := `git rev-parse --short HEAD`
TAG        := $(IMAGE):$(BRANCH)-$(VERSION)-$(SHORT_HASH)

deps-up:
	docker-compose -f deps/docker-compose.yml up

deps-up-d:
	docker-compose -f deps/docker-compose.yml up -d

deps-down:
	docker-compose -f deps/docker-compose.yml down

clean:
	@find . -type d -name "lcmap_blackmagic.egg-info" -exec rm -rf {} +
	@find . -type f -name "*pyc" -exec rm -rf {} \;
	@find . -type f -name "*~" -exec rm -rf {} \;
	@find . -type d -name "__pycache__" -exec rm -rf {} +

set-nginx-cache-file-perms: deps-up-d
	docker exec -it blackmagic-nginx /bin/bash -c "chmod -R 777 /data/nginx/cache"

clear-nginx-cache: deps-up-d
	docker exec -it blackmagic-nginx /bin/bash -c "rm -rf /data/nginx/cache/*"

update-test-data: deps-up-d clear-nginx-cache tests set-nginx-cache-file-perms deps-down
	@echo "NGINX cache files updated"

test-with-manual-deps:
	pytest --ignore=deps/nginxcache -p no:warnings -vv

tests: deps-up-d test-with-manual-deps deps-down

docker-build:
	@docker build --build-arg version=$(VERSION) -t $(BUILD_TAG) --rm=true --compress $(PWD)

docker-tag:
	@docker tag $(BUILD_TAG) $(TAG)

docker-login:
	@$(if $(and $(DOCKER_USER), $(DOCKER_PASS)), docker login -u $(DOCKER_USER) -p $(DOCKER_PASS), docker login)

docker-push: docker-login
	docker push $(TAG)

all: tests clean docker-build docker-tag docker-push

debug:
	@echo "VERSION:   $(VERSION)"
	@echo "IMAGE:     $(IMAGE)"
	@echo "BRANCH:    $(BRANCH)"
	@echo "BUILD_TAG: $(BUILD_TAG)"
	@echo "TAG:       $(TAG)"

