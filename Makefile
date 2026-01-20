# Copyright 2021 Akamai Techologies, Inc.
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# Ensure that 'all' is the default target otherwise it will be the first target from Makefile.common.
all::

# 1. PLATFORM DETECTION (Must be at the top)
# Detects your local machine (e.g., darwin/arm64 for your M4)
DETECTED_OS   := $(shell uname -s | tr '[:upper:]' '[:lower:]')
DETECTED_ARCH := $(shell uname -m | sed 's/x86_64/amd64/' | sed 's/aarch64/arm64/')

# 2. CONFIGURATION & DEFAULTS
DOCKER_REPO       ?= akamai
DOCKER_IMAGE_NAME ?= akamai-gtm-metrics-exporter
# These defaults ensure ARCH and OS are never empty
OS                ?= $(DETECTED_OS)
ARCH              ?= $(DETECTED_ARCH)

PROMTOOL_VERSION ?= 3.9.1
PROMTOOL_URL     ?= https://github.com/prometheus/prometheus/releases/download/v$(PROMTOOL_VERSION)/prometheus-$(PROMTOOL_VERSION).$(GO_BUILD_PLATFORM).tar.gz
PROMTOOL         ?= $(FIRST_GOPATH)/bin/promtool

# 3. INCLUDE SHARED LOGIC
include Makefile.common

# 4. OVERRIDE DOCKER TARGETS
# redfine common-docker to use buildx and prevent "DEPRECATED" warnings
.PHONY: common-docker
common-docker: $(BUILD_DOCKER_ARCHS)

$(BUILD_DOCKER_ARCHS): common-docker-%:
	@echo ">> building for linux/$* using modern buildx"
	docker buildx build \
		--platform "linux/$*" \
		-t "$(DOCKER_REPO)/$(DOCKER_IMAGE_NAME)-linux-$*:$(DOCKER_IMAGE_TAG)" \
		-f $(DOCKERFILE_PATH) \
		--build-arg ARCH="$*" \
		--build-arg OS="linux" \
		--load \
		$(DOCKERBUILD_CONTEXT)

# Local convenience target for your current machine
.PHONY: docker
docker:
	@echo ">> building docker image for linux/$(ARCH) using buildx"
	docker buildx build \
		--platform "linux/$(ARCH)" \
		-t "$(DOCKER_REPO)/$(DOCKER_IMAGE_NAME)-linux-$(ARCH):gtm-metrics-exporter" \
		-f ./Dockerfile \
		--build-arg ARCH=$(ARCH) \
		--build-arg OS="linux" \
		--load \
		./

# 5. TOOLS & LINTING
STATICCHECK_IGNORE =

# Use CGO for non-Linux builds.
ifeq ($(GOOS), linux)
    PROMU_CONF ?= .promu.yml
else
    ifndef GOOS
        ifeq ($(GOHOSTOS), linux)
            PROMU_CONF ?= .promu.yml
        else
            PROMU_CONF ?= .promu-cgo.yml
        endif
    else
        ifeq ($(GOOS), openbsd)
            ifeq ($(GOARCH), amd64)
                PROMU_CONF ?= .promu.yml
            else
                PROMU_CONF ?= .promu-cgo.yml
            endif
        else
            PROMU_CONF ?= .promu-cgo.yml
        endif
    endif
endif

PROMU := $(FIRST_GOPATH)/bin/promu --config $(PROMU_CONF)

$(eval $(call goarch_pair,amd64,386))
$(eval $(call goarch_pair,arm64,armv7))
$(eval $(call goarch_pair,mips64,mips))
$(eval $(call goarch_pair,mips64el,mipsel))

.PHONY: all
all:: precheck style check_license lint build

.PHONY: unused
unused: 
	@echo ">> skipping unused check. known Go compiler issue"

.PHONY: promtool
promtool: $(PROMTOOL)

$(PROMTOOL):
	@echo ">> downloading promtool v$(PROMTOOL_VERSION)"
	mkdir -p $(FIRST_GOPATH)/bin
	curl -fsS -L $(PROMTOOL_URL) | tar -xvzf - -C $(FIRST_GOPATH)/bin --no-anchored --strip 1 promtool