# ChaosServe Phase 0 harness. `make test` is the single AC-suite entry
# point: unit + integration tests, then the kind cluster smoke suite.
GO      ?= go
KIND    ?= $(shell command -v kind 2>/dev/null || echo $(HOME)/go/bin/kind)
CLUSTER ?= chaosserve
IMAGE   ?= chaosserve/mockserver:dev

.PHONY: all build test test-unit docker-build kind-up kind-down kind-smoke clean

all: build

build:
	$(GO) build ./...

# Build the CLIs as static binaries to fresh paths (macOS dyld-wedge
# safe); see docs/DECISIONS.md module 8 addendum.
bins:
	CGO_ENABLED=0 $(GO) build -o bin/chaosserve ./cmd/chaosserve
	CGO_ENABLED=0 $(GO) build -o bin/chaoscampaign ./cmd/chaoscampaign
	CGO_ENABLED=0 $(GO) build -o bin/mockserver ./cmd/mockserver

test-unit:
	$(GO) test -race -short ./... -count=1

# The AC suite runs real load with timing-precise gates, so it runs
# without the race detector (which distorts scheduling). The full
# pipeline's concurrency still runs under -race in the unit suite via
# internal/run's in-process end-to-end test.
test-ac:
	$(GO) test ./internal/ac/ -count=1 -timeout 45m

# The full gate: unit tests, the SPEC.md §8 AC suite, the in-cluster
# smoke suite, then the AC7 one-command reproduce, all against kind.
test: test-unit test-ac kind-smoke reproduce campaign-e2e

# AC7: one-command reproduce from a clean checkout against kind.
reproduce:
	KIND=$(KIND) CLUSTER=$(CLUSTER) IMAGE=$(IMAGE) deploy/kind/reproduce.sh

# The §5 repetition pipeline end to end: an N=2 chaoscampaign against the
# live 2-replica deployment, per-run §10 gates, §7 aggregation.
campaign-e2e:
	KIND=$(KIND) CLUSTER=$(CLUSTER) IMAGE=$(IMAGE) deploy/kind/campaign-e2e.sh

docker-build:
	docker build -t $(IMAGE) .

kind-up:
	@$(KIND) get clusters 2>/dev/null | grep -qx '$(CLUSTER)' || \
		$(KIND) create cluster --name $(CLUSTER) --config deploy/kind/kind-config.yaml --wait 120s

kind-down:
	$(KIND) delete cluster --name $(CLUSTER)

kind-smoke: docker-build kind-up
	KIND=$(KIND) CLUSTER=$(CLUSTER) IMAGE=$(IMAGE) deploy/kind/smoke.sh

clean:
	rm -rf bin/
