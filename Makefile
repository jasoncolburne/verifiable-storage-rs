PACKAGES := verifiable-storage verifiable-storage-derive verifiable-storage-postgres verifiable-storage-postgres-derive verifiable-storage-surreal verifiable-storage-surreal-derive
LIBS_DIR := lib
LIBS_SUBDIRS := verifiable-storage verifiable-storage-derive verifiable-storage-postgres verifiable-storage-postgres-derive verifiable-storage-surreal verifiable-storage-surreal-derive

.PHONY: all build clean clippy deny fmt fmt-check install-deny test

all: fmt-check deny clippy test build

build:
	@for pkg in $(PACKAGES); do \
		echo "Building $$pkg..."; \
		cargo build -p $$pkg --release || exit 1; \
	done

clean:
	@echo "Cleaning workspace..."
	cargo clean
	find . -type d -name "target" -exec rm -rf {} +

clippy:
	cargo clippy --workspace --all-targets -- -D warnings

deny:
	@if ! command -v cargo-deny &> /dev/null; then \
		echo "cargo-deny not installed. Install with: cargo install cargo-deny"; \
		exit 1; \
	fi
	@for lib in $(LIBS_SUBDIRS); do \
		echo "Checking lib/$$lib..."; \
		(cd $(LIBS_DIR)/$$lib && cargo deny check -A no-license-field) || exit 1; \
	done

fmt:
	cargo fmt --all

fmt-check:
	cargo fmt --all --check

install-deny:
	cargo install cargo-deny

test:
	cargo test --workspace
