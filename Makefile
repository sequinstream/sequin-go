.PHONY: help signoff signoff-dirty release release-dirty

help: ## Show this help message
	@echo "Available commands:"
	@echo "  make signoff       - Run the signoff script"
	@echo "  make signoff-dirty - Run the signoff script with --dirty flag"
	@echo "  make release       - Create and publish a new release"
	@echo "  make release-dirty - Create and publish a new release with --dirty flag"
	@echo "  make help         - Show this help message"

signoff:
	@./scripts/signoff.sh

signoff-dirty:
	@./scripts/signoff.sh --dirty

release:
	@./scripts/release.sh

release-dirty:
	@./scripts/release.sh --dirty