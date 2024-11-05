#!/bin/bash
set -e

RED='\033[0;31m'
YELLOW='\033[0;33m'
GREEN='\033[0;32m'
RESET='\033[0m'

# Parse command line arguments
DIRTY=false
DRY_RUN=false
while [[ "$#" -gt 0 ]]; do
    case $1 in
        --dirty) DIRTY=true ;;
        --dry-run) DRY_RUN=true ;;
        *) echo "Unknown parameter: $1"; exit 1 ;;
    esac
    shift
done

# Function to get the latest tag from GitHub
get_latest_tag() {
    git fetch --tags
    git describe --tags --abbrev=0
}

# Function to build the CLI for multiple platforms
build_cli() {
    local version=$1
    local release_assets_dir="release_assets"
    local platforms=(
        "darwin/amd64"
        "darwin/arm64"
        "linux/amd64"
        "linux/arm64"
    )

    mkdir -p "$release_assets_dir"

    for platform in "${platforms[@]}"; do
        IFS="/" read -r GOOS GOARCH <<< "$platform"
        output_name="sequin-${version}-${GOOS}-${GOARCH}"
        if [ "$GOOS" = "windows" ]; then
            output_name+=".exe"
        fi

        echo "Building $output_name"
        env GOOS="$GOOS" GOARCH="$GOARCH" go build -o "$output_name" .
        if [ $? -ne 0 ]; then
            echo 'An error has occurred! Aborting the script execution...'
            exit 1
        fi

        zip_name="$release_assets_dir/sequin-${version}-${GOOS}-${GOARCH}.zip"
        zip -j "$zip_name" "$output_name"
        rm "$output_name"
    done
}

# Function to create a GitHub release with assets
create_github_release() {
    local tag=$1
    local assets_dir="release_assets"
    
    gh release create "$tag" \
        --title "Release $tag" \
        --notes "Release notes for $tag" \
        --generate-notes \
        "$assets_dir"/*.zip
}

if [[ "$DIRTY" == false ]] && [[ -n $(git status --porcelain) ]]; then
    echo -e "${RED}Can't release a dirty repository. Use '--dirty' to override.${RESET}" >&2
    git status
    exit 1
fi

# If dirty flag is set, show git status
if [[ "$DIRTY" == true ]]; then
    echo -e "${YELLOW}Warning: Running release on a dirty repository.${RESET}"
    echo -e "${YELLOW}Current git status:${RESET}"
    git status --short
    echo ""
fi

# Get the latest tag
latest_tag=$(get_latest_tag)
echo "Current version: $latest_tag"

# Prompt for the new version
read -p "Enter the new version: " new_version

# Build the CLI
build_cli "$new_version"

if [[ "$DRY_RUN" == true ]]; then
    echo -e "${YELLOW}Dry run mode: The following actions were performed locally:${RESET}"
    echo "1. Built CLI binaries for version $new_version"
    
    echo -e "\n${YELLOW}The following actions would be performed in a real run:${RESET}"
    echo "1. Create and push git tag $new_version"
    echo "2. Create GitHub release for $new_version and upload assets"

    # Clean up assets
    rm -rf release_assets
    exit 0
fi

# Create and push the new tag
git tag "$new_version"
git push origin "$new_version"

echo "New tag $new_version created and pushed to GitHub"

# Create a GitHub release for the new tag and upload assets
create_github_release "$new_version"

# Clean up assets
rm -rf release_assets

echo -e "${GREEN}GitHub release created for $new_version with assets${RESET}"
echo -e "${GREEN}Release process completed successfully!${RESET}"