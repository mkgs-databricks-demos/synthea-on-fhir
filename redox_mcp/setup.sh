#!/usr/bin/env bash
# Setup script for Python DABs environment
# Run this once before deploying: ./setup.sh

set -e

echo "=========================================="
echo "Setting up Python DABs environment"
echo "=========================================="

# Check if Python 3 is installed
if ! command -v python3 &> /dev/null; then
    echo "Error: python3 is not installed"
    exit 1
fi

# Create virtual environment
echo ""
echo "Creating virtual environment in .venv/"
python3 -m venv .venv

# Activate virtual environment
echo ""
echo "Activating virtual environment..."
source .venv/bin/activate

# Install dependencies
echo ""
echo "Installing dependencies from requirements.txt..."
pip install --upgrade pip
pip install -r requirements.txt

echo ""
echo "=========================================="
echo "Setup complete!"
echo "=========================================="
echo ""
echo "To activate the virtual environment manually, run:"
echo "  source .venv/bin/activate"
echo ""
echo "To deploy the bundle, run:"
echo "  databricks bundle deploy -t <target>"
echo ""
echo "Examples:"
echo "  databricks bundle deploy -t dev"
echo "  databricks bundle deploy -t prod"
