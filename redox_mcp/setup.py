#!/usr/bin/env python3
"""
Setup script for Python DABs environment.
Run this once before deploying: python setup.py
"""

import os
import platform
import subprocess
import sys
from pathlib import Path


class Colors:
    """ANSI color codes for terminal output."""
    HEADER = '\033[95m'
    OKBLUE = '\033[94m'
    OKCYAN = '\033[96m'
    OKGREEN = '\033[92m'
    WARNING = '\033[93m'
    FAIL = '\033[91m'
    ENDC = '\033[0m'
    BOLD = '\033[1m'


def print_header(message: str) -> None:
    """Print a formatted header message."""
    print(f"\n{Colors.BOLD}{'=' * 50}{Colors.ENDC}")
    print(f"{Colors.BOLD}{message}{Colors.ENDC}")
    print(f"{Colors.BOLD}{'=' * 50}{Colors.ENDC}\n")


def print_success(message: str) -> None:
    """Print a success message."""
    print(f"{Colors.OKGREEN}✓ {message}{Colors.ENDC}")


def print_error(message: str) -> None:
    """Print an error message."""
    print(f"{Colors.FAIL}✗ {message}{Colors.ENDC}", file=sys.stderr)


def print_info(message: str) -> None:
    """Print an info message."""
    print(f"{Colors.OKCYAN}→ {message}{Colors.ENDC}")


def check_python_version() -> None:
    """Check if Python 3.8+ is installed."""
    print_info("Checking Python version...")
    
    version = sys.version_info
    if version.major < 3 or (version.major == 3 and version.minor < 8):
        print_error(f"Python 3.8+ is required, but found {version.major}.{version.minor}")
        sys.exit(1)
    
    print_success(f"Python {version.major}.{version.minor}.{version.micro} detected")


def create_virtual_environment(venv_path: Path) -> None:
    """Create a Python virtual environment."""
    print_info(f"Creating virtual environment at {venv_path}/")
    
    try:
        subprocess.run(
            [sys.executable, "-m", "venv", str(venv_path)],
            check=True,
            capture_output=True,
            text=True
        )
        print_success("Virtual environment created")
    except subprocess.CalledProcessError as e:
        print_error(f"Failed to create virtual environment: {e.stderr}")
        sys.exit(1)


def get_pip_executable(venv_path: Path) -> str:
    """Get the path to pip in the virtual environment."""
    if platform.system() == "Windows":
        return str(venv_path / "Scripts" / "pip.exe")
    else:
        return str(venv_path / "bin" / "pip")


def upgrade_pip(pip_executable: str) -> None:
    """Upgrade pip to the latest version."""
    print_info("Upgrading pip...")
    
    try:
        subprocess.run(
            [pip_executable, "install", "--upgrade", "pip"],
            check=True,
            capture_output=True,
            text=True
        )
        print_success("pip upgraded")
    except subprocess.CalledProcessError as e:
        print_error(f"Failed to upgrade pip: {e.stderr}")
        sys.exit(1)


def install_requirements(pip_executable: str, requirements_file: Path) -> None:
    """Install dependencies from requirements.txt."""
    print_info(f"Installing dependencies from {requirements_file}...")
    
    if not requirements_file.exists():
        print_error(f"Requirements file not found: {requirements_file}")
        sys.exit(1)
    
    try:
        subprocess.run(
            [pip_executable, "install", "-r", str(requirements_file)],
            check=True,
            capture_output=True,
            text=True
        )
        print_success("Dependencies installed")
    except subprocess.CalledProcessError as e:
        print_error(f"Failed to install dependencies: {e.stderr}")
        sys.exit(1)


def get_activation_command(venv_path: Path) -> str:
    """Get the command to activate the virtual environment."""
    system = platform.system()
    
    if system == "Windows":
        return f"{venv_path}\\Scripts\\activate"
    else:
        return f"source {venv_path}/bin/activate"


def main() -> None:
    """Main setup function."""
    print_header("Setting up Python DABs environment")
    
    # Get project root directory
    project_root = Path(__file__).parent
    venv_path = project_root / ".venv"
    requirements_file = project_root / "requirements.txt"
    
    # Step 1: Check Python version
    check_python_version()
    
    # Step 2: Create virtual environment
    if venv_path.exists():
        print_info(f"Virtual environment already exists at {venv_path}/")
        response = input("Do you want to recreate it? [y/N]: ").strip().lower()
        if response == 'y':
            print_info("Removing existing virtual environment...")
            import shutil
            shutil.rmtree(venv_path)
            create_virtual_environment(venv_path)
        else:
            print_info("Using existing virtual environment")
    else:
        create_virtual_environment(venv_path)
    
    # Step 3: Get pip executable
    pip_executable = get_pip_executable(venv_path)
    
    # Step 4: Upgrade pip
    upgrade_pip(pip_executable)
    
    # Step 5: Install requirements
    install_requirements(pip_executable, requirements_file)
    
    # Step 6: Print completion message
    print_header("Setup complete!")
    
    print(f"\n{Colors.BOLD}Next steps:{Colors.ENDC}\n")
    print(f"1. Activate the virtual environment:")
    print(f"   {Colors.OKCYAN}{get_activation_command(venv_path)}{Colors.ENDC}\n")
    print(f"2. Deploy the bundle:")
    print(f"   {Colors.OKCYAN}databricks bundle deploy -t <target>{Colors.ENDC}\n")
    print(f"{Colors.BOLD}Examples:{Colors.ENDC}")
    print(f"   databricks bundle deploy -t dev")
    print(f"   databricks bundle deploy -t prod\n")


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print_error("\n\nSetup interrupted by user")
        sys.exit(1)
    except Exception as e:
        print_error(f"\n\nUnexpected error: {e}")
        sys.exit(1)
