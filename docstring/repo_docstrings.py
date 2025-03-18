#!/usr/bin/env python3
import os
import re
import subprocess
import sys
import argparse
from pathlib import Path


def find_components(file_path):
    """
    Find all classes, methods, and functions in a Python file.
    
    Args:
        file_path: Path to the Python file to analyze
        
    Returns:
        List of component names found in the file
    """
    with open(file_path, 'r') as f:
        content = f.read()
    
    # Find class definitions
    class_pattern = r'^\s*class\s+(\w+)'
    classes = re.findall(class_pattern, content, re.MULTILINE)
    
    # Find function and method definitions
    func_pattern = r'^\s*def\s+(\w+)'
    functions = re.findall(func_pattern, content, re.MULTILINE)
    
    return classes + functions


def process_file(file_path, docstring_utility):
    """
    Process a single Python file to find components and create docstrings.
    
    Args:
        file_path: Path to the Python file to process
        docstring_utility: Path to the docstring creation utility
    """
    print(f"Processing {file_path}")
    components = find_components(file_path)
    
    for component in components:
        print(f"  Creating docstring for {component}")
        try:
            subprocess.run(
                [sys.executable, docstring_utility, str(file_path), component],
                check=True
            )
        except subprocess.CalledProcessError as e:
            print(f"  Error creating docstring for {component}: {e}")


def main():
    """
    Main function to iterate through the specified directory and create docstrings
    for all Python components found.
    """
    # Set up argument parser
    parser = argparse.ArgumentParser(
        description="Generate docstrings for Python components in a directory"
    )
    parser.add_argument(
        "src_dir", 
        help="Source directory to process"
    )
    args = parser.parse_args()
    
    src_dir = Path(args.src_dir).resolve()
    docstring_utility = Path.home() / "Desktop" / "utilities" / "create_docstring.py"
    
    # Verify the source directory exists
    if not src_dir.exists() or not src_dir.is_dir():
        print(f"Error: Source directory not found: {src_dir}")
        return
        
    # Verify the docstring utility exists
    if not docstring_utility.exists():
        print(f"Error: Docstring utility not found at {docstring_utility}")
        return
    
    print(f"Processing source directory: {src_dir}")
    
    # Walk through the directory structure
    for root, _, files in os.walk(src_dir):
        for file in files:
            if file.endswith('.py'):
                file_path = Path(root) / file
                process_file(file_path, docstring_utility)


if __name__ == "__main__":
    main()
