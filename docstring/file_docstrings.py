#!/usr/bin/env python3
import re
import sys
import subprocess
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
    Main function to process a single Python file and create docstrings
    for all Python components found.
    """
    # Set up argument parser
    parser = argparse.ArgumentParser(
        description="Generate docstrings for Python components in a single file"
    )
    parser.add_argument(
        "file_path", 
        help="Python file to process"
    )
    args = parser.parse_args()
    
    file_path = Path(args.file_path).resolve()
    docstring_utility = Path.home() / "Desktop" / "utilities" / "create_docstring.py"
    
    # Verify the file exists
    if not file_path.exists() or not file_path.is_file():
        print(f"Error: File not found: {file_path}")
        return
        
    # Verify the file is a Python file
    if not file_path.name.endswith('.py'):
        print(f"Error: Not a Python file: {file_path}")
        return
        
    # Verify the docstring utility exists
    if not docstring_utility.exists():
        print(f"Error: Docstring utility not found at {docstring_utility}")
        return
    
    process_file(file_path, docstring_utility)


if __name__ == "__main__":
    main()
