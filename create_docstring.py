#!/usr/bin/env python3
"""
Tool to automatically generate high-quality docstrings for Python components.

This script uses an LLM to analyze a target component (function, method, or class)
and generate a comprehensive docstring that explains its purpose, implementation details,
and usage patterns.
"""

import argparse
import os
import re
import sys
from pathlib import Path
from typing import List, Set

from aider.models import Model
from aider.coders import Coder
from aider.io import InputOutput


def main() -> None:
    """
    Main function to handle the docstring generation workflow.
    
    Parses CLI arguments, sets up the LLM, generates a docstring for the target component,
    and handles user feedback for iterative improvement.
    """
    # Parse command line arguments
    parser = argparse.ArgumentParser(description="Generate docstrings for Python components")
    parser.add_argument("target_file", type=str, help="File containing the component to document")
    parser.add_argument("target_component", type=str, help="Name of function/method/class to document")
    parser.add_argument("--repo-dir", type=str, default=".", 
                        help="Repository directory to search for files using the target component")
    args = parser.parse_args()

    # Set up file paths
    editable_file = Path(args.target_file)
    
    # Find files that reference the target component
    repo_dir = Path(args.repo_dir)
    read_only_files = find_files_using_component(repo_dir, args.target_component, editable_file)
    
    if read_only_files:
        print(f"Found {len(read_only_files)} files referencing '{args.target_component}':")
        for file in read_only_files:
            print(f"  - {file}")

    # Ensure target file exists
    if not editable_file.exists():
        print(f"Error: Target file '{editable_file}' does not exist.")
        sys.exit(1)

    # Initialize the LLM
    model = Model("claude-3-7-sonnet-latest")
    io = InputOutput(yes=True)
    
    # Create the coder instance
    coder = Coder.create(
        main_model=model,
        editable_files=[editable_file],
        read_only_fnames=read_only_files,
        auto_commits=False,
        suggest_shell_commands=False,
        io=io
    )

    # Read the target file content
    with open(editable_file, "r") as f:
        file_content = f.read()

    # Create the initial prompt
    feedback = ""
    previous_docstring = ""
    while True:
        prompt = create_prompt(file_content, args.target_component, previous_docstring, feedback)
        
        # Run the LLM to generate the docstring
        coder.run(prompt)
        
        # Print the diff
        print("\nProposed docstring changes:")
        coder.show_diffs()
        
        # Get user feedback
        user_input = input("\nAccept these changes? (yes/no): ").strip().lower()
        if user_input in ["yes", "y"]:
            coder.save_files()
            print("Changes saved successfully!")
            break
        else:
            # Extract the current docstring from the diff
            previous_docstring = extract_docstring_from_diff(coder)
                
            feedback = input("Please provide feedback for improvement: ")
            if not feedback:
                print("Exiting without saving changes.")
                break


def find_files_using_component(repo_dir: Path, component_name: str, target_file: Path) -> List[Path]:
    """
    Find all Python files in the repository that use the target component.
    
    Args:
        repo_dir: The repository directory to search
        component_name: The name of the component to look for
        target_file: The file containing the component definition (to exclude from results)
        
    Returns:
        A list of Path objects for files that reference the component
    """
    using_files: Set[Path] = set()
    target_file = target_file.resolve()
    
    # Create a regex pattern to find uses of the component
    # This handles cases like: function_name(), ClassName(), module.function_name(), etc.
    pattern = re.compile(rf'(?<![a-zA-Z0-9_])({re.escape(component_name)})(?=\s*\(|\s*\.|\s*:|\s*$)')
    
    # Walk through the repository
    for root, _, files in os.walk(repo_dir):
        for file in files:
            if not file.endswith('.py'):
                continue
                
            file_path = Path(os.path.join(root, file)).resolve()
            
            # Skip the target file itself
            if file_path == target_file:
                continue
                
            try:
                with open(file_path, 'r', encoding='utf-8') as f:
                    content = f.read()
                    
                # Check if the component is used in this file
                if pattern.search(content):
                    using_files.add(file_path)
            except Exception as e:
                print(f"Warning: Could not read {file_path}: {e}", file=sys.stderr)
    
    return list(using_files)


def extract_docstring_from_diff(coder: Coder) -> str:
    """
    Extract the docstring from the coder's diff output.
    
    Args:
        coder: The Coder instance with diff information
        
    Returns:
        The extracted docstring as a string
    """
    # Get the diff from the coder
    if not hasattr(coder, 'diffs') or not coder.diffs:
        return ""
    
    # Look for added lines in the diff that are likely part of a docstring
    docstring_lines = []
    in_docstring = False
    
    for file_path, diff in coder.diffs.items():
        for line in diff.split('\n'):
            # Skip diff metadata lines
            if line.startswith('+++') or line.startswith('---') or line.startswith('@@'):
                continue
                
            # Look for added lines (start with '+')
            if line.startswith('+'):
                content = line[1:]  # Remove the '+' prefix
                
                # Check for docstring delimiters
                if '"""' in content or "'''" in content:
                    # If this is the start of a docstring
                    if not in_docstring and (content.strip().startswith('"""') or content.strip().startswith("'''")):
                        in_docstring = True
                        # If it's a single line docstring, handle it
                        if content.count('"""') >= 2 or content.count("'''") >= 2:
                            docstring_lines.append(content)
                            in_docstring = False
                        else:
                            docstring_lines.append(content)
                    # If this is the end of a docstring
                    elif in_docstring and (content.strip().endswith('"""') or content.strip().endswith("'''")):
                        docstring_lines.append(content)
                        in_docstring = False
                    # If we're inside a docstring
                    elif in_docstring:
                        docstring_lines.append(content)
                # If we're inside a docstring, add the line
                elif in_docstring:
                    docstring_lines.append(content)
    
    # Join the docstring lines
    return '\n'.join(docstring_lines)


def create_prompt(file_content: str, target_component: str, previous_docstring: str = "", feedback: str = "") -> str:
    """
    Create a structured prompt for the LLM to generate a docstring.
    
    Args:
        file_content: The content of the target file
        target_component: The name of the component to document
        previous_docstring: The previously generated docstring (if any)
        feedback: Optional user feedback for iterative improvement
    
    Returns:
        A structured prompt string
    """
    prompt = f"""
# TASK: WRITE A HIGH-QUALITY DOCSTRING

## CONTEXT
I need you to write a comprehensive docstring for the '{target_component}' component in the provided code.
The docstring should help other developers understand:
1. What the component does and why it exists
2. How it works at a high level
3. Important implementation details and design decisions
4. Usage patterns and examples where appropriate

## CODE
```python
{file_content}
```

## INSTRUCTIONS
1. ONLY modify the docstring for '{target_component}' - do not change any other code
2. Write the docstring at the highest scope level of the component
3. Follow PEP 257 conventions
4. Be comprehensive but concise
5. Include parameter descriptions, return values, and exceptions where applicable
6. Explain WHY certain implementation choices were made, not just WHAT the code does

## OUTPUT FORMAT
Return ONLY the docstring, nothing else. Do not include the function/class definition or any code.
"""

    if previous_docstring and feedback:
        prompt += f"""
## PREVIOUS DOCSTRING
```
{previous_docstring}
```

## FEEDBACK ON PREVIOUS DOCSTRING
The previous docstring was not satisfactory. Here's the feedback:
{feedback}

Please address this feedback in your new docstring.
"""

    return prompt


if __name__ == "__main__":
    main()
