#!/usr/bin/env python3
"""
Tool to automatically generate high-quality docstrings for Python components.

This script uses an LLM to analyze a target component (function, method, or class)
and generate a comprehensive docstring that explains its purpose, implementation details,
and usage patterns.
"""

import argparse
import sys
from pathlib import Path
from typing import List

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
    parser.add_argument("--references", type=str, nargs="*", default=[], 
                        help="Files that reference the target component (for context)")
    args = parser.parse_args()

    # Set up file paths
    editable_file = Path(args.target_file)
    read_only_files = [Path(f) for f in args.references]

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
            # Extract the current docstring to pass as previous for the next iteration
            # This is a simplification - in a real implementation, you might want to
            # extract the actual docstring from the diff
            previous_docstring = "The previously generated docstring"  # Placeholder
            try:
                # Try to get the actual docstring from the coder's edit history
                if hasattr(coder, 'last_edit') and coder.last_edit:
                    previous_docstring = coder.last_edit
            except:
                pass
                
            feedback = input("Please provide feedback for improvement: ")
            if not feedback:
                print("Exiting without saving changes.")
                break


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
