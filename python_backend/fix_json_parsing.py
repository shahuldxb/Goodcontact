#!/usr/bin/env python3
"""
Fix JSON parsing in Deepgram analysis classes
This script adds JSON string parsing to all remaining analysis classes
"""
import os
import glob
import re

def add_json_parsing_to_class(file_path):
    """Add JSON parsing logic to a Deepgram analysis class file"""
    with open(file_path, 'r') as f:
        content = f.read()
    
    # Check if the file already has JSON parsing code
    if "isinstance(deepgram_response_json" in content or "json.loads(deepgram_response_json" in content:
        print(f"File {file_path} already has JSON parsing logic")
        return False
    
    # Find the main function definition in different naming variations
    main_function_pattern = r"(async\s+def\s+main\s*\(\s*self\s*,\s*)([a-zA-Z0-9_]+)(\s*,\s*fileid\s*(?:,|\)|\s*.*?\)))"
    
    if not re.search(main_function_pattern, content):
        print(f"Could not find main function pattern in {file_path}")
        return False
    
    # Replace the parameter name and add JSON parsing
    new_content = re.sub(
        main_function_pattern,
        r"\1\2_str\3", 
        content, 
        count=1
    )
    
    # Get the parameter name that was used
    param_name_match = re.search(main_function_pattern, content)
    if not param_name_match:
        print(f"Could not extract parameter name from {file_path}")
        return False
    
    param_name = param_name_match.group(2)
    param_name_str = param_name + "_str"
    
    # Find the function body (indented block after the function def)
    function_body_start = new_content.find(":", new_content.find("async def main"))
    if function_body_start == -1:
        print(f"Could not find function body start in {file_path}")
        return False
    
    # Find the end of the docstring or the first line of code
    docstring_start = new_content.find('"""', function_body_start)
    if docstring_start != -1:
        docstring_end = new_content.find('"""', docstring_start + 3)
        insertion_point = new_content.find("\n", docstring_end) + 1
    else:
        # No docstring, find first indented line
        insertion_point = new_content.find("\n", function_body_start) + 1
        while new_content[insertion_point].isspace():
            insertion_point = new_content.find("\n", insertion_point) + 1
    
    # Count the indentation of the first line after the docstring
    first_line = new_content[insertion_point:new_content.find("\n", insertion_point)]
    indentation = ""
    for char in first_line:
        if char.isspace():
            indentation += char
        else:
            break
    
    # Prepare the JSON parsing code to insert
    json_parsing_code = f"""{indentation}# Parse JSON string into dictionary if needed
{indentation}if {param_name_str} and isinstance({param_name_str}, str):
{indentation}    try:
{indentation}        {param_name} = json.loads({param_name_str})
{indentation}    except json.JSONDecodeError as e:
{indentation}        print(f"Failed to parse {param_name_str} as JSON: {{str(e)}}")
{indentation}        return {{"error": f"Invalid JSON: {{str(e)}}", "fileid": fileid, "status": "Error"}}
{indentation}else:
{indentation}    {param_name} = {param_name_str}

"""
    
    # Insert the JSON parsing code
    new_content = new_content[:insertion_point] + json_parsing_code + new_content[insertion_point:]
    
    # Make sure json is imported
    if "import json" not in content:
        import_insertion_point = 0
        while new_content[import_insertion_point:import_insertion_point+1].isspace() or new_content[import_insertion_point] == '#':
            import_insertion_point = new_content.find("\n", import_insertion_point) + 1
        
        new_content = new_content[:import_insertion_point] + "import json\n" + new_content[import_insertion_point:]
    
    # Write the modified content back to the file
    with open(file_path, 'w') as f:
        f.write(new_content)
    
    print(f"Added JSON parsing logic to {file_path}")
    return True

def main():
    """Process all Deepgram analysis class files"""
    # Get the directory of this script
    script_dir = os.path.dirname(os.path.abspath(__file__))
    
    # Look for all dg_class_*.py files
    class_files = glob.glob(os.path.join(script_dir, "dg_class_*.py"))
    
    modified_count = 0
    for file_path in class_files:
        if add_json_parsing_to_class(file_path):
            modified_count += 1
    
    print(f"Modified {modified_count} out of {len(class_files)} files")

if __name__ == "__main__":
    main()