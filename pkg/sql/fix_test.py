import re

with open('distsql_check_test.go', 'r') as f:
    lines = f.readlines()

new_lines = []
i = 0

while i < len(lines):
    line = lines[i]
    
    # Check for renderNode struct literal start
    if 'return &renderNode{' in line:
        # Collect the full struct literal
        indent = len(line) - len(line.lstrip())
        struct_start = i
        brace_count = line.count('{') - line.count('}')
        full_struct = [line.rstrip()]
        i += 1
        
        while i < len(lines) and brace_count > 0:
            full_struct.append(lines[i].rstrip())
            brace_count += lines[i].count('{') - lines[i].count('}')
            i += 1
        
        # Join and parse
        struct_text = '\n'.join(full_struct)
        
        # Extract the source/input node
        input_match = re.search(r'input:\s*([^,}]+)', struct_text)
        # Extract render expressions - need to handle multi-line
        render_start = struct_text.find('render:')
        if render_start != -1:
            # Find the matching closing brace for the render array
            render_text = struct_text[render_start:]
            # Simple approach: find the line with render:
            render_lines = []
            in_render = False
            for orig_line in full_struct:
                if 'render:' in orig_line:
                    in_render = True
                if in_render:
                    render_lines.append(orig_line)
                    if orig_line.strip().endswith('},'):
                        break
            
            render_content = '\n'.join(render_lines).replace('render:', 'Render:')
            input_val = input_match.group(1).strip() if input_match else '&zeroNode{}'
            
            # Build the fixed version
            base_indent = ' ' * indent
            new_lines.append(f'{base_indent}rn := &renderNode{{\n')
            new_lines.append(f'{base_indent}\t{render_content}\n')
            new_lines.append(f'{base_indent}}}\n')
            new_lines.append(f'{base_indent}rn.Source = {input_val}\n')
            new_lines.append(f'{base_indent}return rn\n')
        else:
            # Fallback: keep original
            new_lines.extend(full_struct)
            new_lines.append('\n')
    else:
        new_lines.append(line)
        i += 1

with open('distsql_check_test.go', 'w') as f:
    f.writelines(new_lines)

print("Fixed test file")
