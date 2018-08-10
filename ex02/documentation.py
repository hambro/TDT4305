import code 

def get_module_docstring(filepath):
    "Get module-level docstring of Python module at filepath, e.g. 'path/to/file.py'."
    co = compile(open(filepath, encoding='utf-8').read(), filepath, 'exec')
    if co.co_consts and isinstance(co.co_consts[0], str):
        docstring = co.co_consts[0]
    else:
        docstring = None
    return docstring


files = ['part2.py',]
with open('report.md', 'w+', encoding='utf-8') as doc:
	for fi in files:
		doc.writelines(get_module_docstring(fi))