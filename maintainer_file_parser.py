from collections import defaultdict

def parse(text):
    start_pos = text.find("---\n") + 5
    modules = text[start_pos:].split("\n\n")
    module_info = {}
    for module_text in modules:
        module = module_text.split("\n")
        name = module[0]
        info = defaultdict(list)
        for line in module[1:]:
            try:
                parts = line.split('\t')
                if len(parts) < 2:
                    continue
                key, value = parts[0], parts[1]
                info[key[0]].append(value)
            except Exception, e:
                print line
                raise e
        module_info[name] = info
    return module_info