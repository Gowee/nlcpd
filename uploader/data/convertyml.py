newm = {}
for c, cc in mappings['categories'].items():
    for b in j:
        if b['name'].startswith(c):
            newm[int(b['id'])] = (cc,)
            break
    else: print("F", c)
for f, ff in mappings['filenames'].items():
    for b in j:
        if b['name'] == f:
            if int(b['id']) not in newm:
                newm[int(b['id'])] = (None,)
            newm[int(b['id'])] += (ff,)
            break
    else: print("FF", f)
with open("./test.yml", "w") as f:
    f.write(yaml.dump(newm, allow_unicode=True, sort_keys=False))