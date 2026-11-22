import random, shutil, sys
from pathlib import Path
src, dst = Path(sys.argv[1]), Path(sys.argv[2])
shutil.copytree(src, dst)
rnd = random.Random(5)
for f in sorted((dst / "events").glob("*.csv")) + sorted((dst / "clicks").glob("*.csv")):
    lines = f.read_bytes().split(b"\n")
    out = [lines[0]]
    for l in lines[1:]:
        x = rnd.random()
        if l and x < 0.02:
            p = l.split(b",")
            i = rnd.randrange(len(p))
            if f.parent.name == "events" and i == 3:
                i = 1
            if f.parent.name == "clicks":
                i = 0
            pad = b" " * rnd.randrange(60, 300)
            p[i] = (b'"' + p[i].strip(b'"') + pad + b'"') if rnd.random() < 0.5 else p[i] + pad
            l = b",".join(p)
        elif l and x < 0.025:
            l = l + b"," + b"x" * rnd.randrange(100, 200)  # too many fields, long
        elif l and x < 0.03 and f.parent.name == "events":
            l = l + b"\r"
        out.append(l)
    f.write_bytes(b"\n".join(out))
