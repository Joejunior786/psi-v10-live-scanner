import ast, base64, re, zlib

def unpack(path):
    raw=open(path,"r",encoding="utf-8").read()
    m=re.search(r'base64\.b64decode\("([^"]+)"\)', raw)
    if not m:
        return raw
    return zlib.decompress(base64.b64decode(m.group(1))).decode("utf-8")

src=unpack("psi_v14_pinpoint.py")
tests=unpack("test_v14_pinpoint.py")
tree=ast.parse(src)
print("=== V14 AUDIT SUMMARY ===")
for node in tree.body:
    if isinstance(node,(ast.Assign,ast.AnnAssign)):
        targets=node.targets if isinstance(node,ast.Assign) else [node.target]
        try: val=ast.literal_eval(node.value)
        except Exception: continue
        for t in targets:
            if isinstance(t,ast.Name) and (
                t.id in {"REVISION","AUTHORITY_CHAIN","ROLE","EXECUTION_AUTHORITY"}
                or t.id.startswith(("ML","MAX_","MIN_","BEAST","EXHAUST","BREAKOUT","HARD"))
            ):
                print(f"CONST {t.id}={val!r}")

funcs=[]
for node in tree.body:
    if isinstance(node,(ast.FunctionDef,ast.AsyncFunctionDef)):
        funcs.append(node.name)
print("FUNCTIONS", ",".join(funcs))
keywords=("gate","hard","safety","beast","exhaust","breakout","ml","prob","candidate","rank","top","augment","install","supervisor","outcome","authority","engine","setup")
for node in tree.body:
    if isinstance(node,(ast.FunctionDef,ast.AsyncFunctionDef)) and any(k in node.name.lower() for k in keywords):
        seg=ast.get_source_segment(src,node)
        if seg:
            print(f"\n=== FUNCTION {node.name} ===\n{seg[:12000]}")
print("\n=== TEST NAMES ===")
tt=ast.parse(tests)
for node in ast.walk(tt):
    if isinstance(node,(ast.FunctionDef,ast.AsyncFunctionDef)) and node.name.startswith("test_"):
        print(node.name)
