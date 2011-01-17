import typedbytes
import sys


b = typedbytes.PairedInput(sys.stdin)
c = typedbytes.PairedOutput(sys.stdout)
for x in b:
    c.write(x)
