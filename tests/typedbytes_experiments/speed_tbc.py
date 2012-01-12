import ctypedbytes as typedbytes
import sys


b = typedbytes.PairedInput(sys.stdin)
c = typedbytes.PairedOutput(sys.stdout)
c.writes(b)
