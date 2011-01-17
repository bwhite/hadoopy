import hadoopy

while True:
    try:
        hadoopy._typedbytes.write_tb(hadoopy._typedbytes.read_tb())
    except StopIteration:
        break
