import hadoopy

with hadoopy.TypedBytesFile() as fp:
    for x in fp:
        fp.write(x)
