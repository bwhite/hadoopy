from subprocess import Popen, PIPE
p1 = Popen(['cat'], stdin=PIPE, stdout=PIPE, close_fds=True)
p2 = Popen(['grep', 'a'], stdin=p1.stdout, stdout=PIPE, close_fds=True)
p1.stdin.write("aaaaaaaaaaaaaaaa\n")
p1.stdin.close()
print p2.stdout.read()
