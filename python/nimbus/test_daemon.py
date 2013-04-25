from self.self_subprocess import call

for i in range(100):
    call("echo '%s Hello,Levi.' &"%i)
