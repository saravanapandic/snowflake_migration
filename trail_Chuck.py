procedure='a varchar,b d int'
x = procedure.split(',')
y=[]
for i in x:
    y.clear()
    y=i.split(" ")
    print(len(y))
    print(y[len(y)-1])

print(len(x))
print(x)