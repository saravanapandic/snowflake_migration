procedure='a varchar,b d int,c int,a d djj'
x = procedure.split(',')
joins=""
len_splite=0
for i in x:
    if len_splite == 0:
        y=i.split(" ")
        print(y[len(y)-1])
        joins=joins +y[len(y)-1]
        len_splite =1
    else:
        y=i.split(" ")
        print(y[len(y)-1])
        joins=joins +','+y[len(y)-1]
        len_splite =1

    
print(joins)
