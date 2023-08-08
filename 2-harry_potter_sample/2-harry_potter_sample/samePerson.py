#!/usr/bin/env python
# -*- coding: utf-8 -*-

filename = 'person_name_list.txt'
file = open(filename,encoding='utf-8')
lines = file.readlines()
d = {}
named = {}
c = 0
for line in lines:
    line = line.split('\n')[0]
    name = line.split('·')[0]

    d.setdefault(name,c)
    c+=1

    if name in d.keys():
        named[line] = name
file.close()   #关闭文件



out = open('out1.txt','w',encoding='utf-8')
for line in lines:
    line = line.split('\n')[0]

    out.write(str(line)+','+str(d[named[line]])+'\n')
out.close()



# invertd = {}
# for k,v in d.items():
#     a = invertd.setdefault(v,[k])
#     if len(a) > 1:
#         invertd[v].append(k)

# out = open('out2.txt','w')
# for k,v in invertd.items():
#     out.write(str(k)+','+str(v)+'\n')
# out.close()
