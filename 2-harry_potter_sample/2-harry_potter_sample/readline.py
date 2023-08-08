filename = 'juntuan.txt'
file = open(filename,encoding='utf-8')
done = 0
d = {}
tmpkards = []
tmpids = []
while not  done:
        aLine = file.readline()
        if(aLine != ''):
            if aLine[0]=='1':
                tmpkards.append(''.join(aLine.split(' ')[2:]))
            elif aLine[0]=='%':
                for i in range(5,len(aLine)):
                    if i%2==1 and aLine[i]!=';':
                        tmpids.append(''.join(aLine[i:i+2]))
                for i,x in enumerate(tmpids):
                    d[tmpkards[i]] = x
                tmpids = []
                tmpkards = []
        else:
            done = 1
out = open('out1.txt','w')
for k,v in d.items():
    out.write(str(k[0:-1])+','+str(v)+'\n')
out.close()

file.close()   #关闭文件