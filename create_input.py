import sys

if len(sys.argv) < 2:
	print "usage : python create_input.py <input_size>"
	quit()

num=int(sys.argv[1])

fout=open("input.txt","w")
for i in range(1,num+1):
	fout.write(str(i)+"\n")

fout.close()
