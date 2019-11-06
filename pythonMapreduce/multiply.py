import MapReduce
import sys

mr = MapReduce.MapReduce()

# =============================
# Do not modify above this line

def mapper(record):
    if record[0] == 'a':
        for i in range(0,4):
            mr.emit_intermediate((record[1],i),(record[2],record[3]))    
    elif record[0] == 'b':
        for i in range(0,4):
            mr.emit_intermediate((i,record[2]),(record[1],record[3]))

def reducer(key, list_of_values):
    counter = [0]*5
    result=[0]*5
    final=0
    for v in list_of_values:
        idx = v[0]
        counter[idx]+=1
        if result[idx]==0:
            result[idx]+=v[1]
        else:
            result[idx]*=v[1]
    for i, r in enumerate(counter):
        if(r<2):
            result[i]=0
        else:
            final+=result[i]
    mr.emit((key[0],key[1],final))


# Do not modify below this line
# =============================
if __name__ == '__main__':
    inputdata = open(sys.argv[1])
    mr.execute(inputdata, mapper, reducer)
