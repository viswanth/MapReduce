import MapReduce
import sys

"""
Word Count Example in the Simple Python MapReduce Framework
"""

"""
The input to the map function will be a row of a matrix represented as a list. 
Each list will be of the form [matrix, i, j, value] where matrix is a string and i, j, and value are integers.

The first item, matrix, is a string that identifies which matrix the record originates from. 
This field has two possible values: "a" indicates that the record is from matrix A and "b" indicates that the record is from matrix B
"""

"""
The output from the reduce function will also be a row of the result matrix represented as a tuple. 
Each tuple will be of the form (i, j, value) where each element is an integer.

You can test your solution to this problem using matrix.json:

$ python multiply.py matrix.json
You can verify your solution by comparing your result with the file multiply.json.
"""

mr = MapReduce.MapReduce()

# =============================
# Do not modify above this line

def mapper(record):
    matrix = record[0]
    row = record[1]
    col = record[2]
    value = record[3]
	
    if matrix == 'a':
        for k in range(0,5):
            mr.emit_intermediate((row,k),[matrix,col,value])

    else:
        for i in range(0,5):
            mr.emit_intermediate((i,col),[matrix,row,value])		

def reducer(key, list_of_values):
    a_values = filter(lambda cell: cell[0] == 'a', list_of_values)
    b_values = filter(lambda cell: cell[0] == 'b', list_of_values)

    a_set = set(map(lambda s: s[1], a_values))
    b_set = set(map(lambda s: s[1], b_values))
    a_b_set = a_set & b_set

    b_rows = filter(lambda row: row[1] in a_b_set, b_values)
    a_cols = filter(lambda row: row[1] in a_b_set, a_values)

    a_b_mult = map(lambda x: x[0][2] * x[1][2], zip(b_rows, a_cols))

    mr.emit((key[0],key[1],sum(a_b_mult)))	

# Do not modify below this line
# =============================
if __name__ == '__main__':
  inputdata = open(sys.argv[1])
  mr.execute(inputdata, mapper, reducer)
