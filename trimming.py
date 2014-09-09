import MapReduce
import sys

"""
Word Count Example in the Simple Python MapReduce Framework
"""

"""
Each input record is a 2 element list [sequence id, nucleotides] where sequence id is a string 
representing a unique identifier for the sequence and nucleotides is a string representing a sequence of nucleotides
"""

"""
The output from the reduce function should be the unique trimmed nucleotide strings.

You can test your solution to this problem using dna.json:

$ python trimming.py dna.json

You can verify your solution by comparing your result with the file unique_trims.json.
"""

mr = MapReduce.MapReduce()

# =============================
# Do not modify above this line

def mapper(record):
    seq_id = record[0]
    nuc = record[1]
	
    trimmed = nuc[:len(nuc) - 10]
	
    mr.emit_intermediate(trimmed,seq_id)

def reducer(key, list_of_values):
    mr.emit(key)

# Do not modify below this line
# =============================
if __name__ == '__main__':
  inputdata = open(sys.argv[1])
  mr.execute(inputdata, mapper, reducer)
