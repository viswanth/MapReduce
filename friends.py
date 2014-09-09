import MapReduce
import sys

"""
Word Count Example in the Simple Python MapReduce Framework
"""

"""
Each input record is a 2 element list [personA, personB] where personA is a string representing 
the name of a person and personB is a string representing the name of one of personA's friends. 
Note that it may or may not be the case that the personA is a friend of personB.
"""

"""
The output should be a pair (person, friend_count) where person is a string and friend_count 
is an integer indicating the number of friends associated with person.

You can test your solution to this problem using friends.json:

$ python friends.py friends.json

You can verify your solution by comparing your result with the file friend_count.json.
"""

mr = MapReduce.MapReduce()

# =============================
# Do not modify above this line

def mapper(record):
    person = record[0]
    friend = record[1]
    mr.emit_intermediate(person,[person,friend])
    mr.emit_intermediate(friend,[friend,person])

def reducer(key, list_of_values):
    pairs = [tuple(value) for value in list_of_values]
    all_set = set(pairs)
	
    dups_set = set([pair for pair in pairs if pairs.count(pair) > 1])
	
    asym_friends = all_set ^ dups_set
    for pair in asym_friends:
        mr.emit(pair)

# Do not modify below this line
# =============================
if __name__ == '__main__':
  inputdata = open(sys.argv[1])
  mr.execute(inputdata, mapper, reducer)
