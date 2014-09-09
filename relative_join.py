import MapReduce
import sys

"""
Word Count Example in the Simple Python MapReduce Framework
"""

"""
Each input record is a list of strings representing a tuple in the database. Each list element corresponds to a different attribute of the table

The first item (index 0) in each record is a string that identifies the table the record originates from. This field has two possible values:

"line_item" indicates that the record is a line item.
"order" indicates that the record is an order.
The second element (index 1) in each record is the order_id.

LineItem records have 17 attributes including the identifier string.

Order records have 10 elements including the identifier string.
"""

"""
The output should be a joined record: a single list of length 27 that contains the attributes from the order record 
followed by the fields from the line item record. Each list element should be a string.

You can test your solution to this problem using records.json:

$ python relative_join.py records.json

You can can compare your solution with join.json.
"""

mr = MapReduce.MapReduce()

# =============================
# Do not modify above this line

def mapper(record):
    person = record[0]
	mr.emit_intermediate(person,1)

def reducer(key, list_of_values):
    mr.emit((key,sum(list_of_values)))

# Do not modify below this line
# =============================
if __name__ == '__main__':
  inputdata = open(sys.argv[1])
  mr.execute(inputdata, mapper, reducer)
