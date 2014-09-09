MapReduce
=========

Will be designing and implementing MapReduce algorithms for a variety of common data processing tasks

Has a python library called MapReduce.py that implements the MapReduce programming model. The framework faithfully implements the MapReduce programming model, but it executes entirely on a single machine -- it does not involve parallel computation

The various problems that are addressed using this design paradigm are: 

1) Create an Inverted index. Given a set of documents, an inverted index is a dictionary where each word is associated with a list of the document identifiers in which that word appears.

2) Implement a relational join as a MapReduce query

Consider the following query:

SELECT * 
FROM Orders, LineItem 
WHERE Order.order_id = LineItem.order_id

Your MapReduce query should produce the same result as this SQL query executed against an appropriate database.

You can consider the two input tables, Order and LineItem, as one big concatenated bag of records that will be processed by the map function record by record.

3)Consider a simple social network dataset consisting of a set of key-value pairs (person, friend) representing a friend relationship between two people. Describe a MapReduce algorithm to count the number of friends for each person.

4)The relationship "friend" is often symmetric, meaning that if I am your friend, you are my friend. Implement a MapReduce algorithm to check whether this property holds. Generate a list of all non-symmetric friend relationships.

5)Consider a set of key-value pairs where each key is sequence id and each value is a string of nucleotides, e.g., GCTTCCGAAATGCTCGAA....

Write a MapReduce query to remove the last 10 characters from each string of nucleotides, then remove any duplicates generated.

6)Assume you have two matrices A and B in a sparse matrix format, where each record is of the form i, j, value. Design a MapReduce algorithm to compute the matrix multiplication A x B
