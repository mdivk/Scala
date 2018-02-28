Problem Scenario 48 : You have been given below Python code snippet, with intermediate output. 
We want to take a list ot records about people and then we want to sum up their ages and count them. 
So tor this example the type in the RDD will be a Dictionary in the format of {name: NAME, age:AGE, gender:GENDER}. 

The result type will be a tuple that looks like so (Sum ot Ages, Count) 

people = O 
people.append({'name':'Amit', 'age':45,'gender':'M'}) 
people.append({'name':'Ganga', 'age':43,'gender':'F'}) 
people.append({'name':'John', 'age':28,'gender':'M'}) 
people.append({'name':'Lolita', 'age':33,'gender':'F'}) 
people.append({'name':'Dont Know', 'age':18,'gender':'T'}) 
peopleRdd=sc.parallelize(people) //Create an RDD 
peopleRdd.aggregate((0,0), seqOp, combOp) 

//Output ot above line : 167, 5) 
Now define two operation seqOp and combOp , such that 

seqOp : Sum the age ot all people as well count them, in each partition. 
combOp : Combine results from all partitions. 

==================================================================================

Solution : 

seqOp = (lambda x,y: (x[0] +y[‘age’],x[1] + 1)) 
combOp = (lambda x,y: (x[O] + Y[O], x[l] + Y[l])) 
