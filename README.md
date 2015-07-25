HiveQueryMRJoin
==================
<h3><b>Converting Hive query into Java MapReduce using reduce side join</b></h3>

License
 
Apache licensed.

<br>
Joins are possibly one of the most complex operations one can execute in MapReduce. 
By design, MapReduce is very good at processing large data sets by looking at every record or group in isolation, so joining two very large data sets together does not fit into the paradigm gracefully. 

What Reduce side join performs : <br>
Map <br>
1.Mapper starts the join operation by reading different input files and outputs all records to Reducer. <br>
2.Tag each records for identifying from which source the record has arrived. <br>
3.Key of the map output has to be the join key <br>
<br>
Reducer <br> 
1.Reducer will get shuffled data from all files with common key. <br>
2.Combines the record for both depending upon tag attribute.<br>

<h5>Problem statement : </h5> Find total amount purchased along with number of transaction for each customer. <br>
<br>
Customer table will have unique customer ID along with other details of Customer.
<br>
****************Customer table****************
<br>
cust_id|cust_fname|cust_lname|location 
<br>
867230|William|smith|New York 
<br>
973239|Alex|bard|Canada 
<br>
124847|Michael|george|Washington 
<br>
<br>
Purchases will have unique purchase Id for each purchase. There will be multiple purchases for each customer.<br> 
<br>
****************Purchases table****************
<br>
purchase_id|cid|store
<br>
23|973239|Wallmart
<br>
99|234958|DStore 
<br>
25|973239|Oasis 
<br>
66|973239|Wallmart 
<br>
33|124847|Pearson 
<br>
83|973239|Trad| 
<br>
72|124847|Wallmart 
<br>
54|038403|Suz 
<br>
<br>
Transaction table will have unique transaction Id for each transaction along with purchase amount for each transaction 
<br>
****************Transaction table****************
<br>
purchase_id|transa_date|purchase_amt 
<br>
23|2015-01-23|23434 
<br>
99|2015-01-12|89734 
<br>
25|2014-03-28|36495 
<br>
66|2015-05-20|76577 
<br>
33|2015-03-17|9736 
<br>
83|2015-01-10|32873 
<br>
72|2015-01-04|453822 
<br>
54|2014-02-13|3290843 
<br>
<br>
<h5>Solution: </h5> 
We will first do a simple Cross join on Customer and Purchases table using customer id from both tables and prepare a file where we have data of both in a single file. 

<h5>The HIVE query is : </h5>
select c.cust_fname ,sum(t.purchase_amt) ,count(*) from customer c 
inner join purchases p 
on c.cust_id=p.cid 
inner join transaction t 
on p.purchase_id=t.purchase_id 
group by c.cust_id,c.cust_fname; 

<h5>Output </h5>
alex|169379|4 <br>
michael|463558|2 

```
hadoop jar HiveMRJoin.jar /sreejith/hive-loc/customer/data/Customer /sreejith/hive-loc/purchases/data/Purchases /sreejith/hive-loc/transaction/data/Transaction /sreejith/prac/output1 /sreejith/prac/output2 /sreejith/prac/FinalOutput
```
1. First argument- Input File of Customer <br>
2. Second argument- Input File of Purchases <br>
3. Third argument- Input File of Transaction <br>
4. Fourth argument- Intermediate Output directory 1<br>
5. Fifth argument- Intermediate Output directory 2<br>
6. Third argument Final Output Result<br>
<br>

