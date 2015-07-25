HiveQueryMRJoin
==================
<h3><b>Converting Hive query into Java MapReduce using reduce side join</b></h3>

License
 
Apache licensed.

<br>
Joins are possibly one of the most complex operations one can execute in MapReduce. 
By design, MapReduce is very good at processing large data sets by looking at every record or group in isolation, so joining two very large data sets together does not fit into the paradigm gracefully. 

What Reduce side join performs : 
Map 
1.Mapper starts the join operation by reading different input files and outputs all records to Reducer. 
2.Tag each records for identifying from which source the record has arrived. 
3.Key of the map output has to be the join key 

Reducer 
1.Reducer will get shuffled data from all files with common key. 
2.Combines the record for both depending upon tag attribute.

Problem statement : Find total amount purchased along with number of transaction for each customer. 

Customer table will have unique customer ID along with other details of Customer. 
***********Customer table*********** 
cust_id|cust_fname|cust_lname|location 
867230|William|smith|New York 
973239|Alex|bard|Canada 
124847|Michael|george|Washington 

Purchases will have unique purchase Id for each purchase. There will be multiple purchases for each customer. 
***********Purchases table*********** 
purchase_id|cid|store 
23|973239|Wallmart 
99|234958|DStore 
25|973239|Oasis 
66|973239|Wallmart 
33|124847|Pearson 
83|973239|Trad| 
72|124847|Wallmart 
54|038403|Suz 

Transaction table will have unique transaction Id for each transaction along with purchase amount for each transaction 
***********Transaction table*********** 
purchase_id|transa_date|purchase_amt 
23|2015-01-23|23434 
99|2015-01-12|89734 
25|2014-03-28|36495 
66|2015-05-20|76577 
33|2015-03-17|9736 
83|2015-01-10|32873 
72|2015-01-04|453822 
54|2014-02-13|3290843 

Solution: 
We will first do a simple Cross join on Customer and Purchases table using customer id from both tables and prepare a file where we have data of both in a single file. 

The HIVE query is : 
select c.cust_fname ,sum(t.purchase_amt) ,count(*) from customer c 
inner join purchases p 
on c.cust_id=p.cid 
inner join transaction t 
on p.purchase_id=t.purchase_id 
group by c.cust_id,c.cust_fname; 

The output of the above Hive query is 
alex|169379|4 
michale|463558|2 

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

