### What is the purpose of this?

This is a toy SQL problem, where we will parse a SQL query, and we will create a tree from the query. Note this project
is not meant to do full SQL parsing - it is only meant to take simple queries and learn how it all works.

In this part, we will only parse the query. In next samples we will :

1. run rules to re-structure the tree
2. create a execution plan
3. execute a query

#### How to run?

Run ```SelectQueryParserTest``` test to try this out.

#### Query Parsing

This branch has a simple example to parse a query and create a Tree from the input query. Given below are toy queries
and the output tree:

```sql
SELECT `id`, `name`, `created_at`
             FROM `main_table`
             WHERE `main_table`.`id` > 10;
             
Output:
UnresolvedSingleSelect: 
	UnresolvedProjection: `id`,`name`,`created_at`
	UnresolvedFromClause: table=`main_table`
	UnresolvedWhere: table=`main_table` filed=`id` operator=>             
```

```sql
SELECT `id`, `name`, `created_at`
             FROM `main_table` INNER JOIN `other_table`
                ON `main_table`.`column_name_main` = `other_table`.`column_name_other`
             WHERE `main_table`.`id` > 10;
                 
Output:
UnresolvedSingleSelect: 
	UnresolvedProjection: `id`,`name`,`created_at`
	UnresolvedFromClause: table=`main_table`
		UnresolvedJoin: PrimaryRelation=`main_table` SecondaryRelation=`other_table`
	UnresolvedWhere: table=`main_table` filed=`id` operator=>
```