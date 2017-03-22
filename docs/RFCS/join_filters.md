- Feature Name: pushing down filters through joins
- Status: completed
- Start Date: 2016-12-15
- Authors: Radu
- RFC PR: [#12430](https://github.com/cockroachdb/cockroach/pull/12430)
- Cockroach Issue: [#12418](https://github.com/cockroachdb/cockroach/issues/12418)

# Summary

A short write-up of the rules for pushing down filters and ON conditions when
doing JOINs.

# Motivation

Optimization of queries that include JOINs.

# Detailed design

There are two expressions to consider:
 - ON expression (e.g. `SELECT * FROM a JOIN b ON a.x = b.x`)
 - filter expression (e.g. `SELECT * FROM a JOIN b WHERE a.x = b.x`)

For the purpose of this discussion, we consider `USING` and `NATURAL JOIN` as an
implicit form of an `ON` expression.

We consider that the expression in question is made up of three expressions:
```
  expr = (leftExpr AND rightExpr AND combinedExpr)`
```
where:
 - `leftExpr` references only columns from the left table (I will use "table"
   to refer to any join operand); `rightExpr` 
 - `rightExpr` references only columns from the right table
 - `combinedExpr` is an expression that references columns from both tables.

## INNER JOIN

Inner joins are simple: there is no semantic difference between the ON
expression and the filter; any expression can be pushed down. The following are
all equivalent:

```sql
SELECT * FROM l JOIN r WHERE (leftExpr AND rightExpr AND combinedExpr)

SELECT * FROM l JOIN r ON (leftExpr AND rightExpr AND combinedExpr)

SELECT * FROM
  (SELECT * FROM l WHERE leftExpr)
JOIN
  (SELECT * FROM r WHERE rightExpr)
ON combinedExpr
```

## LEFT OUTER JOIN

In a left outer join, all rows from the left table end up in the output in some
form, for any ON condition.

Consider a general form:
```sql
SELECT * FROM
  l LEFT OUTER JOIN r ON (onLeft AND onRight AND onCombined)
WHERE (filterLeft AND filterRight AND filterCombined)
```

Observations:
 - `onRight` can be pushed down to `r`:
```sql
SELECT * FROM
  l LEFT OUTER JOIN (SELECT * from r WHERE onRight)
ON (onLeft AND onCombined)
WHERE (filterLeft AND filterRight AND filterCombined)
```

 - `filterLeft` can be pushed down to `l`:
```sql
SELECT * FROM
  (SELECT * from l WHERE filterLeft) JOIN r
ON (onLeft AND onRight AND onCombined)
WHERE (filterRight AND filterCombined)
```

This is all we can do in general. If we know that the filter expression always
evaluates to false if all columns on the right side are all NULLs, the query is
equivalent to an inner join (and the inner join optimizations would apply).

## RIGHT OUTER JOIN

A right outer join is symmetrical to a left outer join, so all the above apply
if we swap `left` and `right`.

## FULL OUTER JOIN

For a full outer join, we cannot push down any filters.

As before, if we know that the filter excludes rows that have NULLs on all the
left (or right, or both) columns, the query would be equivalent to a left outer
(or right outer, or inner, respectively) join.
