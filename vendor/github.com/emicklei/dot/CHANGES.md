# Change history of the dot package


## v0.15.0 - 2020-10-30

- add Node initializer, see Issue #15
- add Edge initializer

## v0.14.0 - 2020-08-25

- add Attrs for conveniently adding multiple label=value attribute pairs.

## v0.13.0 - 2020-08-22

- add FindSubgraph

## v0.12.0 - 2020-08-20

- Added style methods to Edge to easily add bold,dotted and dashed lines. (#21)

## v0.11.0 - 2020-05-16

- add functionality to find node by id
- add function to find all nodes of a graph

## v0.10.2 - 2020-01-31 

- Fix indexing subgraphs by label ; must use id. Issue #16
- Add Label(newLabel) to Graph
- Add Delete(key) to AttributesMap
- Use internal ids for subgraphs

## v0.10.0

- Allow setting same rank for a group of nodes
- Introduce Literal attribute type
- Introduce Node.Label(string) function

## v0.9.2 and earlier

- Add support for HTML attributes.
- fixed undirected transitions
- Change how node is printed, so that attributes only affect individual node
