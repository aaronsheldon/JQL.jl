**JQL** is not a **Q**uery **L**anguage
=======================================

[![Build Status](https://travis-ci.org/aaronsheldon/JQL.jl.svg?branch=master)](https://travis-ci.org/aaronsheldon/JQL.jl)

As the name implies this package is not a declarative syntax for Julia. This package provides an indexable type for lazy-eager hash joins. Included in this package are support types for constructing table like objects that are stored as tuples of vectors but are iterated through as a vector of tuples.

Joins
-----

Joins are implemented as iterable concrete leaf types of the abstract join type. The following joins are supported:

* Inner
* Full Outer
* Left Outer
* Right Outer
* Left Semi
* Right Semi
* Left Anti
* Right Anti

Tables
------

Table structures are supported through the concrete indexable type DataTable. The DataTable type is triple parameterized by the number of columns, the tuple type signature of a single row, and the signature of the tuple of vectors of the data store.

Column names are supported by a parameterized singleton DataView. This type is parameterized by the object identifier of the column store, and either the symbol naming the column, or the index of the column. The two subtypes reference each other through a pair of overloads of [Base.call()](http://docs.julialang.org/en/release-0.4/stdlib/base/?highlight=base.call#Base.call).
