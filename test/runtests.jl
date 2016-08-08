using JQL
using Base.Test

# Set up data for data view type testing
a = rand(0:9)
b = rand(0:9)
c = rand(0:9)

# Data view type existence
@test AbstractDataView <: Enum
@test AbstractDataView{a} <: AbstractDataView
@test AbstractDataView{a, b} <: AbstractDataView{a}
@test DataView <: AbstractDataView
@test DataView{a} <: AbstractDataView{a}
@test DataView{a, b} <: AbstractDataView{a, b}

# Undefined column fail test
@test_throws ErrorException AbstractDataView{a, b, c}
@test_throws ErrorException DataView{a, b, c}
@test_throws ErrorException DataView()
@test_throws ErrorException DataView{a}()
@test_throws ErrorException DataView{a, b}()

# Set up data for data view call overloading
goodid = rand(UInt64)
badid = (x = rand(UInt64); x == goodid ? x += 0x0000000000000001 : x; x)
goodname = rand([collect('A':'Z'); collect('a':'z')])
goodsymbol = Symbol(goodname)
badname = (x = rand([collect('A':'Z'); collect('a':'z')]); x == goodname ? x += 1 : x; x)
badsymbol = Symbol(badname)
goodindex = rand(Int64)
badindex = (x = rand(Int64); x == goodid ? x += 1 : x; x)

# Build the singleton types
_DataView(goodid, goodsymbol, goodindex)
@test DataView{goodid, goodsymbol}() == goodindex
@test DataView{goodid, goodindex}() == goodsymbol

# Make sure this does not add any other overloads
@test_throws ErrorException DataView{badid, goodsymbol}()
@test_throws ErrorException DataView{badid, goodindex}()
@test_throws ErrorException DataView{goodid, badsymbol}()
@test_throws ErrorException DataView{goodid, badindex}()
@test_throws ErrorException DataView{badid, badsymbol}()
@test_throws ErrorException DataView{badid, badindex}()
