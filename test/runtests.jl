using JQL
using Base.Test

# Undefined column fail test
@test_throws MethodError DataView()

# Set up data for data view call overloading
test00id = rand(UInt64)
test00index = rand(Int64)
test00name = rand([collect('A':'Z'); collect('a':'z')])
test00symbol = Symbol(test00name)

# Build the singleton types
_DataView(test00id, test00symbol, test00index)
@test DataView(Val{test00id}(), Val{test00symbol}()) == test00index
@test DataView(Val{test00id}(), Val{test00index}()) == test00symbol

# Set up test data for courtesy functions
test01id = rand(UInt64)
test01index = rand(Int64)
test01name = rand([collect('A':'Z'); collect('a':'z')])
test01symbol = Symbol(test01name)

# Build the singleton types
setcolumnname(test01id, test01symbol, test01index)
@test DataView(Val{test01id}(), Val{test01symbol}()) == test01index
@test DataView(Val{test01id}(), Val{test01index}()) == test01symbol

# Set up test data for courtesy functions
test02id = rand(UInt64)
test02index = rand(Int64)
test02name = rand([collect('A':'Z'); collect('a':'z')])
test02symbol = Symbol(test02name)

# Build the singleton types
setcolumnname(test02id, test02name, test02index)
@test DataView(Val{test02id}(), Val{test02symbol}()) == test02index
@test DataView(Val{test02id}(), Val{test02index}()) == test02symbol

# Make sure unique check works
@test_throws ErrorException setcolumnnames(rand(UInt64), fill(Symbol(rand([collect('A':'Z'); collect('a':'z')])), rand(1:10)))

# Set up test data for courtesy functions
test03id = rand(UInt64)
test03indexes = collect(1:52)
test03names = [collect('A':'Z'); collect('a':'z')][randperm(52)]
test03symbols = [Symbol(t) for t in test03names]

# Build the singleton types
setcolumnnames(test03id, test03symbols)
@test all([DataView(Val{test03id}(), Val{test03symbols[i]}()) == test03indexes[i] for i = 1:52])
@test all([DataView(Val{test03id}(), Val{test03indexes[i]}()) == test03symbols[i] for i = 1:52])

# Set up test data for courtesy functions
test04id = rand(UInt64)
test04indexes = collect(1:52)
test04names = [collect('A':'Z'); collect('a':'z')][randperm(52)]
test04symbols = [Symbol(t) for t in test04names]

# Build the singleton types
setcolumnnames(test04id, test04names)
@test all([(DataView(Val{test04id}(), Val{test04symbols[i]}()) == test04indexes[i])::Bool for i = 1:52])
@test all([(DataView(Val{test04id}(), Val{test04indexes[i]}()) == test04symbols[i])::Bool for i = 1:52])

# Set up test data for courtesy functions
test05id = rand(UInt64)
test05index = rand(Int64)
test05name = rand([collect('A':'Z'); collect('a':'z')])
test05symbol = Symbol(test05name)

# Build the singleton types
setcolumnname(test05id, test05name, test05index)
@test getcolumnindex(test05id, test05symbol) == test05index
@test getcolumnindex(test05id, test05name) == test05index
@test getcolumnname(test05id, test05index) == test05symbol
