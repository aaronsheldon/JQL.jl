# Overloadable skinner for column names
# Make this a singleton and overload call to ensure it is parameterized by abstract data
# table
immutable DataView{N}
	table::AbstractDataTable{N}
end

# Generate the column name skins for the column vector store. Each column name-index pair is
# stored in a pair of constant functions, essentially forming a look up table, or
# enumeration. The functions are typed by the singleton types encoding the object id of the
# column vector tuple store, and either the symbol of the column name, or the index of the
# column.
function _DataView(view::DataView, columnname::Symbol, columnindex::Int64)
	eval(:(DataView(::Type{Val{$(view)}}, ::Type{Val{$(QuoteNode(columnname))}}) = $(columnindex)))
	eval(:(DataView(::Type{Val{$(view)}}, ::Type{Val{$(columnindex)}}) = $(QuoteNode(columnname))))
end

# Courtesy method to ensure the names are valid
Base.isvalid{N}(V::DataView{N}) =
	length(unique(getcolumnnames(V))) == N &&
	all([(getcolumnindex(V, getcolumnname(V, c)) == c)::Bool for c = 1:N])

# Iterable methods
Base.start(::DataView) = 1
Base.next(V::DataView, r::Int64) = (V[r], r + 1)
Base.done(V::DataView, r::Int64) = r > length(V)
Base.eltype(::Type{DataView}) = Symbol
Base.length{N}(V::DataView{N}) = N

# Collection methods
Base.isempty(D::DataView) = length(D) < 1
Base.endof(D::DataView) = length(D)

# Indexable methods
Base.setindex!(V::DataView, n::Symbol, c::Int64) = _DataView(V, n, c)
Base.getindex(V::DataView, c::Int64) = DataView(Val{V}, Val{c})
Base.getindex(V::DataView, n::Symbol) = DataView(Val{V}, Val{n})

# Array methods
Base.ndims(::DataView) = 1
Base.size(V::DataView) = (length(V))
Base.linearindexing(::Type{DataView}) = Base.LinearFast()
