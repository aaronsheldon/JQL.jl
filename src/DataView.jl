# Overloadable skinner for column names
function DataView end

# Generate the column name skins for the column vector store. Each column name-index pair is
# stored in a pair of constant functions, essentially forming a look up table, or
# enumeration. The functions are typed by the singleton types encoding the object id of the
# column vector tuple store, and either the symbol of the column name, or the index of the
# column.
function _DataView(columns::UInt64, columnname::Symbol, columnindex::Int64)
	eval(:(DataView(::Type{Val{$(columns)}}, ::Type{Val{$(QuoteNode(columnname))}}) = $(columnindex)))
	eval(:(DataView(::Type{Val{$(columns)}}, ::Type{Val{$(columnindex)}}) = $(QuoteNode(columnname))))
end

# Courtesy method to ensure the names are valid
Base.isvalid{N}(::Type{Val{DataView}}, D::AbstractDataView{N}) =
	length(unique(getcolumnnames(D))) == N &&
	all([(getcolumnindex(D, getcolumnname(D, c)) == c)::Bool for c = 1:N])

# Courtesy overloads to build the column names
setcolumnname(cs::UInt64, n::Symbol, c::Int64 = 1) = _DataView(cs, n, c)
setcolumnname(cs::UInt64, n::Symbol, c::Int64 = 1)
setcolumnname{T<:Union{AbstractString, Char}}(cs::UInt64, n::T, c::Int64 = 1) = setcolumnname(cs, Symbol(n), c)
function setcolumnnames(cs::UInt64, ns::Vector{Symbol})
	if length(ns) != length(unique(ns))
		error("Column names are not unique")
	end
	for n in 1:length(ns)
		@inbounds setcolumnname(cs, ns[n], n)
	end
end
setcolumnnames{T<:Union{AbstractString, Char}}(cs::UInt64, ns::AbstractVector{T}) = setcolumnnames(cs, [Symbol(n) for n in ns])

# Methods to retrieve column information
getcolumnname(cs::UInt64, c::Int64) = DataView(Val{cs}, Val{c})
getcolumnname(D::AbstractDataTable, c::Int64) = getcolumnname(object_id(getcolumns(D)), c)
getcolumnindex(cs::UInt64, c::Symbol) = DataView(Val{cs}, Val{c})
getcolumnindex(D::AbstractDataTable, c::Symbol) = getcolumnindex(object_id(getcolumns(D)), c)
getcolumnindex{T<:Union{AbstractString, Char}}(cs::UInt64, c::T) = getcolumnindex(cs, Symbol(c))
getcolumnindex{T<:Union{AbstractString, Char}}(D::AbstractDataTable, c::T) = getcolumnindex(D, Symbol(c))
