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

# Courtesy overloads to build and return information about the column names
setcolumnname(cs::UInt64, n::Symbol, c::Int64 = 1) = _DataView(cs, n, c)
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
getcolumnname(cs::UInt64, c::Int64) = DataView(Val{cs}, Val{c})
getcolumnindex(cs::UInt64, c::Symbol) = DataView(Val{cs}, Val{c})
getcolumnindex{T<:Union{AbstractString, Char}}(cs::UInt64, c::T) = getcolumnindex(cs, Symbol(c))

# Bucket all tables with the same column counts together in anticipation of get index
# overloading. This will minimize the number of overloaded functions added to the system.
abstract AbstractDataTable{N, R<:NTuple{N}, C<:NTuple{N, AbstractVector}}

# Courtesy method to check that the table has been constructed properly
Base.isvalid{N, R, C}(::AbstractDataTable{N, R, C}) =
	N == length(R.parameters) && N == length(C.parameters) &&
	all([(R.parameters[i] == C.parameters[i].parameters[1])::Bool for i = 1:N])

# Iterable methods
Base.start(::AbstractDataTable) = 1
Base.next(D::AbstractDataTable, r::Int64) = (D[r], r + 1)
Base.done(D::AbstractDataTable, r::Int64) = r > length(D)
Base.eltype{N, R, C}(::Type{AbstractDataTable{N, R, C}}) = R

# Collection methods
Base.isempty(D::AbstractDataTable) = length(D) < 1
Base.endof(D::AbstractDataTable) = length(D)

# Array methods
Base.ndims(::AbstractDataTable) = 2
Base.size{N}(D::AbstractDataTable{N}) = (length(D), N)
Base.linearindexing(::Type{AbstractDataTable}) = Base.LinearFast()

# Un-tuple-ize a vector of tuples into a tuple of vectors
@generated convert{R<:Tuple}(::Type{Tuple{[Vector{r} for r in R.parameters]...}}, vs::AbstractVector{R})
	x = :(t = (); for i = 1:length(vs) end; t;)
	x.args[1].args[2].args = [:(Vector{$(r)}(length(vs))) for r in R.parameters]
	x.args[2].args[1].args[2].args = [:(@inbounds t[$(r)][i] = vs[i][$(r)]) for r = 1:length(R.parameters)]
	x
end


# Thin wrappers for the column vector methods
function Base.push!{N, R, C}(D::AbstractDataTable{N, R, C}, vs::R...)
	t::C = ([c(length(vs)) for c in C.parameters]...)
	for j = 1:length(vs)
		for i = 1:N
			@inbounds t[i][j] = vs[j][i]
		end
	end
	append!(D, t)
end
function Base.pop!{N, R, C}(D::AbstractDataTable{N, R, C}, r::Int64 = length(D), d::R = D[r])
	if 0 < r && r <= length(D)
		v = D[r]
		deleteat!(D, r)
	else
		v = d
	end
	v
end
function Base.unshift!{N, R, C}(D::AbstractDataTable{N, R, C}, vs::R...)
	t::C = ([c(length(vs)) for c in C.parameters]...)
	for j = 1:length(vs)
		for i = 1:N
			@inbounds t[i][j] = vs[j][i]
		end
	end
	prepend!(D, t)
end
Base.shift!(D::AbstractDataTable) = pop!(D, 1)
function Base.splice!{N, R, C}(D::AbstractDataTable{N, R, C}, I, vs::AbstractVector{R})
	t::C = ([c(length(vs)) for c in C.parameters]...)
	for j = 1:length(vs)
		for i = 1:N
			@inbounds t[i][j] = vs[j][i]
		end
	end
	splice!(D, I, t)
	vs
end
