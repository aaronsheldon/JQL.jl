# Bucket all tables with the same column counts together in anticipation of get index
# overloading. This will minimize the number of overloaded functions added to the system.
abstract AbstractDataTable{N, R<:NTuple{N}, C<:NTuple{N, AbstractVector}}

# Tuple unroller: Un-tuple-ize a vector of tuples into a tuple of vectors
@generated function Base.convert{R<:Tuple}(::Type{Tuple{[Vector{r} for r in R.parameters]...}}, vs::AbstractVector{R})
	x = :(t = (); for i = 1:length(vs) end; t;)
	x.args[1].args[2].args = [:(Vector{$(r)}(length(vs))) for r in R.parameters][:]
	x.args[2].args[1].args[2].args = [:(@inbounds t[$(r)][i] = vs[i][$(r)]) for r = 1:length(R.parameters)][:]
	x
end

# Vector unroller: index a tuple of vectors into a tuple
@generated function Base.getindex{N}(D::AbstractDataTable{N}, r::Int64)
	x = :(())
	prepend!(x.args, [:(Base.getindex(getcolumns(D)[$(c)], r)) for c = 1:N])
	x
end

# Unroll value setting
@generated function Base.setindex!{N, R, C}(D::AbstractDataTable{N, R, C}, v::R, r::Int64)
	x = quote v end
	prepend!(x.args, [:(Base.setindex!(getcolumns(D)[$(c)], v[$(c)], r)) for c = 1:N])
	x
end

# Unroll vector emptying
@generate function Base.empty!{N}(D::AbstractDataTable{N})
	x = quote D end
	prepend!(x.args, [:(Base.empty!(getcolumns(D)[$(c)])) for c = 1:N])
	x
end

# All concrete types must implement getcolumns that returns NTuple{N, AbstractVector}
# getcolumns()

# Courtesy method to check that the table has been constructed properly
Base.isvalid{N, R, C}(D::AbstractDataTable{N, R, C}) =
	length(getcolumns(D)) == N &&
	length(R.parameters) == N &&
	length(C.parameters) == N &&
	minimum([length(getcolumns(D)[c]) for c = 1:N]) == maximum([length(getcolumns(D)[c]) for c = 1:N]) &&
	typeof(getcolumns(D)) == C &&
	all([(R.parameters[i] == C.parameters[i].parameters[1])::Bool for i = 1:N]) &&
	isvalid(Val{DataView}, D)

# Iterable methods
Base.start(::AbstractDataTable) = 1
Base.next(D::AbstractDataTable, r::Int64) = (D[r], r + 1)
Base.done(D::AbstractDataTable, r::Int64) = r > length(D)
Base.eltype{N, R, C}(::Type{AbstractDataTable{N, R, C}}) = R
Base.length{N}(D::AbstractDataTable{N}) = minimum([length(getcolumns(D)[c]) for c = 1:N])

# Collection methods
Base.isempty(D::AbstractDataTable) = length(D) < 1
Base.endof(D::AbstractDataTable) = length(D)

# Indexable methods
# A single column returns a vector; type unstable
# A single row and column pair returns an element; type unstable
Base.getindex{N, R, C}(D::AbstractDataTable{N, R, C}, c::Symbol) = getcolumns(D)[getcolumnindex(D, c)]::C.parameters[c]
Base.getindex{N, R, C}(D::AbstractDataTable{N, R, C}, c::AbstractString) = D[Symbol(c)]::C.parameters[c]
Base.getindex{N, R, C}(D::AbstractDataTable{N, R, C}, r::Int64, c::Int64) = getcolumns(D)[c][r]::R.parameters[c]
Base.getindex{N, R, C}(D::AbstractDataTable{N, R, C}, r::Int64, c::Symbol) = D[r, getcolumnindex(D, c)]::R.parameters[c]
Base.getindex{N, R, C}(D::AbstractDataTable{N, R, C}, r::Int64, c::AbstractString) = D[r, Symbol(c)]::R.parameters[c]

# Array methods
Base.ndims(::AbstractDataTable) = 2
Base.size{N}(D::AbstractDataTable{N}) = (length(D), N)
Base.linearindexing(::Type{AbstractDataTable}) = Base.LinearFast()
