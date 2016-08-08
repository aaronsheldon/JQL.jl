# Specialize getindex overloading on the number of elements in the returned tuple. For each
# set of tables with the same number of columns dynamically overload getindex so that a
# tuple does not have to be dynamically generated using ANY type vectors; which leads to
# type instability during the call of get index. This also sidesteps splatting into tuples.
# The net result is that getting or setting into the table by tuple assignment is type
# stable.
function _getindex(N::Int64)
	x = :(Base.getindex(D::DataTable{$(N)}, r::Int64) = ())
	x.args[2].args[2].args = [:(D.columns[$(c)][r]) for c = 1:N][:]
	eval(x)
end

# A concrete table is specified by its column types
immutable DataTable{N, R, C} <: AbstractDataTable{N, R, C}
	columns::C
	object::UInt64

	# Columns constructor
	function call{C<:Tuple{Vararg{AbstractVector}}}(::Type{DataTable}, columns::C)
		ls = [length(c) for c in columns]
		if minimum(ls) != maximum(ls)
			error("Columns are not equal length")
		end
		_getindex(length(C.parameters))
		new{length(C.parameters), Tuple{[c.parameters[1] for c in C.parameters]...}, C}(columns, object_id(columns))
	end

	# Preallocation or empty constructor
	function call{R<:Tuple}(::Type{DataTable{R}}, rows::Int64 = 0)
		columns = ([Vector{r}(rows) for r in R.parameters]...)
		_getindex(length(R.parameters))
		new{length(R.parameters), R, Tuple{[Vector{r} for r in R.parameters]...}}(columns, object_id(columns))
	end
end

# ToDo: lots of nice contructors, like R style, dictionaries, pairs, etc...
# Functions that instantiate types, by definition, cannot be type stable.
function call{R<:Tuple}(::Type{DataTable{R}}, columnnames::AbstractString...)
	if length(columnnames) != length(R.parameters)
		error("Incorrect number of column names specified")
	end
	D = DataTable{R}()
	setcolumnnames(D.object, [columnames...])
	D
end
function call{R<:Tuple}(::Type{DataTable{R}}, rows::Int64, columnnames::AbstractString...)
	if length(columnnames) != length(R.parameters)
		error("Incorrect number of column names specified")
	end
	D = DataTable{R}(rows)
	setcolumnnames(D.object, [columnames...])
	D
end
function DataTable(columns::AbstractVector...)
	D = DataTable(columns)
	setcolumnnames(D.object, [Symbol(c) for c = 1:length(columns)])
	D
end
function DataTable(rows::Int64 = 0; keywords...)
	k = Vector{Symbol}(length(keywords))
	v = Vector{AbstractVector}(length(keywords))
	for i = 1:length(keywords)
		@inbounds begin
			if isa(keywords[i][2], AbstractVector)
				k[i] = keywords[i][1]
				v[i] = keywords[i][2]
			elseif isa(keywords[i][2], DataType)
				k[i] = keywords[i][1]
				v[i] = Vector{keywords[i][2]}(rows)
			end
		end
	end
	D = DataTable((v...))
	setcolumnnames(D.object, k)
	D
end

# Courtesy method to check that the table has not been messed with
Base.isvalid{N}(D::DataTable{N}) =
	minimum([length(D.columns[c]) for c = 1:N]) == maximum([length(D.columns[c]) for c = 1:N]) &&
	length(unique([DataView{D.object, c}() for c = 1:N])) == N &&
	all([DataView{D.object, DataView{D.object, c}()}() == c for c = 1:N]) &&
	length(D.columns) == N && D.object == object_id(D.columns) && isvalid(D::AbstractDataTable)

# Iterable methods
Base.length{N}(D::DataTable{N}) = minimum([length(D.columns[c]) for c = 1:N])

# Collection methods
function Base.empty!{N}(D::DataTable{N})
	for c = 1:N
		@inbounds empty!(D.columns[c])
	end
	D
end

# Indexable methods, ignore getindex, it is overloaded on explicit instantiation
# A single row gets a type stable tuple
# Base.getindex{N}(D::DataTable{N}, r::Int64) = ([D.columns[c][r] for c = 1:N]...)

# A single column returns a vector; type unstable
Base.getindex(D::DataTable, c::Symbol) = D.columns[DataView{D.object, c}()]
Base.getindex(D::DataTable, c::AbstractString) = D.columns[Symbol(c)]

# A single row and column pair returns an element; type unstable
Base.getindex(D::DataTable, r::Int64, c::Int64) = D.columns[c][r]
Base.getindex(D::DataTable, r::Int64, c::Symbol) = D[r, DataView{D.object, c}()]
Base.getindex(D::DataTable, r::Int64, c::AbstractString) = D[r, Symbol(c)]

# To prevent inadvertent casting only concrete types can create concrete types
# These are horribly not type stable. To prevent syntax collision to get a table both ranges
# must be specified. From all combinations of Int, {Vec, Ran}, Col excluding intXint and
# colXcol we have 9-2=7 overloads.
function Base.getindex(D::DataTable, r::Int64, cs::Union{Vector{Int64}, Range{Int64}})
	E = DataTable(([[D.columns[c][r]] for c in cs]...))
	setcolumnnames(E.object, [DataView{D.object, c}() for c in cs])
	E
end
function Base.getindex{N}(D::DataTable{N}, r::Int64, ::Colon)
	E = DataTable(([[D.columns[c][r]] for c = 1:N]...))
	setcolumnnames(E.object, [DataView{D.object, c}() for c = 1:N])
	E
end
function Base.getindex(D::DataTable, rs::Union{Vector{Int64}, Range{Int64}}, c::Int64)
	E = DataTable(([D.columns[c][rs]...]))
	setcolumnname(E.object, DataView{D.object, c}())
	E
end
function Base.getindex(D::DataTable, rs::Union{Vector{Int64}, Range{Int64}}, cs::Union{Vector{Int64}, Range{Int64}})
	E = DataTable(([[D.columns[c][rs]...] for c in cs]...))
	setcolumnnames(E.object, [DataView{D.object, c}() for c in cs])
	E
end
function Base.getindex{N}(D::DataTable{N}, rs::Union{Vector{Int64}, Range{Int64}}, ::Colon)
	E = DataTable(([[D.columns[c][rs]...] for c = 1:N]...))
	setcolumnnames(E.object, [DataView{D.object, c}() for c = 1:N])
	E
end
function Base.getindex(D::DataTable, ::Colon, c::Int64)
	E = DataTable((D.columns[c]))
	setcolumnname(E.object, DataView{D.object, c}())
	E
end
function Base.getindex(D::DataTable, ::Colon, cs::Union{Vector{Int64}, Range{Int64}})
	E = DataTable(([D.columns[c] for c in cs]...), [D.columnnames(cs)...])
	setcolumnnames(E.object, [DataView{D.object, c}() for c in cs]))
	E
end

# Symbol and string feeder methods
Base.getindex(D::DataTable, rs::Union{Vector{Int64}, Range{Int64}, Colon}, c::Symbol) = D[rs, DataView{D.object, c}()]
Base.getindex(D::DataTable, rs::Union{Vector{Int64}, Range{Int64}, Colon}, c::AbstractString) = D[rs, Symbol(c)]
Base.getindex(D::DataTable, rs::Union{Int64, Vector{Int64}, Range{Int64}, Colon}, cs::Vector{Symbol}) = D[rs, [DataView{D.object, c}() for c in cs]]
Base.getindex(D::DataTable, rs::Union{Vector{Int64}, Range{Int64}, Colon}, cs::AbstractString...) = D[rs, [Symbol(c) for c in cs]]
Base.getindex(D::DataTable, rs::Int64, c::AbstractString, cs::AbstractString...) = D[rs, [Symbol(c); [Symbol(c) for c in cs]]]

# Setting a whole row by a tuple is type stable
function Base.setindex!{N, R, C}(D::DataTable{N, R, C}, v::R, r::Int64)
	for c = 1:N
		@inbounds D.columns[c][r] = v[c]
	end
	v
end

# Setting single elements is not type stable
Base.setindex!(D::DataTable, v, r::Int64, c::Int64) = D.columns[c][r] = v
Base.setindex!(D::DataTable, v, r::Int64, c::Symbol) = D[r, DataView{D.object, c}()] = v
Base.setindex!(D::DataTable, v, r::Int64, c::AbstractString) = D[r, Symbol(c)] = v

# Table builders by concatentation: vcat works like UNION ALL, so first in gets
# naming rights. hcat works like inner join on row index.
function Base.vcat{N, R, C}(Ds::DataTable{N, R, C}...)
	E = DataTable(([vcat([D.columns(c) for D in Ds]...) for c = 1:N]...))
	setnames(E.object, [DataView{Ds[1].object, c}() for c = 1:N])
	E
end
function Base.hcat(Ds::DataTable...)
	E = DataTable((vcat([[D.columns...] for D in Ds]...)...), vcat([D.columnnames for D in Ds]...))
	setnames(E.object, vcat([[DataView{D.object, c}() for c = 1:length(D.columns)] for D in Ds]...))
	E
end

# Instance specific vector methods, treating the table like a vector of tuples
# This could be wrapped in a macro, because it is basically the same code: iterate through
# the columns applying the appropriate vector method.
Base.push!{N, R, C}(D::DataTable{N, R, C}, v::R) = insert(D, length(D) + 1, v)
function Base.insert!{N, R, C}(D::DataTable{N, R, C}, r::Int64, v::R)
	for c = 1:N
		@inbounds insert!(D.columns(c), r, v(c))
	end
	D
end
function Base.deleteat!{N}(D::DataTable{N}, I)
	for c = 1:N
		@inbounds deleteat!(D.columns[c], I)
	end
	D
end
function Base.splice!{N, R, C}(D::DataTable{N, R, C}, I, v::R)
	for c = 1:N
		@inbounds splice!(D.columns[c], I, v[c])
	end
end
function Base.splice!{N, R, C}(D::DataTable{N, R, C}, I, cs::C)
	for c = 1:N
		@inbounds splice!(D.columns[c], I, cs[c])
	end
end
function Base.append!{N, R, C}(D::DataTable{N, R, C}, cs::C)
	for c = 1:N
		@inbounds append!(D.columns[c], cs[c])
	end
	D
end
function Base.resize!{N}(D::DataTable{N}, rs::Int64)
	for c = 1:N
		@inbounds resize!(D.columns(c), rs)
	end
	D
end
Base.append!{N, R, C}(D::DataTable{N, R, C}, E::DataTable{N, R, C}) = append!(D, E.columns)
function Base.prepend!{N, R, C}(D::DataTable{N, R, C}, cs::C)
	for c = 1:N
		@inbounds prepend!(D.columns[c], cs[c])
	end
	D
end
Base.prepend!{N, R, C}(D::DataTable{N, R, C}, E::DataTable{N, R, C}) = prepend!(D, E.columns)
