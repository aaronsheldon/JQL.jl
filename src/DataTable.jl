# A concrete table is specified by its column types
immutable DataTable{N, R, C} <: AbstractDataTable{N, R, C}
	columns::C

	# Columns constructor
	function call{C<:Tuple{Vararg{AbstractVector}}}(::Type{DataTable}, columns::C)
		ls = [length(c) for c in columns]
		if minimum(ls) != maximum(ls)
			error("Columns are not equal length")
		end
		_getindex(length(C.parameters))
		new{length(C.parameters), Tuple{[c.parameters[1] for c in C.parameters]...}, C}(columns)
	end

	# Preallocation constructor
	function call{R<:Tuple}(::Type{DataTable{R}}, rows::Int64)
		columns = ([Vector{r}(rows) for r in R.parameters]...)
		_getindex(length(R.parameters))
		new{length(R.parameters), R, Tuple{[Vector{r} for r in R.parameters]...}}(columns)
	end
end

# ToDo: lots of nice contructors, like R style, dictionaries, pairs, etc...
# Functions that instantiate types, by definition, cannot be type stable.
function call{R<:Tuple}(::Type{DataTable{R}}, columnnames::AbstractString...)
	if length(columnnames) != length(R.parameters)
		error("Incorrect number of column names specified")
	end
	D = DataTable{R}(0)
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

# Setting single elements is not type stable
Base.setindex!(D::DataTable, v, r::Int64, c::Int64) = D.columns[c][r] = v
Base.setindex!(D::DataTable, v, r::Int64, c::Symbol) = D[r, DataView{D.object, c}()] = v
Base.setindex!(D::DataTable, v, r::Int64, c::AbstractString) = D[r, Symbol(c)] = v

# Table builders by concatentation: vcat works like UNION ALL, so first in gets
# naming rights. hcat works like inner join on row index.
function Base.vcat{N, R, C}(Ds::DataTable{N, R, C}...)
	E = DataTable(([vcat([D.columns(c) for D in Ds]...) for c = 1:N]...))
	setcolumnnames(E.object, [DataView{Ds[1].object, c}() for c = 1:N])
	E
end
function Base.hcat(Ds::DataTable...)
	E = DataTable((vcat([[D.columns...] for D in Ds]...)...), vcat([D.columnnames for D in Ds]...))
	setcolumnnames(E.object, vcat([[DataView{D.object, c}() for c = 1:length(D.columns)] for D in Ds]...))
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
