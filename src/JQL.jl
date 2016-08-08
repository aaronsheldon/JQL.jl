module JQL
# JQL is not a Query Language
#
# Wrap lazy-eager hash joins, of various flavours, in an iterator

import Base.∩, Base.∪

export AbstractJoinType, Inner, Outer, Left, Right, LeftSemi, RightSemi, AbstractJoin, Join, ∪, ∩

abstract AbstractJoinType <: Enum
abstract AbstractJoin{J<:AbstractJoinType, I, T}
immutable Inner <: AbstractJoinType end
immutable Outer <: AbstractJoinType end
immutable Left <: AbstractJoinType end
immutable Right <: AbstractJoinType end
immutable LeftSemi <: AbstractJoinType end
immutable RightSemi <: AbstractJoinType end

# Light weight pointers and metadata of the sources to join
immutable Join{J<:AbstractJoinType, I, T} <: AbstractJoin{J, I, T}
	large::I
	small::I
	parity::Bool
	ratio::Int64
	remainder::Int64
	function call{J<:AbstractJoinType, I}(::Type{Join}, ::Type{J}, left::I, right::I)
		if eltype(left) != eltype(right)     # this would be weird if it happened; nonetheless
			error("Column types do not match") # it is possible to write type unstable iterators
		end
		if length(left) <= length(right)
			return new{J, I, eltype(left)}(right, left, true, div(right, left), right % left )
		else
			return new{J, I, eltype(left)}(left, right, false, div(left, right), left % right)
		end
	end
end

# Nicer than subscripting tuples
immutable _JoinValue
	large::Vector{Int64}
	small::Vector{Int64}
end

# Would this be more efficient as object id dictionaries?
immutable _JoinState{T}
	reserve::Dict{T, _JoinValue}
	consign::Dict{T, _JoinValue}
	largemarchin::Int64
	smallmarchin::Int64
	largemarchout::Int64
	smallmarchout::Int64
end

# ToDo: drop J and fix on Inner, as this algorithm is just the inner join
# ToDo: use ratio/remainder to skip march in the small compared to the large
# with an initial preallocation of the small of remainder size.

# Alternating march in until there is a hit, then send through done to next for march out
function Base.start{J<:AbstractJoinType, I, T}(iterator::Join{J, I, T})
	reserve = Dict{T, _JoinValue}()
	consign = Dict{T, _JoinValue}()
	sizehint!(reserve, length(iterator.small))
	for i = 1:iterator.smalllength

		# March in from the small source
		smallvalue = get!(reserve, iterator.small(i), _JoinValue(Vector{Int64}(0), Vector{Int64}(0)))
		push!(smallvalue.small, i)

		# March in from the large source
		largevalue = get!(reserve, iterator.large(i), _JoinValue(Vector{Int64}(0), Vector{Int64}(0)))
		push!(largevalue.large, i)

		# Return the hit
		if length(smallvalue.large) > 0 || length(largevalue.small) > 0
			return _JoinState(reserve, consign, i, i, length(largevalue.small), length(smallvalue.large))
		end
	end

	# Both sources are the same size, so we are done
	if length(iterator.small) == length(iterator.large)
		return _JoinState(reserve, consign, length(iterator.large) + 1, length(iterator.small) + 1, 0, 0)
	end

	# Still data left in the large source, we only need roll call, without marching in
	for i = (length(iterator.small) + 1):length(iterator.large)
		largevalue = get(reserve, iterator.large(i), _JoinValue(Vector{Int64}(0), Vector{Int64}(0)))
		if length(largevalue.small) > 0
			return _JoinState(reserve, consign, i, length(iterator.small) + 1, length(largevalue.small), 0)
		end
	end

	# No matches found
	_JoinState(reserve, consign, length(iterator.large) + 1, length(iterator.small) + 1, 0, 0)
end

# March out the finds, small then big, until there are none, and then alternatingly march
# in until there is another hit
function Base.next{J<:AbstractJoinType, I, T}(iterator::Join{J, I}, state::_JoinState{T})

	# We are in next so there must be something to return, start by marching out the small
	# finds, on the left
	if state.smallmarchout > 0 && iterator.parity
		item = (iterator.small[state.smallmarchin], state.smallmarchin, state.reserve[iterator.small[state.smallmarchin]].large[state.smallmarchout])
		largemarchout = state.largemarchout
		smallmarchout = state.smallmarchout - 1

	# March out the small finds, on the right
	elseif state.smallmarchout > 0
		item = (iterator.small[state.smallmarchin], state.reserve[iterator.small[state.smallmarchin]].large[state.smallmarchout], state.smallmarchin)
		largemarchout = state.largemarchout
		smallmarchout = state.smallmarchout - 1

	# March out the large finds, on the right
	elseif state.largemarchout > 0 && iterator.parity
		ritem = (iterator.large[state.largemarchin], state.reserve[iterator.large[state.largemarchin]].small[state.largemarchout], state.largemarchin)
		largemarchout = state.largemarchout - 1
		smallmarchout = state.smallmarchout

	# March out the large finds, on the left
	elseif state.largemarchout > 0
		item = (iterator.large[state.largemarchin], state.largemarchin, state.reserve[iterator.large[state.largemarchin]].small[state.largemarchout])
		largemarchout = state.largemarchout
		smallmarchout = state.smallmarchout - 1

	# Opps something went wrong, maybe next was called out of loop, or sequence
	else
		error("next() was called on an invalid state")
	end

	# More left to march out after current, safe to loop through done to next
	if largemarchout > 0 || smallmarchout > 0
		return (item, _JoinState(state.reserve, state.consign, state.largemarchin, state.smallmarchin, largemarchout, smallmarchout))
	end

	# Nothing left to march out after the current item, so proceed with alternating march in
	if state.smallmarchin < length(iterator.small)
		for i = (state.smallmarchin + 1):length(iterator.small)

			# March in from the small source
			smallvalue = get!(state.reserve, iterator.small(i), _JoinValue(Vector{Int64}(0), Vector{Int64}(0)))
			push!(smallvalue.small, i)

			# March in from the large source
			largevalue = get!(state.reserve, iterator.large(i), _JoinValue(Vector{Int64}(0), Vector{Int64}(0)))
			push!(largevalue.large, i)

			# Return the hit
			if length(smallvalue.large) > 0 || length(largevalue.small) > 0
				return (item, _JoinState(state.reserve, state.consign, i, i, length(largevalue.small), length(smallvalue.large)))
			end
		end
	end

	# Both sources are the same size, so we are done
	if length(iterator.small) == length(iterator.large)
		return (item, _JoinState(state.reserve, state.consign, length(iterator.small) + 1, length(iterator.large) + 1, 0, 0))
	end

	# Still data left in the large source, we only need roll call, without marching in
	for i = (length(iterator.small) + 1):length(iterator.large)
		largevalue = get(state.reserve, iterator.large(i), _JoinValue(Vector{Int64}(0), Vector{Int64}(0)))
		if length(largevalue.small) > 0
			return (item, _JoinState(state.reserve, state.consign, i, length(iterator.small) + 1, length(largevalue.small), 0))
		end
	end

	# Whew! made it all the way through and found no more matches than the current one.
	(item, _JoinState(state.reserve, state.consign, length(iterator.large) + 1, length(iterator.small) + 1, 0, 0))
end
Base.done{J<:AbstractJoinType, I, T}(iterator::Join{J, I, T}, state::_JoinState{T}) =
	state.largemarchin > length(iterator.large) &&
	state.smallmarchin > length(iterator.small) &&
	state.largemarchout < 1 &&
	state.smallmarchout < 1
Base.eltype{J<:AbstractJoinType, I, T}(::Type{Join{J, I, T}}) = Tuple{T, Int64, Int64}

# These are just examples. Eventually they will be replaced with relational algebra symbols
∩(left::Tuple{Vararg{AbstractVector}}, right::Tuple{Vararg{AbstractVector}}) = Join(Inner, zip(left...), zip(right...))
∪(left::Tuple{Vararg{AbstractVector}}, right::Tuple{Vararg{AbstractVector}}) = Join(Outer, zip(left...), zip(right...))

end
