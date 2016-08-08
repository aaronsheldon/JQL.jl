# ToDo: Flush out the remainder of the outer join, including checking the reserve for hits.
# Can we safely copy into consign all the time and then just pop from reserve on first hit?

# Alternating march in until there is a hit, then send through done to next for march out
function Base.start{I, T}(iterator::Join{Outer, I, T})
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

		# Return the hit on moving both to consign
		if iterator.small[i] == iterator.large[i] && length(smallvalue.large) > 0 && length(largevalue.small) > 0
			consign[iterator.small[i]] = pop!(reserve, iterator.small[i])
			return _JoinState(reserve, consign, i, i, length(largevalue.small), length(smallvalue.large))
		elseif length(smallvalue.large) > 0 && length(largevalue.small) > 0
			consign[iterator.small[i]] = pop!(reserve, iterator.small[i])
			consign[iterator.large[i]] = pop!(reserve, iterator.large[i])
			return _JoinState(reserve, consign, i, i, length(largevalue.small), length(smallvalue.large))
		elseif length(largevalue.small) > 0
			consign[iterator.large[i]] = pop!(reserve, iterator.large[i])
			return _JoinState(reserve, consign, i, i, length(largevalue.small), length(smallvalue.large))
		elseif length(smallvalue.large) > 0
			consign[iterator.small[i]] = pop!(reserve, iterator.small[i])
		end
	end

	# There were no hits so it is safe to tell next to just loop through both sources.
	# ToDo: patch this state so that is actually does this
	_JoinState(reserve, consign, length(iterator.large) + 1, length(iterator.small) + 1, 0, 0)

	#=
	# Both sources are the same size, so we start sending out all the data
	if length(iterator.small) == length(iterator.large)
		return _JoinState(reserve, consign, length(iterator.large) + 1, length(iterator.small) + 1, 0, 0)
	end

	# Still data left in the large source, so we start se
	for i = (length(iterator.small) + 1):length(iterator.large)
		largevalue = get(reserve, iterator.large(i), _JoinValue(Vector{Int64}(0), Vector{Int64}(0)))
		if length(largevalue.small) > 0
			return _JoinState(reserve, consign, i, length(iterator.small) + 1, length(largevalue.small), 0)
		end
	end

	# No matches found
	_JoinState(reserve, consign, length(iterator.large) + 1, length(iterator.small) + 1, 0, 0)
	=#
end

# March out the finds, small then big, until there are none, and then alternatingly march
# in until there is another hit
function Base.next{I, T}(iterator::Join{Outer, I}, state::_JoinState{T})

	# We are in next so there must be something to return, start by marching out the small
	# finds, on the left. All matches are always in consign.
	if state.smallmarchout > 0 && iterator.parity
		item = (iterator.small[state.smallmarchin], state.smallmarchin, state.consign[iterator.small[state.smallmarchin]].large[state.smallmarchout])
		largemarchout = state.largemarchout
		smallmarchout = state.smallmarchout - 1

	# March out the small finds, on the right
	elseif state.smallmarchout > 0
		item = (iterator.small[state.smallmarchin], state.consign[iterator.small[state.smallmarchin]].large[state.smallmarchout], state.smallmarchin)
		largemarchout = state.largemarchout
		smallmarchout = state.smallmarchout - 1

	# March out the large finds, on the right
	elseif state.largemarchout > 0 && iterator.parity
		ritem = (iterator.large[state.largemarchin], state.consign[iterator.large[state.largemarchin]].small[state.largemarchout], state.largemarchin)
		largemarchout = state.largemarchout - 1
		smallmarchout = state.smallmarchout

	# March out the large finds, on the left
	elseif state.largemarchout > 0
		item = (iterator.large[state.largemarchin], state.largemarchin, state.consign[iterator.large[state.largemarchin]].small[state.largemarchout])
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
			smallvalue = get(state.consign, iterator.small(i), _JoinValue(Vector{Int64}(0), Vector{Int64}(0)))
			if length(smallvalue.large) < 1
				smallvalue = get!(state.reserve, iterator.small(i), smallvalue)
			end
			push!(smallvalue.small, i)

			# March in from the large source
			largevalue = get(state.consign, iterator.large(i), _JoinValue(Vector{Int64}(0), Vector{Int64}(0)))
			if length(largevalue.small) < 1 && length(largevalue.large) < 1
				largevalue = get!(state.reserve, iterator.large(i), largevalue)
			end
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
