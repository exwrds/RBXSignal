--!nonstrict
--	// Filename: Signal
--	// Written by: @exwrds
--	// Description: Mock RBXScriptSignal/RBXScriptConnection.

-- Types
type Fn<Args...> = (Args...) -> ()

export type Connection = {
	Connected: boolean,
	Disconnect: typeof(
		--[=[
			Disconnects the connection from the event.
		]=]
		function(self: Connection): ...any
			return nil :: any
		end
	),
}

export type Signal<Args...> = {
	Connect: typeof(
		--[=[
			Connects the given function to the event and returns an
			Signal::Connection that represents it.
		]=]
		function(self: Signal<Args...>, Callback: Fn<Args...>): Connection
			return nil :: any
		end
	),
	
	ConnectParallel: typeof(
		--[=[
			Connects the given function to the event and returns an Signal::Connection
			that represents it.
		]=]
		function(self: Signal<Args...>, Callback: Fn<Args...>): Connection
			return nil :: any
		end
	),
	
	Wait: typeof(
		--[=[
			Yields the current thread until the signal fires
			and returns the arguments provided by the
			signal.
		]=]
		function(self: Signal<Args...>): Args...
			return nil :: any
		end
	),
	
	Once: typeof(
		--[=[
			Connects the given function to the event (for a single invocation) and
			returns an Signal::Connection that represents it.
		]=]
		function(self: Signal<Args...>, Callback: Fn<Args...>): Connection
			return nil :: any
		end
	),
	
	Fire: typeof(
		--[=[
			Fires the signal, invoking every connected callback with
			the provided arguments.
		]=]
		function(self: Signal<Args...>, ...: Args...): ()
			return nil :: any
		end
	),
}

-- Constant Expressions
const IS_ACTOR_ENVIRONMENT = pcall(task.synchronize)

-- Method Tables
const Connection = {}
Connection.__index = Connection

const SignalMethods = {}
SignalMethods.__index = SignalMethods

-- Util Functions

const function isCallable(value: any): boolean
	const valueType = type(value)
	if valueType == "function" then
		return true
	end
	
	if valueType ~= "table" and valueType ~= "userdata" then
		return false
	end
	
	const success, metatable = pcall(getmetatable, value)
	if not success or type(metatable) ~= "table" then
		return false
	end
	
	const callMethod = metatable.__call
	return callMethod ~= nil and isCallable(callMethod)
end

const function runParallelHandler(handler: Fn<any>, ...)
	task.desynchronize()
	handler(...)
	task.synchronize()
end

const function insertConnection(signal, connection)
	const tail = signal._tail
	if tail then
		tail._next = connection
		connection._prev = tail
	else
		signal._head = connection
	end
	
	signal._tail = connection
	return connection
end

const function createConnection(signal, callback, once, parallel)
	return setmetatable({
		Connected = true,
		_handler = callback,
		_once = once,
		_parallel = parallel,
		_signal = signal,
		_next = nil,
		_prev = nil,
	}, Connection)
end

-- Connection Methods

function Connection:Disconnect()
	if not self.Connected then
		return
	end
	
	self.Connected = false
	
	const signal = self._signal
	const prevConnection, nextConnection = self._prev, self._next
	
	if prevConnection then
		prevConnection._next = nextConnection
	elseif signal then
		signal._head = nextConnection
	end
	
	if nextConnection then
		nextConnection._prev = prevConnection
	elseif signal then
		signal._tail = prevConnection
	end
	
	self._signal = nil
	self._handler = nil
	self._next = nil
	self._prev = nil
end

-- Signal Methods

const function connect(signal, callback, once, parallel)
	if not isCallable(callback) then
		return error("Attempt to connect failed: Passed value is not a function", 3)
	end
	
	return insertConnection(signal, createConnection(signal, callback, once, parallel))
end

function SignalMethods:Connect(callback: Fn<any>)
	return connect(self, callback, false, false)
end

function SignalMethods:ConnectParallel(callback: Fn<any>)
	if not IS_ACTOR_ENVIRONMENT then
		return error("Scripts that connect in parallel must be rooted under an Actor.", 2)
	end
	
	return connect(self, callback, false, true)
end

function SignalMethods:Once(callback: Fn<any>)
	return connect(self, callback, true, false)
end

function SignalMethods:Wait()
	const current_thread = coroutine.running()
	const yielded_threads = self._yieldedThreads
	if yielded_threads[current_thread] then
		return error("Somehow called ::Wait() while already yielded?", 2)
	end
	
	yielded_threads[current_thread] = true
	return coroutine.yield()
end

function SignalMethods:Fire(...)
	local connection = self._head
	while connection do
		const nextConnection = connection._next
		if connection.Connected then
			const handler = connection._handler
			if connection._once then
				connection:Disconnect()
			end
			
			if connection._parallel then
				task.spawn(runParallelHandler, handler, ...)
			else
				task.spawn(handler, ...)
			end
		end
		
		connection = nextConnection
	end
	
	for thread in pairs(self._yieldedThreads) do
		self._yieldedThreads[thread] = nil
		task.defer(thread, ...)
	end
end

-- Signal Constructor

const function New<Args...>(): Signal<Args...>
	return setmetatable({
		_head = nil,
		_tail = nil,
		_yieldedThreads = setmetatable({}, { __mode = "k" }),
	}, SignalMethods) :: any
end

return { New = New }