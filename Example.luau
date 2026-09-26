--!strict
const RBXSignal = require("./RBXSignal")
const ExampleSignal = RBXSignal.New<<(number, number, number)>>()

task.spawn(function()
	task.wait(2)
	ExampleSignal:Fire(math.random(1, 10), math.random(1, 50), math.random(5, 100))	
end)

const x, y, z = ExampleSignal:Wait()
print(x, y, z)