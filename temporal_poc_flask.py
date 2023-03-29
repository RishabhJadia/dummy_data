from flask import Flask
from temporalio.client import Client
from temporalio.worker import Worker
from temporalio import activity, workflow

app = Flask(__name__)

# Initialize a Temporal client
client = Client(...)

# Define an activity implementation
@activity.defn
def my_activity(param1: str, param2: int) -> str:
    return f"Result: {param1} {param2}"

# Define a workflow implementation
@workflow.defn
class my_workflow:
    @workflow.run
    async def run(self, param1: str, param2: int):
        result1 = await workflow.execute_activity(my_activity, param1, param2)
        result2 = await workflow.execute_activity(my_activity, result1, 123)
        return result2

# Create a worker that listens for task queues
# worker = Worker(client, task_queue="hello-task-queue", workflows=[my_workflow], activities=[my_activity])

# # Register activity implementations
# worker.register_activities_implementation(my_activity)

# # Register workflow implementations
# worker.register_workflow_implementation_type(my_workflow)

# Start the worker
# worker.start()

# Define a Flask route that starts a workflow
@app.route("/start_workflow")
async def start_workflow():
    client = await Client.connect("localhost:7233")
    async with Worker(client, task_queue="hello-parallel-activity-task-queue", workflows=[my_workflow], activities=[my_activity]):
        workflow_execution = await client.execute_workflow(
            my_workflow.run,
            {"param1": "hello", "param2": 123},
            id="my-workflow-id",
            task_queue="hello-task-queue"
        )
    return f"Workflow execution started with run ID {workflow_execution.run_id}"

if __name__ == "__main__":
    app.run()
