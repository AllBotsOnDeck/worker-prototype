async def say_hello():
    # say hello to the user and request their name by appending to the manual queue and waiting for a response
    name = await manual_queue.append("Hello, what is your name?")
    return
