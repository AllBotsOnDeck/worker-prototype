import redis

class SimpleQueue:
    def __init__(self, name, namespace='queue'):
        """
        The key name in Redis will be namespace:name.
        """
        self.__db = redis.Redis()
        self.key = f"{namespace}:{name}"

    def enqueue(self, item):
        """
        Add an item to the end of the queue.
        """
        self.__db.rpush(self.key, item)

    def dequeue(self):
        """
        Remove and return an item from the front of the queue.
        """
        return self.__db.lpop(self.key)

    def size(self):
        """
        Return the number of items in the queue.
        """
        return self.__db.llen(self.key)

    def peek(self):
        """
        Return the next item from the front of the queue without removing it.
        """
        return self.__db.lindex(self.key, 0)

    def clear(self):
        """
        Remove all items from the queue.
        """
        self.__db.delete(self.key)


if __name__ == '__main__':
    # Example usage:
    queue = SimpleQueue("my_queue")

    # Enqueue items
    queue.enqueue("Message 1")
    queue.enqueue("Message 2")

    # Dequeue items
    print(queue.dequeue())  # Message 1
    print(queue.dequeue())  # Message 2



