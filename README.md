# Concurrency Lab: Channels in C

## Lab Description

In this lab, I implemented channels as a model for synchronization via message passing in C. Channels can be visualized as a queue or buffer of messages with a fixed maximum size, allowing multiple threads to communicate efficiently. Each channel allows for multiple senders to add messages to the queue and multiple receivers to retrieve them, facilitating concurrent programming constructs.

## Key Features

1. **Channel Implementation**: Designed and implemented channels that support both blocking and non-blocking modes for send and receive operations.

   - **Blocking Mode**: Ensures that receivers block until data is available and senders wait if the queue is full.
   - **Non-blocking Mode**: Allows receivers to return immediately if no data is available and senders to leave if the queue is full.

2. **Concurrency**: Enabled multiple clients to read from and write to the channel simultaneously, ensuring thread-safe operations and correct synchronization.

3. **Performance Considerations**: Focused on functionality over performance but avoided inefficient designs that waste CPU time or rely on fixed time sleeps.

## Honors Option

- **Unbuffered Channels**: Extended the implementation to support unbuffered channels with a size of zero, requiring senders and receivers to wait for each other, and redesigned synchronization mechanisms to accommodate this feature.

Through this lab, I gained hands-on experience in concurrent programming and synchronization, which are crucial skills for developing high-performance, multi-threaded applications.
