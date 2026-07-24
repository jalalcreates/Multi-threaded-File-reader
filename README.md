Hydra Engine Multi-threading Project

So this is my second dive into C++ concurrency. I wanted to build something a bit more complex than a basic producer-consumer setup to really test my understanding of how threads interact. The idea here is a high-performance data pipeline that ingests different types of files, processes them concurrently, and handles the results safely. It is definitely a learning project, but I tried to implement some actual industry concepts like lock-free structures and cache alignment.

What it does

The pipeline is split into a few stages. First, an async producer pushes data chunks into a lock-free ring buffer. Then, a custom work-stealing thread pool pulls from that buffer and processes the chunks. Each core has its own cache-aligned metrics to track bytes and chunks processed. Finally, the main thread harvests the results and runs a lazy evaluation for a heavy audit report.

Features I implemented

- Lock-free ring buffer using CAS loops for the ingestion pipeline
- Work-stealing thread pool so idle threads can grab tasks from busy neighbors
- Cache-line isolation for metrics to prevent false sharing
- Deadlock-free resource transfers using std::scoped_lock
- Lazy evaluation using std::launch::deferred for the heavy audit report
- Batch processing logic to reduce atomic overhead

The bugs I ran into

Concurrency is unforgiving and I spent a lot of time debugging weird behavior. 

The biggest one was when my PC literally froze. I was running the code in my IDE and the screen went black, I had to hard reset my machine. It took me a while to figure out that my main loop was spinning at 100% CPU. The ring buffer was empty, the batch pop returned nothing, and the while loop just kept hammering the CPU without yielding. Adding a simple this_thread::yield() when the batch is empty fixed it immediately.

Then there was the 0x0 checksum bug. My deep audit report kept printing a combined checksum of zero no matter what. I stared at the code for an hour before realizing that std::future::get() actually consumes the future once. Since I already harvested the results in the main loop, the deferred audit had nothing left to read. I had to restructure the logic to reset the future state after harvesting.

I also kept getting compiler errors about incomplete types for std::ostringstream and missing operators for std::hex. I just completely forgot to include <sstream> and <iomanip> at the top of the file. Classic mistake.

Another weird issue was a compiler error about "qualifiers dropped in binding reference" for the Chunk struct. I was capturing it in the lambda and passing it to a function that expected a non-const reference. I just changed the process function to take a const reference instead.

And my debug_hits counter was giving me different numbers every time I ran it. It would print 4 in the editor but 5 in the terminal. It was a plain int being incremented by multiple threads at the same time. Switched it to an atomic int and the data race went away.

What I learned

Anyway, it was a good learning experience. The hardest part was figuring out when to use a mutex versus an atomic variable, and actually understanding why cache alignment matters for performance. Writing a work-stealing pool from scratch was tricky too, especially making sure the threads don't deadlock when trying to steal from each other's queues. Seeing it all run cleanly at the end made the debugging worth it.
