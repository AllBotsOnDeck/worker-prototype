# worker-prototype

Prototype for a simple worker design

## Running

<!-- ### Install postgres via docker

### Install redis

1. `brew install redis`
2. `brew services start redis` -->

### Install poetry and run the code

1. Install `poetry`
2. Run `poetry install`
3. Run `poetry shell`
4. Run `poetry run v1`

## Plan

"Atoms" - simple lambdas that have a state machine. They add other processes to the queue and then suspend. When reanimated they check their state and resume. Should be somewhat deterministic

"Playbooks" - like atoms but can occasionally use an LLM to determine the next step so the logic is more fuzzy

Example of an email assistant

======================

1. User hires email assistant.
2. Email assistant playbook is enqueued. When dequeued it automatically adds a number of items to the user input queue (needs auth, preferences, etc) then suspends. It might also enqueue certain timers to make sure the process isn't getting stuck or the user isn't getting confused.
3. Every time the user enters information it will check if it has enough to proceed. Once it does it might enqueue the "bulk email analysis" playbook. This playbook might take some parameters like user preferences, daily/weekly/hourly digests, etc and some of those preferences might be decided by an LLM based on freetext user input.
4. The "bulk email analysis" performs a few steps - pulls emails from gmail, groups by hour/day/week depending on the digest, gets a summary, and importance ranking for each email,
   creates a digest for each group once the summaries and rankings are complete. Note that some of these actions need to run and suspend. For example the worker might request emails from gmail then suspend and enqueue itself with a pointer to its progress. Or it might call a separate atom that just requests emails and calls a "success" callback for every batch of emails.
5. When an unknown action is required, a worker can be enqueued that uses a high level LLM to figure out what's next.

=========

## Prior art

- temporal
- azure durable functions
- saga pattern
- amazon step functions
- workflow engines
- state machines
- trigger.dev
- actor model? elixer?
