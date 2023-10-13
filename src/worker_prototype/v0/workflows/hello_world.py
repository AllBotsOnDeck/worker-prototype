async def workflow():
    # show hellow world to the user and wait for a response
    name = await say_hello()
    # add something to the informational queue for the user
    await info_queue.append(f"Hello {name}!")


@task(priority=1,...)
def get_emails():
    emails = gmailapi.get_emails()
    db.store_emails(emails)
    analyze_emails(emails)

@task
def analyze_emails(emails):
    for email in emails:
        # summarize, determine the importance and potentially add to a digest
        ...
