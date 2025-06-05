# Backup Trees

Original Author: Kevin Cao

Last Updated: June 2024

---

Currently, the way we structure our backups is that you can have a full backup, and then a chain of incremental backups on top of that backup (imagine a linked-list). However, you can only have so many incremental backups per full backup.

That means that if a customer wants to have daily backups, the best RPO they can have is 30 minutes. If they want to have a lower RPO, they will need to increase the full backup frequency.

To solve this problem, we’ve come up with backup trees. In essence, we allow incremental backups to have their own incremental backups. Let’s say that a customer performs full backups on a weekly basis, but wants an RPO of an hour. Under the old system, this would require 7 * 24 = 168 incremental backups, something we most certainly would not recommend.

The idea is that instead perform daily incrementals, but additionally have incremental hourly backups between each day. Our backups would be structured something like this:

![Tree Backups](../images/tree-backups.png)
Oh look, it’s a sideways tree! Now, if we want to restore day 1, hour 5, we can do so by restoring the first weekly backup, then the first daily incremental, followed by the 5 hourly incrementals. But more importantly, if we wish to restore day 3, hour 10, instead of having to restore 1 + 24 + 24 + 10 = 58 backups, we can essentially fast forward. We would restore the weekly full backup, three daily incrementals, and then 10 hourly incrementals. So instead of 58 backups, we are restoring 14 backups.

This backup tree method gives us far more granularity with our RPO, but also reduces the need for frequent full backups and minimizes the overhead of doing incremental restores.
