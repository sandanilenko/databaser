# Databaser

**Problem**: when solving various types of tasks, you either need to fix the current state of data in the database and be able to return to it. If data is corrupted, you must re-deploy the database of the region that the task is associated with, or connect to other databases in the same region, whose number is limited. One way or another, there are situations when "clean" databases run out and there is a need to reverse from scratch. On average, on an ssd disk, it takes about 4 hours (usually) and 180 gigabytes of free space to turn around, which is often a problem.

**Appointment:** Creating cross-section images of the database for selected institutions to solve tasks.

**Condition:** Console application.

**Performance:** on average, it takes 20-25 minutes to create an image when working with a remote database. If the donor database is located in the local network, the image removal time is reduced to 10-15 minutes. All indicators depend on the amount of data to be transferred.

