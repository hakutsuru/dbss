dbss
====

**Behold, my first Python program!**
------------------------------------

dbss (database snapshot) [circa 2013-04] was a script to take snapshots of non-production Microsoft SQL Server databases to facilitate restoring baseline fixture data for testing. For reasons, updating the data in testing and staging environments was expensive, yet system testing required data to be altered often. We sought a simple way to restore databases without exporting/loading data.

I forget the reason this script was abandoned, but I think it had something to do with how snapshots only work in Development and Enterprise versions of the database, but we were using a different version... And yeah, though I strive for veracity, I am reluctant to revisit that product matrix.

Foolishly, I was rather proud of this.

I found Python to be straightforward (except for it's object syntax and packaging, etc). And I loved *docopt*. Yet my soul burned with heresy, *I thought Ruby less magical*.

Fear & Loathing
---------------

*dbss.py* is my last revision of the program (from 2013-04-07).

fudomunro reviewed and refactored *dbss*. Those changes are not included, so the script may be damp (un-DRY) and amateurish. Changes included...

1] "Removing concatenation for multi-line strings"

That broke message formatting, which made me sad.

2] "Streamlining use of lists"

Whereupon nested for loops were replaced by inscrutable list comprehensions, which made me angry.

Let's dive into snark waters, the trouble with the Python community is *pythonistas*. We were trying to get Windows administrators to use our devops tooling written in Python, and instead of making simple procedural solutions that anyone could understand...

*We were rewriting Maven in Python.*

To wit, if you are working with Python experts, brain-twisting list comprehensions are great. Otherwise, you are driving your colleagues crazy, and shame on you.

Ultimately, we were far less successful than we could have been.
