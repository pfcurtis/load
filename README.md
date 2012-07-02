load
====

This load generator uses a trace file of read and write operations to generate somewhat realistic patterns 
for stressing an I/O system.

The basic idea is that a generator schedules I/O operations according to a calendar of rates for
read and write transactions.  The I/O operations themselves as well as all sleeps and time checks
are handled by an object that is injected into the generator which allows these operations to be
abstracted for testing.

The primary command line entry point (com.mapr.load.Load) accepts a list of files containing traces
on the command line.

DONE Test high percentile statistic stuff

TODO Provide some scheme for resetting stats after warmup.  Perhaps reset at beginning of each trace
TODO Make random access read support power law read patterns to help evaluating caching performance
TODO Build random access and linear reader and write actors that delegate unhandled operations.
TODO Build base class for actor delegation that records segment boundaries.
TODO Build multi-threaded generator based on the ability to pass a scale factor to generators.
