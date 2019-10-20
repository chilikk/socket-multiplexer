# socket-multiplexer

This script allows to distribute the data sent by a single publisher over
plain TCP socket to multiple listeners, as illustrated by the following diagram.
```
                                       ----------
                                 /---->|listener|
  -----------       -------------      ----------
  |publisher|------>|multiplexer|
  -----------       -------------      ----------
                                 \---->|listener|
                                       ----------
```
The data flowing in the opposite direction (i.e. sent by listeners) is ignored.
