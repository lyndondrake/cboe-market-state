
# Optimisations

Without optimisations, a test run took about 3 minutes 18 seconds. With optimisations enabled (but still a debug profile build with debug symbols enabled), the same test run took about 53 seconds. Both tests are on my MB with an M1 Pro. At that pace, 1TB would take a little under four hours.

# Possibilities

- it would be relatively simple to create a tool which dumps a single sequenced unit header block into a file, which could then be analysed in a KTX viewer. This would probably be essential for debugging problems with larger data files.
- the current hand-coded parser is probably difficult to optimise further. The most promising parser generator would be the [`nom` parser combinator](https://github.com/rust-bakery/nom). It would not be difficult to port the current code to `nom` and compare its performance. If a `nom` parser performs similarly, it will have advantages in safety and maintainability.
