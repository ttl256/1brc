# The One Billion Row Challenge

https://github.com/gunnarmorling/1brc

# Generating input data

The official repo
[states](https://github.com/gunnarmorling/1brc?tab=readme-ov-file#running-the-challenge)
two ways to generate input data: 1) via `create_measurements.sh`, which runs a
Java program 2) via `src/main/python/create_measurements.py`. The python version
of the generator do not create a proper 10K keysize dataset and average
temperature is not normally distributed.

This repo includes its own generator featuring:
- Seeding. With custom seed it's possible to recreate the same dataset on different machines without copying over ~16GB file.
- 10K keyset size.
- No sampling data. Weatherstations' name is randomly generated while maintaining a the same 7th order curve to produce names.
- Average temperature is based on latitude and normally distributed as per official generation program.

To build the executable and generate the input data run:
```
make create_measurements
./create_measurements.exe
```

```
Usage of ./create_measurements.exe:
  -c int
        Number of rows to generate (default 1000000000)
  -f string
        Path to output file with measurements data (default "./measurements.txt")
  -seed string
        Seed for random number generator. If len(seed) < 32 it's padded with zeroes, if len(seed) > 32 it's truncated to 32.
```

# Run

`make 1brc`

Baseline implementation for performance comparison and result verification.

`./1brc.exe -f $path_to_output_file -baseline -print`

Author's current implementation.

`./1brc.exe -f $path_to_output_file -print`
